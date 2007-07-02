package com.aoindustries.aoserv.master;

/*
 * Copyright 2002-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>InterBaseHandler</code> handles all the accesses to the InterBase tables.
 *
 * @author  AO Industries, Inc.
 */
final public class InterBaseHandler {

    private final static Map<String,Boolean> disabledInterBaseUsers=new HashMap<String,Boolean>();
    private final static Map<Integer,Boolean> disabledInterBaseServerUsers=new HashMap<Integer,Boolean>();

    private final static Object interBaseDBGroupLock=new Object();

    /**
     * Adds an InterBase database to the system.
     */
    public static int addInterBaseDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name,
        int dbGroup,
        int datdba
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "addInterBaseDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,int)", null);
        try {
            checkAccessInterBaseDBGroup(conn, source, "addInterBaseDatabase", dbGroup);
            
            checkAccessInterBaseServerUser(conn, source, "addInterBaseDatabase", datdba);
            if(isInterBaseServerUserDisabled(conn, datdba)) throw new SQLException("Unable to add InterBaseDatabase '"+name+"', datdba disabled: "+datdba);

            // Must be a valid name format
            List<String> reservedWords=getReservedWords(conn);
            if(!InterBaseDatabaseTable.isValidDatabaseName(name, reservedWords)) throw new SQLException("Invalid InterBase database name: "+name);

            // Find the accouting code
            int aoServer=getAOServerForInterBaseDBGroup(conn, dbGroup);
            String accounting=PackageHandler.getBusinessForPackage(conn, getPackageForInterBaseDBGroup(conn, dbGroup));

            // Add the entry to the database
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('interbase_databases_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  interbase_databases\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  "+InterBaseDatabase.DEFAULT_BACKUP_LEVEL+",\n"
                + "  "+InterBaseDatabase.DEFAULT_BACKUP_RETENTION+"\n"
                + ")",
                pkey,
                name,
                dbGroup,
                datdba
            );

            // Notify all clients of the update, the server will detect this change and automatically add the database
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_DATABASES,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds an InterBaseDBGroup to a server.
     */
    public static int addInterBaseDBGroup(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name,
        int lsg
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "addInterBaseDBGroup(MasterDatabaseConnection,RequestSource,InvalidateList,String,int)", null);
        try {
            // Must be a valid name format
            List<String> reservedWords=getReservedWords(conn);
            if(!InterBaseDBGroupTable.isValidDBGroupName(name, reservedWords)) throw new SQLException("Invalid InterBase DB Group name: "+name);

            // Security checks
            LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addInterBaseDBGroup", lsg);

            int aoServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, lsg);
            String server=ServerHandler.getHostnameForServer(conn, aoServer);

            int pkey;

            // Lock to avoid concurrent updates from causing multiple db groups with same name on same server
            synchronized(interBaseDBGroupLock) {
                // Must not already exist on this AOServer
                if(
                    conn.executeBooleanQuery(
                        "select (\n"
                        + "  select\n"
                        + "    idg.pkey\n"
                        + "  from\n"
                        + "    interbase_db_groups idg,\n"
                        + "    linux_server_groups lsg\n"
                        + "  where\n"
                        + "    idg.name=?\n"
                        + "    and idg.linux_server_group=lsg.pkey\n"
                        + "    and lsg.ao_server=?\n"
                        + "  limit 1\n"
                        + ") is not null",
                        name,
                        aoServer
                    )
                ) throw new SQLException("InterBaseDBGroup already exists on aoServer #"+aoServer+" ("+server+"): "+name);
             
                // Get the pkey
                pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('interbase_db_groups_pkey_seq')");

                // Add the DB group
                conn.executeUpdate(
                    "insert into interbase_db_groups values(?,?,?)",
                    pkey,
                    name,
                    lsg
                );
            }

            // Invalidate caches
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_DB_GROUPS,
                LinuxAccountHandler.getBusinessForLinuxServerGroup(conn, lsg),
                server,
                false
            );
            
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds an InterBase server user.
     */
    public static int addInterBaseServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "addInterBaseServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,String,int)", null);
        try {
            checkAccessInterBaseUser(conn, source, "addInterBaseServerUser", username);
            if(isInterBaseUserDisabled(conn, username)) throw new SQLException("Unable to add InterBaseServerUser, InterBaseUser disabled: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add InterBaseServerUser for user '"+LinuxAccount.MAIL+'\'');
            ServerHandler.checkAccessServer(conn, source, "addInterBaseServerUser", aoServer);
            UsernameHandler.checkUsernameAccessServer(conn, source, "addInterBaseUser", username, aoServer);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('interbase_server_users_pkey_seq')");
            conn.executeUpdate(
                "insert into interbase_server_users values(?,?,?,null,null)",
                pkey,
                username,
                aoServer
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_SERVER_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                ServerHandler.getHostnameForServer(conn, aoServer),
                true
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds an InterBase user.
     */
    public static void addInterBaseUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username,
        String firstname,
        String middlename,
        String lastname
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "addInterBaseUser(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "addInterBaseUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add InterBaseUser, Username disabled: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add InterBaseUser for user '"+LinuxAccount.MAIL+'\'');
            if(!InterBaseUser.isValidUsername(username)) throw new SQLException("Invalid InterBaseUser username: "+username);

            if(!InterBaseUser.isValidName(firstname)) throw new SQLException("Invalid firstname: "+firstname);
            if(!InterBaseUser.isValidName(middlename)) throw new SQLException("Invalid middlename: "+middlename);
            if(!InterBaseUser.isValidName(lastname)) throw new SQLException("Invalid lastname: "+lastname);
            conn.executeUpdate(
                "insert into interbase_users values(?,?,?,?,null)",
                username,
                firstname,
                middlename,
                lastname
            );

            // Notify all clients of the update
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_USERS,
                accounting,
                BusinessHandler.getServersForBusiness(conn, accounting),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Backs up an InterBaseDatabase by dumping it and passing the data
     * to the backup server for that database server.
     */
    public static int backupInterBaseDatabase(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "backupInterBaseDatabase(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessInterBaseDatabase(conn, source, "backupInterBaseDatabase", pkey);

            // Get the backup settings
            short backup_level=conn.executeShortQuery("select backup_level from interbase_databases where pkey=?", pkey);
            if(backup_level==0) throw new SQLException("Unable to backup InterBaseDatabase with a backup_level of 0: "+pkey);
            short backup_retention=conn.executeShortQuery("select backup_retention from interbase_databases where pkey=?", pkey);

            // Figure out where the dump is coming from
            String packageName=getPackageForInterBaseDatabase(conn, pkey);
            int aoServer=getAOServerForInterBaseDatabase(conn, pkey);
            String name=conn.executeStringQuery("select name from interbase_databases where pkey=?", pkey);
            String dbGroupName=conn.executeStringQuery(
                "select\n"
                + "  idg.name\n"
                + "from\n"
                + "  interbase_databases id,\n"
                + "  interbase_db_groups idg\n"
                + "where\n"
                + "  id.pkey=?\n"
                + "  and id.db_group=idg.pkey",
                pkey
            );

            // Stream from one server to the other
            long startTime=System.currentTimeMillis();
            int backupData=BackupHandler.backupDatabase(
                conn,
                backupConn,
                source,
                invalidateList,
                aoServer,
                name+".gbak.base64",
                AOServDaemonProtocol.BACKUP_INTERBASE_DATABASE,
                pkey
            );
            long endTime=System.currentTimeMillis();

            // Add to the backup database
            int backupPKey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('interbase_backups_pkey_seq')");
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into interbase_backups values(?,?,?,?,?,?,?,?,?,?)");
            try {
                pstmt.setInt(1, backupPKey);
                pstmt.setInt(2, PackageHandler.getPKeyForPackage(conn, packageName));
                pstmt.setString(3, dbGroupName);
                pstmt.setString(4, name);
                pstmt.setInt(5, aoServer);
                pstmt.setTimestamp(6, new Timestamp(startTime));
                pstmt.setTimestamp(7, new Timestamp(endTime));
                pstmt.setInt(8, backupData);
                pstmt.setShort(9, backup_level);
                pstmt.setShort(10, backup_retention);
                backupConn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_BACKUPS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            return backupPKey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canAccessInterBaseBackup(MasterDatabaseConnection conn, BackupDatabaseConnection backupConn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "canAccessInterBaseBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            String username=source.getUsername();
            MasterUser mu=MasterServer.getMasterUser(conn, username);
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, username).length==0) return true;
                else return ServerHandler.canAccessServer(conn, source, getAOServerForInterBaseBackup(backupConn, pkey));
            } else return PackageHandler.canAccessPackage(conn, source, getPackageForInterBaseBackup(backupConn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessInterBaseBackup(MasterDatabaseConnection conn, BackupDatabaseConnection backupConn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "checkAccessInterBaseBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(
                !canAccessInterBaseBackup(conn, backupConn, source, pkey)
            ) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access interbase_backup: action='"
                    +action
                    +", pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessInterBaseDatabase(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        checkAccessInterBaseDBGroup(conn, source, action, getInterBaseDBGroupForInterBaseDatabase(conn, pkey));
    }

    public static boolean canAccessInterBaseDBGroup(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "canAccessInterBaseDBGroup(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            String username=source.getUsername();
            MasterUser mu=MasterServer.getMasterUser(conn, username);
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, username).length==0) return true;
                else return ServerHandler.canAccessServer(conn, source, getAOServerForInterBaseDBGroup(conn, pkey));
            } else return PackageHandler.canAccessPackage(conn, source, getPackageForInterBaseDBGroup(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessInterBaseDBGroup(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "checkAccessInterBaseDBGroup(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(
                !canAccessInterBaseDBGroup(conn, source, pkey)
            ) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access interbase_db_groups: action='"
                    +action
                    +", pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canAccessInterBaseServerUser(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "canAccessInterBaseServerUser(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            String username=source.getUsername();
            MasterUser mu=MasterServer.getMasterUser(conn, username);
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, username).length==0) return true;
                else return ServerHandler.canAccessServer(conn, source, getAOServerForInterBaseServerUser(conn, pkey));
            } else return UsernameHandler.canAccessUsername(conn, source, getInterBaseUserForInterBaseServerUser(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessInterBaseServerUser(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "checkAccessInterBaseServerUser(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessInterBaseServerUser(conn, source, pkey)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access interbase_server_user: action='"
                    +action
                    +"', pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessInterBaseUser(MasterDatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "checkAccessInterBaseUser(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, action, username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableInterBaseServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "disableInterBaseServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isInterBaseServerUserDisabled(conn, pkey)) throw new SQLException("InterBaseServerUser is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableInterBaseServerUser", disableLog, false);
            checkAccessInterBaseServerUser(conn, source, "disableInterBaseServerUser", pkey);
            String username=getInterBaseUserForInterBaseServerUser(conn, pkey);
            if(username.equals(InterBaseUser.SYSDBA)) throw new SQLException("Not allowed to disable the "+InterBaseUser.SYSDBA+" InterBase user");

            conn.executeUpdate(
                "update interbase_server_users set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_SERVER_USERS,
                getBusinessForInterBaseServerUser(conn, pkey),
                ServerHandler.getHostnameForServer(conn, getAOServerForInterBaseServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableInterBaseUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "disableInterBaseUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isInterBaseUserDisabled(conn, username)) throw new SQLException("InterBaseUser is already disabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableInterBaseUser", disableLog, false);
            UsernameHandler.checkAccessUsername(conn, source, "disableInterBaseUser", username);
            if(username.equals(InterBaseUser.SYSDBA)) throw new SQLException("Not allowed to disable the "+InterBaseUser.SYSDBA+" InterBase user");

            IntList isus=getInterBaseServerUsersForInterBaseUser(conn, username);
            for(int c=0;c<isus.size();c++) {
                int isu=isus.getInt(c);
                if(!isInterBaseServerUserDisabled(conn, isu)) {
                    throw new SQLException("Cannot disable InterBaseUser '"+username+"': InterBaseServerUser not disabled: "+isu);
                }
            }

            conn.executeUpdate(
                "update interbase_users set disable_log=? where username=?",
                disableLog,
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Dumps an InterBase database
     */
    public static void dumpInterBaseDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int dbPKey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "dumpInterBaseDatabase(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,int)", null);
        try {
            checkAccessInterBaseDatabase(conn, source, "dumpInterBaseDatabase", dbPKey);

            int aoServer=getAOServerForInterBaseDatabase(conn, dbPKey);
            DaemonHandler.getDaemonConnector(conn, aoServer).dumpInterBaseDatabase(dbPKey, out);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableInterBaseServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "enableInterBaseServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForInterBaseServerUser(conn, pkey);
            if(disableLog==-1) throw new SQLException("InterBaseServerUser is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableInterBaseServerUser", disableLog, true);
            checkAccessInterBaseServerUser(conn, source, "enableInterBaseServerUser", pkey);
            String iu=getInterBaseUserForInterBaseServerUser(conn, pkey);
            if(isInterBaseUserDisabled(conn, iu)) throw new SQLException("Unable to enable InterBaseServerUser #"+pkey+", InterBaseUser not enabled: "+iu);

            conn.executeUpdate(
                "update interbase_server_users set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_SERVER_USERS,
                UsernameHandler.getBusinessForUsername(conn, iu),
                ServerHandler.getHostnameForServer(conn, getAOServerForInterBaseServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableInterBaseUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "enableInterBaseUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForInterBaseUser(conn, username);
            if(disableLog==-1) throw new SQLException("InterBaseUser is already enabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableInterBaseUser", disableLog, true);
            UsernameHandler.checkAccessUsername(conn, source, "enableInterBaseUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable InterBaseUser '"+username+"', Username not enabled: "+username);

            conn.executeUpdate(
                "update interbase_users set disable_log=null where username=?",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Generates a unique InterBase database name.
     */
    public static String generateInterBaseDatabaseName(
        MasterDatabaseConnection conn,
        int dbGroup,
        String template_base,
        String template_added
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "generateInterBaseDatabaseName(MasterDatabaseConnection,int,String,String)", null);
        try {
            List<String> reservedWords=getReservedWords(conn);

            // Load the entire list of interbase database names in the group
            List<String> names=conn.executeStringListQuery("select name from interbase_databases where db_group=?", dbGroup);
            int size=names.size();

            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            sorted.addAll(names);

            // Find one that is not used
            String goodOne=null;
            for(int c=0;c<Integer.MAX_VALUE;c++) {
                String name= (c==0) ? template_base : (template_base+template_added+c);
                if(!InterBaseDatabaseTable.isValidDatabaseName(name, reservedWords)) throw new SQLException("Invalid InterBase database name: "+name);
                if(!sorted.contains(name)) {
                    goodOne=name;
                    break;
                }
            }

            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available InterBase database name for template_base="+template_base+" and template_added="+template_added);
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Generates a unique InterBase DB Group name.
     */
    public static String generateInterBaseDBGroupName(
        MasterDatabaseConnection conn,
        int aoServer,
        String template_base,
        String template_added
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "generateInterBaseDBGroupName(MasterDatabaseConnection,int,String,String)", null);
        try {
            List<String> reservedWords=getReservedWords(conn);

            // Load the entire list of interbase_db_group database names
            List<String> names=conn.executeStringListQuery(
                "select\n"
                + "  idg.name\n"
                + "from\n"
                + "  interbase_db_groups idg,\n"
                + "  linux_server_groups lsg\n"
                + "where\n"
                + "  idg.linux_server_group=lsg.pkey\n"
                + "  and lsg.ao_server=?\n"
                + "group by\n"
                + "  idg.name",
                aoServer
            );
            int size=names.size();

            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            sorted.addAll(names);

            // Find one that is not used
            String goodOne=null;
            for(int c=0;c<Integer.MAX_VALUE;c++) {
                String name= (c==0) ? template_base : (template_base+template_added+c);
                if(!InterBaseDBGroupTable.isValidDBGroupName(name, reservedWords)) throw new SQLException("Invalid InterBaseDBGroup name: "+name);
                if(!sorted.contains(name)) {
                    goodOne=name;
                    break;
                }
            }

            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available InterBaseDBGroup name for template_base="+template_base+" and template_added="+template_added+" on AOServer #"+aoServer);
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForInterBaseServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getBusinessForInterBaseServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  interbase_server_users isu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  isu.pkey=?\n"
                + "  and isu.username=un.username\n"
                + "  and un.package=pk.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForInterBaseServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getDisableLogForInterBaseServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from interbase_server_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForInterBaseUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getDisableLogForInterBaseUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from interbase_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getInterBaseServerUsersForInterBaseUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getInterBaseServerUsersForInterBaseUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery("select pkey from interbase_server_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getInterBaseUserForInterBaseServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getInterBaseUserForInterBaseServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select username from interbase_server_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForInterBaseBackup(BackupDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getAOServerForInterBaseBackup(BackupDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from interbase_backups where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForInterBaseDBGroup(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getAOServerForInterBaseDBGroup(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  lsg.ao_server\n"
                + "from\n"
                + "  interbase_db_groups idg,\n"
                + "  linux_server_groups lsg\n"
                + "where\n"
                + "  idg.pkey=?\n"
                + "  and idg.linux_server_group=lsg.pkey",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getInterBaseDBGroupForInterBaseDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getInterBaseDBGroupForInterBaseDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select db_group from interbase_databases where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForInterBaseServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getAOServerForInterBaseServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from interbase_server_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPackageForInterBaseBackup(BackupDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getPackageForInterBaseBackup(BackupDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select package from interbase_backups where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForInterBaseDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getPackageForInterBaseDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  lg.package\n"
                + "from\n"
                + "  interbase_databases id,\n"
                + "  interbase_db_groups idg,\n"
                + "  linux_server_groups lsg,\n"
                + "  linux_groups lg\n"
                + "where\n"
                + "  id.pkey=?\n"
                + "  and id.db_group=idg.pkey\n"
                + "  and idg.linux_server_group=lsg.pkey\n"
                + "  and lsg.name=lg.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getNameForInterBaseDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getNameForInterBaseDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select name from interbase_databases where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForInterBaseDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getAOServerForInterBaseDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  lsg.ao_server\n"
                + "from\n"
                + "  interbase_databases id,\n"
                + "  interbase_db_groups idg,\n"
                + "  linux_server_groups lsg\n"
                + "where\n"
                + "  id.pkey=?\n"
                + "  and id.db_group=idg.pkey\n"
                + "  and idg.linux_server_group=lsg.pkey",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForInterBaseDBGroup(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getPackageForInterBaseDBGroup(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  lg.package\n"
                + "from\n"
                + "  interbase_db_groups idg,\n"
                + "  linux_server_groups lsg,\n"
                + "  linux_groups lg\n"
                + "where\n"
                + "  idg.pkey=?\n"
                + "  and idg.linux_server_group=lsg.pkey\n"
                + "  and lsg.name=lg.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getNameForInterBaseDBGroup(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getNameForInterBaseDBGroup(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select name from interbase_db_groups where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final Object reservedWordLock=new Object();
    private static List<String> reservedWordCache;
    public static List<String> getReservedWords(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "getReservedWords(MasterDatabaseConnection)", null);
        try {
            synchronized(reservedWordLock) {
                if(reservedWordCache==null) {
                    // Load the list of reserved words
                    reservedWordCache = conn.executeStringListQuery("select word from interbase_reserved_words");
                }
                return reservedWordCache;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, InterBaseHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
            switch(tableID) {
                case INTERBASE_RESERVED_WORDS :
                    synchronized(reservedWordLock) {
                        reservedWordCache=null;
                    }
                    break;
                case INTERBASE_SERVER_USERS :
                    synchronized(InterBaseHandler.class) {
                        disabledInterBaseServerUsers.clear();
                    }
                    break;
                case INTERBASE_USERS :
                    synchronized(InterBaseHandler.class) {
                        disabledInterBaseUsers.clear();
                    }
                    break;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Determines if an InterBase database name is available.
     */
    public static boolean isInterBaseDatabaseNameAvailable(
        MasterDatabaseConnection conn,
        RequestSource source,
        int dbGroup,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseDatabaseNameAvailable(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            checkAccessInterBaseDBGroup(conn, source, "isInterBaseDatabaseNameAvailable", dbGroup);
            return conn.executeBooleanQuery("select (select pkey from interbase_databases where name=? and db_group=?) is null", name, dbGroup);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Determines if an InterBaseDBGroup name is available.
     */
    public static boolean isInterBaseDBGroupNameAvailable(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseDBGroupNameAvailable(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, "isInterBaseDBGroupNameAvailable", aoServer);
            return conn.executeBooleanQuery(
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      idg.pkey\n"
                + "    from\n"
                + "      interbase_db_groups idg,\n"
                + "      linux_server_groups lsg\n"
                + "    where\n"
                + "      idg.name=?\n"
                + "      and idg.linux_server_group=lsg.pkey\n"
                + "      and lsg.ao_server=?\n"
                + "    ) is null",
                name,
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isInterBaseServerUserDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseServerUserDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(InterBaseHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledInterBaseServerUsers.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForInterBaseServerUser(conn, pkey)!=-1;
		disabledInterBaseServerUsers.put(I, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isInterBaseServerUserPasswordSet(
        MasterDatabaseConnection conn,
        RequestSource source, 
        int isu
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseServerUserPasswordSet(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessInterBaseServerUser(conn, source, "isInterBaseServerUserPasswordSet", isu);
            if(isInterBaseServerUserDisabled(conn, isu)) throw new SQLException("Unable to determine if the InterBaseServerUser password is set, account disabled: "+isu);
            String username=getInterBaseUserForInterBaseServerUser(conn, isu);

            int aoServer=getAOServerForInterBaseServerUser(conn, isu);
            String password=DaemonHandler.getDaemonConnector(conn, aoServer).getEncryptedInterBaseUserPassword(username);
            return !InterBaseUser.NO_PASSWORD_DB_VALUE.equals(password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isInterBaseUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      username\n"
                + "    from\n"
                + "      interbase_users\n"
                + "    where\n"
                + "      username=?\n"
                + "    limit 1\n"
                + "  ) is not null",
                username
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isInterBaseUserDisabled(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "isInterBaseUserDisabled(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(InterBaseHandler.class) {
		Boolean O=disabledInterBaseUsers.get(username);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForInterBaseUser(conn, username)!=-1;
		disabledInterBaseUsers.put(username, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeExpiredInterBaseBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeExpiredInterBaseBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("non-master user "+username+" not allowed to removeExpiredInterBaseBackups");
            ServerHandler.checkAccessServer(conn, source, "removeExpiredInterBaseBackups", aoServer);

            // Get the list of pkeys that should be removed
            IntList pkeys=backupConn.executeIntListQuery(
                "select\n"
                + "  pkey\n"
                + "from\n"
                + "  interbase_backups\n"
                + "where\n"
                + "  ao_server=?\n"
                + "  and now()>=(end_time+(backup_retention || ' days')::interval)",
                aoServer
            );
            
            // Remove each file
            int size=pkeys.size();
            for(int c=0;c<size;c++) {
                removeInterBaseBackup(
                    conn,
                    backupConn,
                    invalidateList,
                    pkeys.getInt(c)
                );
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes an InterBaseBackup from the system.
     */
    public static void removeInterBaseBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessInterBaseBackup(conn, backupConn, source, "removeInterBaseBackup", pkey);

            removeInterBaseBackup(
                conn,
                backupConn,
                invalidateList,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeInterBaseBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseBackup(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=PackageHandler.getBusinessForPackage(conn, getPackageForInterBaseBackup(backupConn, pkey));
            int aoServer=getAOServerForInterBaseBackup(backupConn, pkey);

            // Remove the backup database entry
            backupConn.executeUpdate("delete from interbase_backups where pkey=?", pkey);
            
            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_BACKUPS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes an InterBaseDatabase from the system.
     */
    public static void removeInterBaseDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InterBaseHandler.class, "removeInterBaseDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessInterBaseDatabase(conn, source, "removeInterBaseDatabase", pkey);

            removeInterBaseDatabase(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes an InterBaseDatabase from the system.
     */
    public static void removeInterBaseDatabase(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseDatabase(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Remove the database entry
            String accounting=PackageHandler.getBusinessForPackage(conn, getPackageForInterBaseDatabase(conn, pkey));
            int aoServer=getAOServerForInterBaseDatabase(conn, pkey);
            String dbName=getNameForInterBaseDatabase(conn, pkey);
            String dbGroupName=getNameForInterBaseDBGroup(conn, getInterBaseDBGroupForInterBaseDatabase(conn, pkey));
            if(dbGroupName.equals(InterBaseDBGroup.IBSERVER) && dbName.equals(InterBaseDatabase.ISC4)) throw new SQLException("Not allowed to remove the "+InterBaseDBGroup.IBSERVER+'/'+InterBaseDatabase.ISC4+" database");

            conn.executeUpdate("delete from interbase_databases where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_DATABASES,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes an InterBaseDBGroup from the system.
     */
    public static void removeInterBaseDBGroup(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InterBaseHandler.class, "removeInterBaseDBGroup(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessInterBaseDBGroup(conn, source, "removeInterBaseDBGroup", pkey);

            removeInterBaseDBGroup(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes an InterBaseDBGroup from the system.
     */
    public static void removeInterBaseDBGroup(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseDBGroup(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Remove the database entry
            String accounting=PackageHandler.getBusinessForPackage(conn, getPackageForInterBaseDBGroup(conn, pkey));
            int aoServer=getAOServerForInterBaseDBGroup(conn, pkey);
            conn.executeUpdate("delete from interbase_db_groups where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_DB_GROUPS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes an InterBaseServerUser from the system.
     */
    public static void removeInterBaseServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessInterBaseServerUser(conn, source, "removeInterBaseServerUser", pkey);

            String username=getInterBaseUserForInterBaseServerUser(conn, pkey);
            if(username.equals(InterBaseUser.SYSDBA)) throw new SQLException("Not allowed to remove InterBaseServerUser for user '"+InterBaseUser.SYSDBA+'\'');

            // Remove the interbase_server_user
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            int aoServer=getAOServerForInterBaseServerUser(conn, pkey);
            conn.executeUpdate("delete from interbase_server_users where pkey=?", pkey);

            // Notify all clients of the updates
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_SERVER_USERS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                true
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes an InterBaseUser from the system.
     */
    public static void removeInterBaseUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, InterBaseHandler.class, "removeInterBaseUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            checkAccessInterBaseUser(conn, source, "removeInterBaseUser", username);
            
            removeInterBaseUser(conn, invalidateList, username);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes an InterBaseUser from the system.
     */
    public static void removeInterBaseUser(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "removeInterBaseUser(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            if(username.equals(InterBaseUser.SYSDBA)) throw new SQLException("Not allowed to remove InterBaseUser for user '"+InterBaseUser.SYSDBA+'\'');

            String accounting=UsernameHandler.getBusinessForUsername(conn, username);

            // Remove the interbase_server_user
            IntList aoServers=conn.executeIntListQuery("select ao_server from interbase_server_users where username=?", username);
            if(aoServers.size()>0) {
                conn.executeUpdate("delete from interbase_server_users where username=?", username);
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.INTERBASE_SERVER_USERS,
                    accounting,
                    aoServers,
                    false
                );
            }

            // Remove the interbase_user
            conn.executeUpdate("delete from interbase_users where username=?", username);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_USERS,
                accounting,
                BusinessHandler.getServersForBusiness(conn, accounting),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void restartInterBase(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "restartInterBase(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "interbase");
            if(!canControl) throw new SQLException("Not allowed to restart InterBase on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartInterBase();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Sets an InterBase password.
     */
    public static void setInterBaseServerUserPassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        int isu,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "setInterBaseServerUserPassword(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            BusinessHandler.checkPermission(conn, source, "setInterBaseServerUserPassword", AOServPermission.Permission.set_interbase_server_user_password);

            checkAccessInterBaseServerUser(conn, source, "setInterBaseServerUserPassword", isu);
            if(isInterBaseServerUserDisabled(conn, isu)) throw new SQLException("Unable to set InterBaseServerUser password, account disabled: "+isu);

            // Get the server, username for the user
            String username=getInterBaseUserForInterBaseServerUser(conn, isu);

            // No setting the super user password
            if(username.equals(InterBaseUser.SYSDBA)) throw new SQLException("The InterBase "+InterBaseUser.SYSDBA+" password may not be set.");

            // Perform the password check here, too.
            if(password!=null && password.length()==0) password=InterBaseUser.NO_PASSWORD;
            if(password!=InterBaseUser.NO_PASSWORD) {
                PasswordChecker.Result[] results = InterBaseUser.checkPassword(Locale.getDefault(), username, password);
                if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
            }

            // Contact the daemon for the update
            DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForInterBaseServerUser(conn, isu)
            ).setInterBaseUserPassword(username, password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startInterBase(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "startInterBase(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "interbase");
            if(!canControl) throw new SQLException("Not allowed to start InterBase on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startInterBase();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void stopInterBase(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "stopInterBase(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "interbase");
            if(!canControl) throw new SQLException("Not allowed to stop InterBase on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopInterBase();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Waits for any pending or processing InterBase database config rebuild to complete.
     */
    public static void waitForInterBaseRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "waitForInterBaseRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForInterBaseRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForInterBaseRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setInterBaseServerUserPredisablePassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int isu,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, InterBaseHandler.class, "setInterBaseServerUserPredisablePassword(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessInterBaseServerUser(conn, source, "setInterBaseServerUserPredisablePassword", isu);
            if(password==null) {
                if(isInterBaseServerUserDisabled(conn, isu)) throw new SQLException("Unable to clear InterBaseServerUser predisable password, account disabled: "+isu);
            } else {
                if(!isInterBaseServerUserDisabled(conn, isu)) throw new SQLException("Unable to set InterBaseServerUser predisable password, account not disabled: "+isu);
            }

            // Update the database
            conn.executeUpdate(
                "update interbase_server_users set predisable_password=? where pkey=?",
                password,
                isu
            );
            
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.INTERBASE_SERVER_USERS,
                getBusinessForInterBaseServerUser(conn, isu),
                ServerHandler.getHostnameForServer(conn, getAOServerForInterBaseServerUser(conn, isu)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
