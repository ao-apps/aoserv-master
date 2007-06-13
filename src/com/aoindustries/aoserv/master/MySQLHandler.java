package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLDatabaseTable;
import com.aoindustries.aoserv.client.MySQLServerUser;
import com.aoindustries.aoserv.client.MySQLUser;
import com.aoindustries.aoserv.client.PasswordChecker;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The <code>MySQLHandler</code> handles all the accesses to the MySQL tables.
 *
 * @author  AO Industries, Inc.
 */
final public class MySQLHandler {

    private final static Map<Integer,Boolean> disabledMySQLServerUsers=new HashMap<Integer,Boolean>();
    private final static Map<String,Boolean> disabledMySQLUsers=new HashMap<String,Boolean>();

    public static void checkAccessMySQLBackup(MasterDatabaseConnection conn, BackupDatabaseConnection backupConn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(MasterServer.getMasterUser(conn, source.getUsername())!=null) {
                int mysqlServer=getMySQLServerForMySQLBackup(backupConn, pkey);
                int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
                ServerHandler.checkAccessServer(conn, source, action, aoServer);
            } else PackageHandler.checkAccessPackage(conn, source, action, getPackageForMySQLBackup(backupConn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessMySQLDatabase(MasterDatabaseConnection conn, RequestSource source, String action, int mysql_database) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLDatabase(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    int mysqlServer=getMySQLServerForMySQLDatabase(conn, mysql_database);
                    int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
                    ServerHandler.checkAccessServer(conn, source, action, aoServer);
                }
            } else {
                PackageHandler.checkAccessPackage(conn, source, action, getPackageForMySQLDatabase(conn, mysql_database));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessMySQLDBUser(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLDBUser(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            checkAccessMySQLDatabase(conn, source, action, getMySQLDatabaseForMySQLDBUser(conn, pkey));
            checkAccessMySQLServerUser(conn, source, action, getMySQLServerUserForMySQLDBUser(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessMySQLServerUser(MasterDatabaseConnection conn, RequestSource source, String action, int mysql_server_user) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLServerUser(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    int mysqlServer = getMySQLServerForMySQLServerUser(conn, mysql_server_user);
                    int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
                    ServerHandler.checkAccessServer(conn, source, action, aoServer);
                }
            } else {
                checkAccessMySQLUser(conn, source, action, getUsernameForMySQLServerUser(conn, mysql_server_user));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessMySQLServer(MasterDatabaseConnection conn, RequestSource source, String action, int mysql_server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLServer(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    // Protect by server
                    int aoServer = getAOServerForMySQLServer(conn, mysql_server);
                    ServerHandler.checkAccessServer(conn, source, action, aoServer);
                }
            } else {
                // Protect by package
                String packageName = getPackageForMySQLServer(conn, mysql_server);
                PackageHandler.checkAccessPackage(conn, source, action, packageName);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessMySQLUser(MasterDatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "checkAccessMySQLUser(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    IntList msus = getMySQLServerUsersForMySQLUser(conn, username);
                    boolean found = false;
                    for(int msu : msus) {
                        int mysqlServer = getMySQLServerForMySQLServerUser(conn, msu);
                        int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
                        if(ServerHandler.canAccessServer(conn, source, aoServer)) {
                            found=true;
                            break;
                        }
                    }
                    if(!found) {
                        String message=
                            "business_administrator.username="
                            +source.getUsername()
                            +" is not allowed to access mysql_user: action='"
                            +action
                            +", username="
                            +username
                        ;
                        MasterServer.reportSecurityMessage(source, message);
                        throw new SQLException(message);
                    }
                }
            } else {
                UsernameHandler.checkAccessUsername(conn, source, action, username);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a MySQL database to the system.
     */
    public static int addMySQLDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name,
        int mysqlServer,
        String packageName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "addMySQLDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,String)", null);
        try {
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);

            PackageHandler.checkPackageAccessServer(conn, source, "addMySQLDatabase", packageName, aoServer);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add MySQLDatabase '"+name+"', Package disabled: "+packageName);

            // Must be a valid name format
            List<String> reservedWords=getReservedWords(conn);
            if(!MySQLDatabaseTable.isValidDatabaseName(name, reservedWords)) throw new SQLException("Invalid MySQL database name: "+name);

            // Must be allowed to access this server and package
            ServerHandler.checkAccessServer(conn, source, "addMySQLDatabase", aoServer);
            PackageHandler.checkAccessPackage(conn, source, "addMySQLDatabase", packageName);

            // Find the accouting code
            String accounting=PackageHandler.getBusinessForPackage(conn, packageName);
            // This sub-account must have access to the server
            BusinessHandler.checkBusinessAccessServer(conn, source, "addMySQLDatabase", accounting, aoServer);

            // Add the entry to the database
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('mysql_databases_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  mysql_databases\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  "+MySQLDatabase.DEFAULT_BACKUP_LEVEL+",\n"
                + "  "+MySQLDatabase.DEFAULT_BACKUP_RETENTION+"\n"
                + ")",
                pkey,
                name,
                mysqlServer,
                packageName
            );

            // Notify all clients of the update, the server will detect this change and automatically add the database
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DATABASES,
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
     * Grants a MySQLServerUser access to a MySQLMasterDatabase.getDatabase().
     */
    public static int addMySQLDBUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int mysql_database,
        int mysql_server_user,
        boolean canSelect,
        boolean canInsert,
        boolean canUpdate,
        boolean canDelete,
        boolean canCreate,
        boolean canDrop,
        boolean canIndex,
        boolean canAlter,
        boolean canCreateTempTable,
        boolean canLockTables,
        boolean canCreateView,
        boolean canShowView,
        boolean canCreateRoutine,
        boolean canAlterRoutine,
        boolean canExecute
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "addMySQLDBUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,int,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean)", null);
        try {
            // Must be allowed to access this database and user
            checkAccessMySQLDatabase(conn, source, "addMySQLDBUser", mysql_database);
            checkAccessMySQLServerUser(conn, source, "addMySQLDBUser", mysql_server_user);
            if(isMySQLServerUserDisabled(conn, mysql_server_user)) throw new SQLException("Unable to add MySQLDBUser, MySQLServerUser disabled: "+mysql_server_user);

            // Must also have matching servers
            int dbServer=getMySQLServerForMySQLDatabase(conn, mysql_database);
            int userServer=getMySQLServerForMySQLServerUser(conn, mysql_server_user);
            if(dbServer!=userServer) throw new SQLException("Mismatched mysql_servers for mysql_databases and mysql_server_users");

            // Add the entry to the database
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('mysql_db_users_pkey_seq')");
            conn.executeUpdate(
                "insert into mysql_db_users values(?,?,?,?,?,?,?,?,?,false,false,?,?,?,?,?,?,?,?,?)",
                pkey,
                mysql_database,
                mysql_server_user,
                canSelect,
                canInsert,
                canUpdate,
                canDelete,
                canCreate,
                canDrop,
                canIndex,
                canAlter,
                canCreateTempTable,
                canLockTables,
                canCreateView,
                canShowView,
                canCreateRoutine,
                canAlterRoutine,
                canExecute
            );

            // Notify all clients of the update, the server will detect this change and automatically update MySQL
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DB_USERS,
                getBusinessForMySQLServerUser(conn, mysql_server_user),
                ServerHandler.getHostnameForServer(conn, getAOServerForMySQLServer(conn, dbServer)),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a MySQL server user.
     */
    public static int addMySQLServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        int mysqlServer,
        String host
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "addMySQLServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,String)", null);
        try {
            checkAccessMySQLUser(conn, source, "addMySQLServerUser", username);
            if(isMySQLUserDisabled(conn, username)) throw new SQLException("Unable to add MySQLServerUser, MySQLUser disabled: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add MySQLServerUser for user '"+LinuxAccount.MAIL+'\'');
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            ServerHandler.checkAccessServer(conn, source, "addMySQLServerUser", aoServer);
            // This sub-account must have access to the server
            UsernameHandler.checkUsernameAccessServer(conn, source, "addMySQLServerUser", username, aoServer);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('mysql_server_users_pkey_seq')");
            conn.executeUpdate(
                "insert into mysql_server_users values(?,?,?,?,null,null,?,?,?,?)",
                pkey,
                username,
                mysqlServer,
                host,
                username.equals(MySQLUser.ROOT)?MySQLServerUser.UNLIMITED_QUESTIONS:MySQLServerUser.DEFAULT_MAX_QUESTIONS,
                username.equals(MySQLUser.ROOT)?MySQLServerUser.UNLIMITED_UPDATES:MySQLServerUser.DEFAULT_MAX_UPDATES,
                username.equals(MySQLUser.ROOT)?MySQLServerUser.UNLIMITED_CONNECTIONS:MySQLServerUser.DEFAULT_MAX_CONNECTIONS,
                username.equals(MySQLUser.ROOT)?MySQLServerUser.UNLIMITED_USER_CONNECTIONS:MySQLServerUser.DEFAULT_MAX_USER_CONNECTIONS
            );

            // Notify all clients of the update
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_SERVER_USERS,
                accounting,
                aoServer,
                true
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a MySQL user.
     */
    public static void addMySQLUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "addMySQLUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "addMySQLUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add MySQLUser, Username disabled: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add MySQLUser for user '"+LinuxAccount.MAIL+'\'');
            if(!MySQLUser.isValidUsername(username)) throw new SQLException("Invalid MySQLUser username: "+username);

            conn.executeUpdate(
                "insert into mysql_users(username) values(?)",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                InvalidateList.allServers,
                true
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Backs up a MySQLDatabase by dumping it and passing the data
     * to the backup server for that database server.
     */
    public static int backupMySQLDatabase(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "backupMySQLDatabase(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMySQLDatabase(conn, source, "backupMySQLDatabase", pkey);

            // Get the backup settings
            short backup_level=conn.executeShortQuery("select backup_level from mysql_databases where pkey=?", pkey);
            if(backup_level==0) throw new SQLException("Unable to backup MySQLDatabase with a backup_level of 0: "+pkey);
            short backup_retention=conn.executeShortQuery("select backup_retention from mysql_databases where pkey=?", pkey);

            // Figure out where the dump is coming from
            int packageNum=getPackageForMySQLDatabase(conn, pkey);
            int mysqlServer=getMySQLServerForMySQLDatabase(conn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            String name=conn.executeStringQuery("select name from mysql_databases where pkey=?", pkey);

            // Stream from one server to the other
            long startTime=System.currentTimeMillis();
            int backupData=BackupHandler.backupDatabase(
                conn,
                backupConn,
                source,
                invalidateList,
                aoServer,
                name+".sql",
                AOServDaemonProtocol.BACKUP_MYSQL_DATABASE,
                pkey
            );
            long endTime=System.currentTimeMillis();

            // Add to the backup database
            int backupPKey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('mysql_backups_pkey_seq')");
            backupConn.executeUpdate(
                "insert into mysql_backups values(?,?,?,?,?,?,?,?,?)",
                backupPKey,
                packageNum,
                name,
                mysqlServer,
                new Timestamp(startTime),
                new Timestamp(endTime),
                backupData,
                backup_level,
                backup_retention
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_BACKUPS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                aoServer,
                false
            );
            return backupPKey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableMySQLServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "disableMySQLServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isMySQLServerUserDisabled(conn, pkey)) throw new SQLException("MySQLServerUser is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableMySQLServerUser", disableLog, false);
            checkAccessMySQLServerUser(conn, source, "disableMySQLServerUser", pkey);

            conn.executeUpdate(
                "update mysql_server_users set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_SERVER_USERS,
                getBusinessForMySQLServerUser(conn, pkey),
                getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableMySQLUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "disableMySQLUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isMySQLUserDisabled(conn, username)) throw new SQLException("MySQLUser is already disabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableMySQLUser", disableLog, false);
            checkAccessMySQLUser(conn, source, "disableMySQLUser", username);
            IntList msus=getMySQLServerUsersForMySQLUser(conn, username);
            for(int c=0;c<msus.size();c++) {
                int msu=msus.getInt(c);
                if(!isMySQLServerUserDisabled(conn, msu)) {
                    throw new SQLException("Cannot disable MySQLUser '"+username+"': MySQLServerUser not disabled: "+msu);
                }
            }

            conn.executeUpdate(
                "update mysql_users set disable_log=? where username=?",
                disableLog,
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Dumps a MySQL database
     */
    public static void dumpMySQLDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int dbPKey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "dumpMySQLDatabase(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,int)", null);
        try {
            checkAccessMySQLDatabase(conn, source, "dumpMySQLDatabase", dbPKey);

            int mysqlServer=getMySQLServerForMySQLDatabase(conn, dbPKey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).dumpMySQLDatabase(dbPKey, out);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableMySQLServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "enableMySQLServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForMySQLServerUser(conn, pkey);
            if(disableLog==-1) throw new SQLException("MySQLServerUser is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableMySQLServerUser", disableLog, true);
            checkAccessMySQLServerUser(conn, source, "enableMySQLServerUser", pkey);
            String mu=getUsernameForMySQLServerUser(conn, pkey);
            if(isMySQLUserDisabled(conn, mu)) throw new SQLException("Unable to enable MySQLServerUser #"+pkey+", MySQLUser not enabled: "+mu);

            conn.executeUpdate(
                "update mysql_server_users set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_SERVER_USERS,
                UsernameHandler.getBusinessForUsername(conn, mu),
                getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableMySQLUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "enableMySQLUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForMySQLUser(conn, username);
            if(disableLog==-1) throw new SQLException("MySQLUser is already enabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableMySQLUser", disableLog, true);
            UsernameHandler.checkAccessUsername(conn, source, "enableMySQLUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable MySQLUser '"+username+"', Username not enabled: "+username);

            conn.executeUpdate(
                "update mysql_users set disable_log=null where username=?",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Generates a unique MySQL database name.
     */
    public static String generateMySQLDatabaseName(
        MasterDatabaseConnection conn,
        String template_base,
        String template_added
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "generateMySQLDatabaseName(MasterDatabaseConnection,String,String)", null);
        try {
            List<String> reservedWords=getReservedWords(conn);

            // Load the entire list of mysql database names
            List<String> names=conn.executeStringListQuery("select name from mysql_databases group by name");
            int size=names.size();

            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            sorted.addAll(names);

            // Find one that is not used
            String goodOne=null;
            for(int c=0;c<Integer.MAX_VALUE;c++) {
                String name= (c==0) ? template_base : (template_base+template_added+c);
                if(!MySQLDatabaseTable.isValidDatabaseName(name, reservedWords)) throw new SQLException("Invalid MySQL database name: "+name);
                if(!sorted.contains(name)) {
                    goodOne=name;
                    break;
                }
            }

            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available MySQL database name for template_base="+template_base+" and template_added="+template_added);
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final Object reservedWordLock=new Object();
    private static List<String> reservedWordCache;
    public static List<String> getReservedWords(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getReservedWords(MasterDatabaseConnection)", null);
        try {
            synchronized(reservedWordLock) {
                if(reservedWordCache==null) {
                    // Load the list of reserved words
                    reservedWordCache=conn.executeStringListQuery("select word from mysql_reserved_words");
                }
                return reservedWordCache;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForMySQLServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getDisableLogForMySQLServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql_server_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForMySQLUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getDisableLogForMySQLUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getUsernameForMySQLServerUser(MasterDatabaseConnection conn, int msu) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getUsernameForMySQLServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select username from mysql_server_users where pkey=?", msu);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getMySQLDatabaseName(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLDatabaseName(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select name from mysql_databases where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, MySQLHandler.class, "invalidateTable(int)", null);
        try {
            switch(tableID) {
                case SchemaTable.MYSQL_RESERVED_WORDS :
                    synchronized(reservedWordLock) {
                        reservedWordCache=null;
                    }
                    break;
                case SchemaTable.MYSQL_SERVER_USERS :
                    synchronized(MySQLHandler.class) {
                        disabledMySQLServerUsers.clear();
                    }
                    break;
                case SchemaTable.MYSQL_USERS :
                    synchronized(MySQLHandler.class) {
                        disabledMySQLUsers.clear();
                    }
                    break;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isMySQLServerUserDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "isMySQLServerUserDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(MySQLHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledMySQLServerUsers.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForMySQLServerUser(conn, pkey)!=-1;
		disabledMySQLServerUsers.put(I, isDisabled?Boolean.TRUE:Boolean.FALSE);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isMySQLUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "isMySQLUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      username\n"
                + "    from\n"
                + "      mysql_users\n"
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

    public static boolean isMySQLUserDisabled(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "isMySQLUserDisabled(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(MySQLHandler.class) {
		Boolean O=disabledMySQLUsers.get(username);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForMySQLUser(conn, username)!=-1;
		disabledMySQLUsers.put(username, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Determines if a MySQL database name is available.
     */
    public static boolean isMySQLDatabaseNameAvailable(
        MasterDatabaseConnection conn,
        RequestSource source,
        String name,
        int mysqlServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "isMySQLDatabaseNameAvailable(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            ServerHandler.checkAccessServer(conn, source, "isMySQLDatabaseNameAvailable", aoServer);
            return conn.executeBooleanQuery("select (select pkey from mysql_databases where name=? and mysql_server=?) is null", name, mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isMySQLServerUserPasswordSet(
        MasterDatabaseConnection conn,
        RequestSource source,
        int msu
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "isMySQLServerUserPasswordSet(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessMySQLServerUser(conn, source, "isMySQLServerUserPasswordSet", msu);
            if(isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to determine if the MySQLServerUser password is set, account disabled: "+msu);
            String username=getUsernameForMySQLServerUser(conn, msu);
            int mysqlServer=getMySQLServerForMySQLServerUser(conn, msu);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            String password=DaemonHandler.getDaemonConnector(conn, aoServer).getEncryptedMySQLUserPassword(mysqlServer, username);
            return !MySQLUser.NO_PASSWORD_DB_VALUE.equals(password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a MySQLBackup from the system.
     */
    public static void removeMySQLBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMySQLBackup(conn, backupConn, source, "removeMySQLBackup", pkey);

            removeMySQLBackup(
                conn,
                backupConn,
                invalidateList,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeMySQLBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLBackup(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=backupConn.executeStringQuery("select pk.accounting from mysql_backups mb, packages pk where mb.pkey=? and mb.package=pk.pkey", pkey);
            int mysqlServer=getMySQLServerForMySQLBackup(backupConn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);

            // Remove the backup database entry
            backupConn.executeUpdate("delete from mysql_backups where pkey=?", pkey);
            
            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_BACKUPS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a MySQLDatabase from the system.
     */
    public static void removeMySQLDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, MySQLHandler.class, "removeMySQLDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMySQLDatabase(conn, source, "removeMySQLDatabase", pkey);

            removeMySQLDatabase(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes a MySQLDatabase from the system.
     */
    public static void removeMySQLDatabase(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLDatabase(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Cannot remove the mysql database
            String dbName=getMySQLDatabaseName(conn, pkey);
            if(dbName.equals(MySQLDatabase.MYSQL)) throw new SQLException("Not allowed to remove the database named '"+MySQLDatabase.MYSQL+'\'');

            // Remove the mysql_db_user entries
            List<String> dbUserAccounts=conn.executeStringListQuery(
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  mysql_db_users mdu,\n"
                + "  mysql_server_users msu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  mdu.mysql_database=?\n"
                + "  and mdu.mysql_user=msu.pkey\n"
                + "  and msu.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "group by\n"
                + "  pk.accounting",
                pkey
            );
            if(dbUserAccounts.size()>0) conn.executeUpdate("delete from mysql_db_users where mysql_database=?", pkey);

            // Remove the database entry
            String accounting=getBusinessForMySQLDatabase(conn, pkey);
            int mysqlServer=getMySQLServerForMySQLDatabase(conn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            conn.executeUpdate("delete from mysql_databases where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DATABASES,
                accounting,
                aoServer,
                false
            );
            if(dbUserAccounts.size()>0) invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DB_USERS,
                dbUserAccounts,
                aoServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a MySQLDBUser from the system.
     */
    public static void removeMySQLDBUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLDBUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMySQLDBUser(conn, source, "removeMySQLDBUser", pkey);

            // Remove the mysql_db_user
            String accounting=getBusinessForMySQLDBUser(conn, pkey);
            int mysqlServer=getMySQLServerForMySQLDBUser(conn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            conn.executeUpdate("delete from mysql_db_users where pkey=?", pkey);

            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DB_USERS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a MySQLServerUser from the system.
     */
    public static void removeMySQLServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMySQLServerUser(conn, source, "removeMySQLServerUser", pkey);

            String username=getUsernameForMySQLServerUser(conn, pkey);
            if(username.equals(MySQLUser.ROOT)) throw new SQLException("Not allowed to remove MySQLServerUser for user '"+MySQLUser.ROOT+'\'');

            // Remove the mysql_db_user
            boolean dbUsersExist=conn.executeBooleanQuery("select (select pkey from mysql_db_users where mysql_user=? limit 1) is not null", pkey);
            if(dbUsersExist) conn.executeUpdate("delete from mysql_db_users where mysql_user=?", pkey);

            // Remove the mysql_server_user
            String accounting=getBusinessForMySQLServerUser(conn, pkey);
            int mysqlServer=getMySQLServerForMySQLServerUser(conn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            conn.executeUpdate("delete from mysql_server_users where pkey=?", pkey);

            // Notify all clients of the updates
            if(dbUsersExist) invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DB_USERS,
                accounting,
                aoServer,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_SERVER_USERS,
                accounting,
                aoServer,
                true
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a MySQLUser from the system.
     */
    public static void removeMySQLUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, MySQLHandler.class, "removeMySQLUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            checkAccessMySQLUser(conn, source, "removeMySQLUser", username);
            
            removeMySQLUser(conn, invalidateList, username);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes a MySQLUser from the system.
     */
    public static void removeMySQLUser(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeMySQLUser(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            if(username.equals(MySQLUser.ROOT)) throw new SQLException("Not allowed to remove MySQLUser for user '"+MySQLUser.ROOT+'\'');

            String accounting=UsernameHandler.getBusinessForUsername(conn, username);

            // Remove the mysql_db_user
            IntList dbUserServers=conn.executeIntListQuery(
		"select\n"
		+ "  md.mysql_server\n"
		+ "from\n"
		+ "  mysql_server_users msu,\n"
		+ "  mysql_db_users mdu,\n"
		+ "  mysql_databases md\n"
		+ "where\n"
		+ "  msu.username=?\n"
		+ "  and msu.pkey=mdu.mysql_user\n"
		+ "  and mdu.mysql_database=md.pkey\n"
		+ "group by\n"
		+ "  md.mysql_server",
		username
	    );
            if(dbUserServers.size()>0) {
                conn.executeUpdate(
                    "delete from\n"
                    + "  mysql_db_users\n"
                    + "where\n"
                    + "  pkey in (\n"
                    + "    select\n"
                    + "      mdu.pkey\n"
                    + "    from\n"
                    + "      mysql_server_users msu,\n"
                    + "      mysql_db_users mdu\n"
                    + "    where\n"
                    + "      msu.username=?\n"
                    + "      and msu.pkey=mdu.mysql_user"
                    + "  )",
                    username
                );
                for(int mysqlServer : dbUserServers) {
                    invalidateList.addTable(
                        conn,
                        SchemaTable.MYSQL_DB_USERS,
                        accounting,
                        getAOServerForMySQLServer(conn, mysqlServer),
                        false
                    );
                }
            }

            // Remove the mysql_server_user
            IntList mysqlServers=conn.executeIntListQuery("select mysql_server from mysql_server_users where username=?", username);
            if(mysqlServers.size()>0) {
                conn.executeUpdate("delete from mysql_server_users where username=?", username);
                for(int mysqlServer : mysqlServers) {
                    invalidateList.addTable(
                        conn,
                        SchemaTable.MYSQL_SERVER_USERS,
                        accounting,
                        getAOServerForMySQLServer(conn, mysqlServer),
                        false
                    );
                }
            }

            // Remove the mysql_user
            conn.executeUpdate("delete from mysql_users where username=?", username);
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_USERS,
                accounting,
                BusinessHandler.getServersForBusiness(conn, accounting),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setMySQLDatabaseBackupRetention(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        short days
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "setMySQLDatabaseBackupRetention(MasterDatabaseConnection,RequestSource,InvalidateList,int,short)", null);
        try {
            // Security checks
            checkAccessMySQLDatabase(conn, source, "setMySQLDatabaseBackupRetention", pkey);

            // Update the database
            conn.executeUpdate(
                "update mysql_databases set backup_retention=?::smallint where pkey=?",
                days,
                pkey
            );

            int mysqlServer=getMySQLServerForMySQLDatabase(conn, pkey);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_DATABASES,
                getBusinessForMySQLDatabase(conn, pkey),
                aoServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets a MySQL password.
     */
    public static void setMySQLServerUserPassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        int mysql_server_user,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "setMySQLServerUserPassword(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            BusinessHandler.checkPermission(conn, source, "setMySQLServerUserPassword", AOServPermission.SET_MYSQL_SERVER_USER_PASSWORD);
            checkAccessMySQLServerUser(conn, source, "setMySQLServerUserPassword", mysql_server_user);
            if(isMySQLServerUserDisabled(conn, mysql_server_user)) throw new SQLException("Unable to set MySQLServerUser password, account disabled: "+mysql_server_user);

            // Get the server, username for the user
            String username=getUsernameForMySQLServerUser(conn, mysql_server_user);

            // No setting the super user password
            if(username.equals(MySQLUser.ROOT)) throw new SQLException("The MySQL "+MySQLUser.ROOT+" password may not be set.");

            // Perform the password check here, too.
            if(password!=null && password.length()==0) password=MySQLUser.NO_PASSWORD;
            if(password!=MySQLUser.NO_PASSWORD) {
                PasswordChecker.Result[] results = MySQLUser.checkPassword(username, password);
                if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results, Locale.getDefault()).replace('\n', '|'));
            }

            // Contact the daemon for the update
            int mysqlServer=getMySQLServerForMySQLServerUser(conn, mysql_server_user);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            DaemonHandler.getDaemonConnector(
                conn,
                aoServer
            ).setMySQLUserPassword(mysqlServer, username, password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setMySQLServerUserPredisablePassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int msu,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "setMySQLServerUserPredisablePassword(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessMySQLServerUser(conn, source, "setMySQLServerUserPredisablePassword", msu);
            if(password==null) {
                if(isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to clear MySQLServerUser predisable password, account disabled: "+msu);
            } else {
                if(!isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to set MySQLServerUser predisable password, account not disabled: "+msu);
            }

            // Update the database
            conn.executeUpdate(
                "update mysql_server_users set predisable_password=? where pkey=?",
                password,
                msu
            );
            
            int mysqlServer=getMySQLServerForMySQLServerUser(conn, msu);
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            invalidateList.addTable(
                conn,
                SchemaTable.MYSQL_SERVER_USERS,
                getBusinessForMySQLServerUser(conn, msu),
                aoServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLDatabaseRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "waitForMySQLDatabaseRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForMySQLDatabaseRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDatabaseRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLDBUserRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "waitForMySQLDBUserRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForMySQLDBUserRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDBUserRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLUserRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "waitForMySQLUserRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForMySQLUserRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLUserRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForMySQLDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getBusinessForMySQLDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from mysql_databases md, packages pk where md.package=pk.name and md.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForMySQLDBUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getBusinessForMySQLDBUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  mysql_db_users mdu,\n"
                + "  mysql_server_users msu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  mdu.pkey=?\n"
                + "  and mdu.mysql_user=msu.pkey\n"
                + "  and msu.username=un.username\n"
                + "  and un.package=pk.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForMySQLServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getBusinessForMySQLServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  mysql_server_users msu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  msu.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and msu.pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPackageForMySQLDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getPackageForMySQLDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select pk.pkey from mysql_databases md, packages pk where md.pkey=? and md.package=pk.name", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getMySQLServerUsersForMySQLUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerUsersForMySQLUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery("select pkey from mysql_server_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerForMySQLDatabase(MasterDatabaseConnection conn, int mysql_database) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerForMySQLDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select mysql_server from mysql_databases where pkey=?", mysql_database);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerForMySQLBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerForMySQLBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeIntQuery("select mysql_server from mysql_backups where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForMySQLServer(MasterDatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getAOServerForMySQLServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from mysql_servers where pkey=?", mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForMySQLServer(MasterDatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getPackageForMySQLServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select package from mysql_servers where pkey=?", mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPortForMySQLServer(MasterDatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getAOServerForMySQLServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select nb.port from mysql_servers ms inner join net_binds nb on ms.net_bind=nb.pkey where ms.pkey=?", mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerForFailoverMySQLReplication(MasterDatabaseConnection conn, int failoverMySQLReplication) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerForFailoverMySQLReplication(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select mysql_server from failover_mysql_replications where pkey=?", failoverMySQLReplication);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPackageForMySQLBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getPackageForMySQLBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeIntQuery("select package from mysql_backups where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerForMySQLDBUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerForMySQLDBUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select msu.mysql_server from mysql_db_users mdu, mysql_server_users msu where mdu.pkey=? and mdu.mysql_user=msu.pkey", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLDatabaseForMySQLDBUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLDatabaseForMySQLDBUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select mysql_database from mysql_db_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerUserForMySQLDBUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerUserForMySQLDBUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select mysql_user from mysql_db_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMySQLServerForMySQLServerUser(MasterDatabaseConnection conn, int mysql_server_user) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "getMySQLServerForMySQLServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select mysql_server from mysql_server_users where pkey=?", mysql_server_user);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeExpiredMySQLBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "removeExpiredMySQLBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("non-master user "+username+" not allowed to removeExpiredMySQLBackups");
            ServerHandler.checkAccessServer(conn, source, "removeExpiredMySQLBackups", aoServer);

            // Get the list of pkeys that should be removed
            IntList pkeys=backupConn.executeIntListQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                "select\n"
                + "  mb.pkey\n"
                + "from\n"
                + "  mysql_servers ms,\n"
                + "  mysql_backups mb\n"
                + "where\n"
                + "  ms.ao_server=?\n"
                + "  and ms.pkey=mb.mysql_server\n"
                + "  and now()>=(mb.end_time+(mb.backup_retention || ' days')::interval)",
                aoServer
            );

            // Remove each file
            int size=pkeys.size();
            for(int c=0;c<size;c++) {
                removeMySQLBackup(
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

    public static void restartMySQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int mysqlServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "restartMySQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "mysql");
            if(!canControl) throw new SQLException("Not allowed to restart MySQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartMySQL(mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startMySQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "startMySQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "mysql");
            if(!canControl) throw new SQLException("Not allowed to start MySQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startMySQL();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void stopMySQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, MySQLHandler.class, "stopMySQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "mysql");
            if(!canControl) throw new SQLException("Not allowed to stop MySQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopMySQL();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void getSlaveStatus(
        MasterDatabaseConnection conn,
        RequestSource source,
        int failoverMySQLReplication,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setPostgresServerUserPassword", AOServPermission.GET_MYSQL_SLAVE_STATUS);
        // Check access
        int mysqlServer = getMySQLServerForFailoverMySQLReplication(conn, failoverMySQLReplication);
        checkAccessMySQLServer(conn, source, "getSlaveStatus", mysqlServer);
        int failoverServer = conn.executeIntQuery("select ffr.to_server from failover_mysql_replications fmr inner join failover_file_replications ffr on fmr.replication=ffr.pkey where fmr.pkey=?", failoverMySQLReplication);
        if(DaemonHandler.isDaemonAvailable(failoverServer)) {
            try {
                String toPath = conn.executeStringQuery("select ffr.to_path from failover_mysql_replications fmr inner join failover_file_replications ffr on fmr.replication=ffr.pkey where fmr.pkey=?", failoverMySQLReplication);
                int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
                FailoverMySQLReplication.SlaveStatus slaveStatus = DaemonHandler.getDaemonConnector(conn, failoverServer).getMySQLSlaveStatus(
                    toPath+"/"+ServerHandler.getHostnameForServer(conn, aoServer),
                    ServerHandler.getOperatingSystemVersionForServer(conn, aoServer),
                    getPortForMySQLServer(conn, mysqlServer)
                );
                if(slaveStatus==null) out.writeByte(AOServProtocol.DONE);
                else {
                    out.writeByte(AOServProtocol.NEXT);
                    out.writeNullUTF(slaveStatus.getSlaveIOState());
                    out.writeNullUTF(slaveStatus.getMasterLogFile());
                    out.writeNullUTF(slaveStatus.getReadMasterLogPos());
                    out.writeNullUTF(slaveStatus.getRelayLogFile());
                    out.writeNullUTF(slaveStatus.getRelayLogPos());
                    out.writeNullUTF(slaveStatus.getRelayMasterLogFile());
                    out.writeNullUTF(slaveStatus.getSlaveIORunning());
                    out.writeNullUTF(slaveStatus.getSlaveSQLRunning());
                    out.writeNullUTF(slaveStatus.getLastErrno());
                    out.writeNullUTF(slaveStatus.getLastError());
                    out.writeNullUTF(slaveStatus.getSkipCounter());
                    out.writeNullUTF(slaveStatus.getExecMasterLogPos());
                    out.writeNullUTF(slaveStatus.getRelayLogSpace());
                    out.writeNullUTF(slaveStatus.getSecondsBehindMaster());
                }
            } catch(IOException err) {
                DaemonHandler.flagDaemonAsDown(failoverServer);
                throw err;
            }
        } else {
            throw new IOException("Server unavailable");
        }
    }
}
