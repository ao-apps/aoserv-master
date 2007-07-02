package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.md5.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>PostgresHandler</code> handles all the accesses to the PostgreSQL tables.
 *
 * @author  AO Industries, Inc.
 */
final public class PostgresHandler {

    private final static Map<Integer,Boolean> disabledPostgresServerUsers=new HashMap<Integer,Boolean>();
    private final static Map<String,Boolean> disabledPostgresUsers=new HashMap<String,Boolean>();

    public static void checkAccessPostgresBackup(MasterDatabaseConnection conn, BackupDatabaseConnection backupConn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "checkAccessPostgresBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(MasterServer.getMasterUser(conn, source.getUsername())!=null) checkAccessPostgresServer(conn, source, action, getPostgresServerForPostgresBackup(backupConn, pkey));
            else PackageHandler.checkAccessPackage(conn, source, action, getPackageForPostgresBackup(backupConn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessPostgresDatabase(MasterDatabaseConnection conn, RequestSource source, String action, int postgres_database) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "checkAccessPostgresDatabase(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresDatabase(conn, postgres_database));
                }
            } else {
                checkAccessPostgresServerUser(conn, source, action, getDatDbaForPostgresDatabase(conn, postgres_database));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessPostgresServer(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "checkAccessPostgresServer(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresServer(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessPostgresServerUser(MasterDatabaseConnection conn, RequestSource source, String action, int postgres_server_user) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "checkAccessPostgresServerUser(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForPostgresServerUser(conn, postgres_server_user));
                }
            } else {
                checkAccessPostgresUser(conn, source, action, getUsernameForPostgresServerUser(conn, postgres_server_user));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessPostgresUser(MasterDatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "checkAccessPostgresUser(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    IntList psus = getPostgresServerUsersForPostgresUser(conn, username);
                    boolean found = false;
                    for(int psu : psus) {
                        if(ServerHandler.canAccessServer(conn, source, getAOServerForPostgresServerUser(conn, psu))) {
                            found=true;
                            break;
                        }
                    }
                    if(!found) {
                        String message=
                            "business_administrator.username="
                            +source.getUsername()
                            +" is not allowed to access postgres_user: action='"
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
     * Adds a PostgreSQL database to the system.
     */
    public static int addPostgresDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name,
        int postgresServer,
        int datdba,
        int encoding,
        boolean enable_postgis
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "addPostgresDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,int,int,boolean)", null);
        try {
            List<String> reservedWords=getReservedWords(conn);
            // Must be a valid name format
            if(!PostgresDatabaseTable.isValidDatabaseName(
                name,
                reservedWords
            )) throw new SQLException("Invalid PostgreSQL database name: "+name);
            // If requesting PostGIS, make sure the version of PostgreSQL supports it.
            if(
                enable_postgis
                && conn.executeBooleanQuery("select pv.postgis_version is null from postgres_servers ps inner join postgres_versions pv on ps.version=pv.version where ps.pkey=?", postgresServer)
            ) throw new SQLException("This version of PostgreSQL doesn't support PostGIS");

            // datdba must be on the same server and not be 'mail'
            int datdbaServer=getPostgresServerForPostgresServerUser(conn, datdba);
            if(datdbaServer!=postgresServer) throw new SQLException("(datdba.postgres_server="+datdbaServer+")!=(postgres_server="+postgresServer+")");
            String datdbaUsername=getUsernameForPostgresServerUser(conn, datdba);
            if(datdbaUsername.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresDatabase with datdba of '"+LinuxAccount.MAIL+'\'');
            if(isPostgresServerUserDisabled(conn, datdba)) throw new SQLException("Unable to add PostgresDatabase, PostgresServerUser disabled: "+datdba);
            // Look up the accounting code
            String accounting=UsernameHandler.getBusinessForUsername(conn, datdbaUsername);
            // Encoding must exist for this version of the database
            if(
                !conn.executeBooleanQuery(
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      pe.pkey\n"
                    + "    from\n"
                    + "      postgres_servers ps,\n"
                    + "      postgres_encodings pe\n"
                    + "    where\n"
                    + "      ps.pkey=?\n"
                    + "      and ps.version=pe.postgres_version\n"
                    + "      and pe.pkey=?\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    postgresServer,
                    encoding
                )
            ) throw new SQLException("PostgresServer #"+postgresServer+" does not support PostgresEncoding #"+encoding);

            // Must be allowed to access this server and package
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            ServerHandler.checkAccessServer(conn, source, "addPostgresDatabase", aoServer);
            UsernameHandler.checkAccessUsername(conn, source, "addPostgresDatabase", datdbaUsername);
            // This sub-account must have access to the server
            BusinessHandler.checkBusinessAccessServer(conn, source, "addPostgresDatabase", accounting, aoServer);

            // Add the entry to the database
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('postgres_databases_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  postgres_databases\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  false,\n"
                + "  true,\n"
                + "  "+PostgresDatabase.DEFAULT_BACKUP_LEVEL+",\n"
                + "  "+PostgresDatabase.DEFAULT_BACKUP_RETENTION+"\n,"
                + "  ?\n"
                + ")",
                pkey,
                name,
                postgresServer,
                datdba,
                encoding,
                enable_postgis
            );

            // Notify all clients of the update, the server will detect this change and automatically add the database
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_DATABASES,
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
     * Adds a PostgreSQL server user.
     */
    public static int addPostgresServerUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username, 
        int postgresServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "addPostgresServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,String,int)", null);
        try {
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresServerUser for user '"+LinuxAccount.MAIL+'\'');

            checkAccessPostgresUser(conn, source, "addPostgresServerUser", username);
            if(isPostgresUserDisabled(conn, username)) throw new SQLException("Unable to add PostgresServerUser, PostgresUser disabled: "+username);
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            ServerHandler.checkAccessServer(conn, source, "addPostgresServerUser", aoServer);
            // This sub-account must have access to the server
            UsernameHandler.checkUsernameAccessServer(conn, source, "addPostgresServerUser", username, aoServer);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('postgres_server_users_pkey_seq')");

            conn.executeUpdate(
                "insert into postgres_server_users values(?,?,?,null,null)",
                pkey,
                username,
                postgresServer
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_SERVER_USERS,
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
     * Adds a PostgreSQL user.
     */
    public static void addPostgresUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "addPostgresUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add PostgresUser for user '"+LinuxAccount.MAIL+'\'');
            UsernameHandler.checkAccessUsername(conn, source, "addPostgresUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add PostgresUser, Username disabled: "+username);
            if(!PostgresUser.isValidUsername(username)) throw new SQLException("Invalid PostgresUser username: "+username);

            conn.executeUpdate(
                "insert into postgres_users(username) values(?)",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                InvalidateList.allServers,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Backs up a PostgresDatabase by dumping it and passing the data
     * to the backup server for that database server.
     */
    public static int backupPostgresDatabase(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "backupPostgresDatabase(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessPostgresDatabase(conn, source, "backupPostgresDatabase", pkey);

            // Get the backup_retention
            short backup_level=conn.executeShortQuery("select backup_level from postgres_databases where pkey=?", pkey);
            if(backup_level==0) throw new SQLException("Unable to backup PostgresDatabase with a backup_level of 0: "+pkey);
            short backup_retention=conn.executeShortQuery("select backup_retention from postgres_databases where pkey=?", pkey);

            // Figure out where the dump is coming from
            int packageNum=getPackageForPostgresDatabase(conn, pkey);
            int postgresServer=getPostgresServerForPostgresDatabase(conn, pkey);
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            String name=conn.executeStringQuery("select name from postgres_databases where pkey=?", pkey);
            String minorVersion=getMinorVersionForPostgresServer(conn, postgresServer);
            int port=getPortForPostgresServer(conn, postgresServer);

            // Stream from one server to the other
            long startTime=System.currentTimeMillis();
            AOServDaemonConnector connector=DaemonHandler.getDaemonConnector(conn, aoServer);

            int backupData;

            // Establish the connection to the source
            AOServDaemonConnection sourceConn=connector.getConnection();
            try {
                CompressedDataOutputStream sourceOut=sourceConn.getOutputStream();
                sourceOut.writeCompressedInt(AOServDaemonProtocol.BACKUP_POSTGRES_DATABASE);
                sourceOut.writeUTF(minorVersion);
                sourceOut.writeUTF(name);
                sourceOut.writeCompressedInt(port);
                sourceOut.flush();

                CompressedDataInputStream sourceIn=sourceConn.getInputStream();
                int sourceCode=sourceIn.read();
                if(sourceCode!=AOServDaemonProtocol.NEXT) {
                    if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(sourceIn.readUTF());
                    if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(sourceIn.readUTF());
                    throw new IOException("Unknown result: " + sourceCode);
                }
                long dataSize=sourceIn.readLong();
                long compressedSize=sourceIn.readLong();
                long md5_hi=sourceIn.readLong();
                long md5_lo=sourceIn.readLong();

                Object[] OA=BackupHandler.findOrAddBackupData(conn, backupConn, source, invalidateList, aoServer, dataSize, md5_hi, md5_lo);
                backupData=((Integer)OA[0]).intValue();
                boolean hasData=((Boolean)OA[1]).booleanValue();
                if(!hasData) {
                    int toServer=((Integer)OA[2]).intValue();
                    int backupPartition=((Integer)OA[3]).intValue();
                    String relativePath = BackupData.getRelativePathPrefix(backupData) + name + ".sql.gz";
                    long key=DaemonHandler.grantDaemonAccess(conn, toServer, AOServDaemonProtocol.BACKUP_POSTGRES_DATABASE_SEND_DATA, relativePath, Integer.toString(backupPartition), MD5.getMD5String(md5_hi, md5_lo));
                    sourceOut.writeBoolean(true);
                    sourceOut.writeLong(key);
                    sourceOut.writeCompressedInt(toServer);
                    sourceOut.writeUTF(DaemonHandler.getDaemonConnectorIP(conn, toServer));
                    sourceOut.writeCompressedInt(DaemonHandler.getDaemonConnectorPort(conn, toServer));
                    sourceOut.writeUTF(DaemonHandler.getDaemonConnectorProtocol(conn, toServer));
                    sourceOut.writeCompressedInt(DaemonHandler.getDaemonConnectorPoolSize(conn, toServer));
                    sourceOut.flush();
                    sourceCode = sourceIn.read();
                    if(sourceCode!=AOServDaemonProtocol.DONE) {
                        if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(sourceIn.readUTF());
                        if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(sourceIn.readUTF());
                        throw new IOException("Unknown result: " + sourceCode);
                    }
                    backupConn.executeUpdate(
                        "update backup_data set compressed_size=?, is_stored=true where pkey=?",
                        compressedSize,
                        backupData
                    );
                } else {
                    sourceOut.writeBoolean(false);
                    sourceOut.flush();
                }
            } catch(IOException err) {
                sourceConn.close();
                throw err;
            } catch(SQLException err) {
                sourceConn.close();
                throw err;
            } finally {
                connector.releaseConnection(sourceConn);
            }
            long endTime=System.currentTimeMillis();

            // Add to the backup database
            int backupPKey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('postgres_backups_pkey_seq')");
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into postgres_backups values(?,?,?,?,?,?,?,?,?)");
            try {
                pstmt.setInt(1, backupPKey);
                pstmt.setInt(2, packageNum);
                pstmt.setString(3, name);
                pstmt.setInt(4, postgresServer);
                pstmt.setTimestamp(5, new Timestamp(startTime));
                pstmt.setTimestamp(6, new Timestamp(endTime));
                pstmt.setInt(7, backupData);
                pstmt.setShort(8, backup_level);
                pstmt.setShort(9, backup_retention);
                backupConn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_BACKUPS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            return backupPKey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disablePostgresServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "disablePostgresServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isPostgresServerUserDisabled(conn, pkey)) throw new SQLException("PostgresServerUser is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disablePostgresServerUser", disableLog, false);
            checkAccessPostgresServerUser(conn, source, "disablePostgresServerUser", pkey);

            conn.executeUpdate(
                "update postgres_server_users set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_SERVER_USERS,
                getBusinessForPostgresServerUser(conn, pkey),
                ServerHandler.getHostnameForServer(conn, getAOServerForPostgresServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disablePostgresUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "disablePostgresUser(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isPostgresUserDisabled(conn, username)) throw new SQLException("PostgresUser is already disabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "disablePostgresUser", disableLog, false);
            checkAccessPostgresUser(conn, source, "disablePostgresUser", username);
            IntList psus=getPostgresServerUsersForPostgresUser(conn, username);
            for(int c=0;c<psus.size();c++) {
                int psu=psus.getInt(c);
                if(!isPostgresServerUserDisabled(conn, psu)) {
                    throw new SQLException("Cannot disable PostgresUser '"+username+"': PostgresServerUser not disabled: "+psu);
                }
            }

            conn.executeUpdate(
                "update postgres_users set disable_log=? where username=?",
                disableLog,
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Dumps a PostgreSQL database
     */
    public static void dumpPostgresDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int dbPKey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "dumpPostgresDatabase(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,int)", null);
        try {
            checkAccessPostgresDatabase(conn, source, "dumpPostgresDatabase", dbPKey);

            int aoServer=getAOServerForPostgresDatabase(conn, dbPKey);
            DaemonHandler.getDaemonConnector(conn, aoServer).dumpPostgresDatabase(dbPKey, out);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enablePostgresServerUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "enablePostgresServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForPostgresServerUser(conn, pkey);
            if(disableLog==-1) throw new SQLException("PostgresServerUser is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enablePostgresServerUser", disableLog, true);
            checkAccessPostgresServerUser(conn, source, "enablePostgresServerUser", pkey);
            String pu=getUsernameForPostgresServerUser(conn, pkey);
            if(isPostgresUserDisabled(conn, pu)) throw new SQLException("Unable to enable PostgresServerUser #"+pkey+", PostgresUser not enabled: "+pu);

            conn.executeUpdate(
                "update postgres_server_users set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_SERVER_USERS,
                UsernameHandler.getBusinessForUsername(conn, pu),
                ServerHandler.getHostnameForServer(conn, getAOServerForPostgresServerUser(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enablePostgresUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "enablePostgresUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForPostgresUser(conn, username);
            if(disableLog==-1) throw new SQLException("PostgresUser is already enabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "enablePostgresUser", disableLog, true);
            UsernameHandler.checkAccessUsername(conn, source, "enablePostgresUser", username);
            if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable PostgresUser '"+username+"', Username not enabled: "+username);

            conn.executeUpdate(
                "update postgres_users set disable_log=null where username=?",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Generates a unique PostgreSQL database name.
     */
    public static String generatePostgresDatabaseName(
        MasterDatabaseConnection conn,
        String template_base,
        String template_added
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "generatePostgresDatabaseName(MasterDatabaseConnection,String,String)", null);
        try {
            // Load the list of reserved words
            List<String> reservedWords=getReservedWords(conn);

            // Load the entire list of postgres database names
            List<String> names=conn.executeStringListQuery("select name from postgres_databases group by name");
            int size=names.size();

            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            sorted.addAll(names);

            // Find one that is not used
            String goodOne=null;
            for(int c=0;c<Integer.MAX_VALUE;c++) {
                String name= (c==0) ? template_base : (template_base+template_added+c);
                if(!PostgresDatabaseTable.isValidDatabaseName(name, reservedWords)) throw new SQLException("Invalid PostgreSQL database name: "+name);
                if(!sorted.contains(name)) {
                    goodOne=name;
                    break;
                }
            }

            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available PostgreSQL database name for template_base="+template_base+" and template_added="+template_added);
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForPostgresServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getDisableLogForPostgresServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from postgres_server_users where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForPostgresUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getDisableLogForPostgresUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from postgres_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getPostgresServerUsersForPostgresUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPostgresServerUsersForPostgresUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery("select pkey from postgres_server_users where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final Object reservedWordLock=new Object();
    private static List<String> reservedWordCache;
    public static List<String> getReservedWords(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getReservedWords(MasterDatabaseConnection)", null);
        try {
            synchronized(reservedWordLock) {
                if(reservedWordCache==null) {
                    // Load the list of reserved words
                    reservedWordCache=conn.executeStringListQuery("select word from postgres_reserved_words");
                }
                return reservedWordCache;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getUsernameForPostgresServerUser(MasterDatabaseConnection conn, int psu) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getUsernameForPostgresServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select username from postgres_server_users where pkey=?", psu);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, PostgresHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
            switch(tableID) {
                case POSTGRES_RESERVED_WORDS :
                    synchronized(reservedWordLock) {
                        reservedWordCache=null;
                    }
                    break;
                case POSTGRES_SERVER_USERS :
                    synchronized(PostgresHandler.class) {
                        disabledPostgresServerUsers.clear();
                    }
                    break;
                case POSTGRES_USERS :
                    synchronized(PostgresHandler.class) {
                        disabledPostgresUsers.clear();
                    }
                    break;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isPostgresServerUserDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresServerUserDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(PostgresHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledPostgresServerUsers.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForPostgresServerUser(conn, pkey)!=-1;
		disabledPostgresServerUsers.put(I, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isPostgresUser(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresUser(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      username\n"
                + "    from\n"
                + "      postgres_users\n"
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

    public static boolean isPostgresUserDisabled(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresUserDisabled(Connectino[],String)", null);
        try {
	    synchronized(PostgresHandler.class) {
		Boolean O=disabledPostgresUsers.get(username);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForPostgresUser(conn, username)!=-1;
		disabledPostgresUsers.put(username, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Determines if a PostgreSQL database name is available.
     */
    public static boolean isPostgresDatabaseNameAvailable(
        MasterDatabaseConnection conn,
        RequestSource source,
        String name,
        int postgresServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresDatabaseNameAvailable(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            ServerHandler.checkAccessServer(
                conn,
                source,
                "isPostgresDatabaseNameAvailable",
                aoServer
            );
            return conn.executeBooleanQuery(
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      pkey\n"
                + "    from\n"
                + "      postgres_databases\n"
                + "    where\n"
                + "      name=?\n"
                + "      and postgres_server=?\n"
                + "    limit 1\n"
                + "  ) is null",
                name,
                postgresServer
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Determines if a PostgreSQL server name is available.
     */
    public static boolean isPostgresServerNameAvailable(
        MasterDatabaseConnection conn,
        RequestSource source,
        String name,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresServerNameAvailable(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, "isPostgresServerNameAvailable", aoServer);
            return conn.executeBooleanQuery("select (select pkey from postgres_servers where name=? and ao_server=? limit 1) is null", name, aoServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isPostgresServerUserPasswordSet(
        MasterDatabaseConnection conn,
        RequestSource source, 
        int psu
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "isPostgresServerUserPasswordSet(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessPostgresServerUser(conn, source, "isPostgresServerUserPasswordSet", psu);
            if(isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to determine if PostgresServerUser password is set, account disabled: "+psu);
            String username=getUsernameForPostgresServerUser(conn, psu);

            int aoServer=getAOServerForPostgresServerUser(conn, psu);
            String password=DaemonHandler.getDaemonConnector(conn, aoServer).getPostgresUserPassword(psu);
            return !PostgresUser.NO_PASSWORD_DB_VALUE.equals(password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresBackup from the system.
     */
    public static void removePostgresBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessPostgresBackup(conn, backupConn, source, "removePostgresBackup", pkey);

            removePostgresBackup(
                conn,
                backupConn,
                invalidateList,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removePostgresBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresBackup(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,pkey)", null);
        try {
            String accounting=backupConn.executeStringQuery("select pk.accounting from postgres_backups pb, packages pk where pb.pkey=? and pb.package=pk.pkey", pkey);
            int dbServer=backupConn.executeIntQuery(
                "select\n"
                + "  ps.ao_server\n"
                + "from\n"
                + "  postgres_backups pb,\n"
                + "  postgres_servers ps\n"
                + "where\n"
                + "  pb.pkey=?\n"
                + "  and pb.postgres_server=ps.pkey",
                pkey
            );

            // Remove the backup database entry
            backupConn.executeUpdate("delete from postgres_backups where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_BACKUPS,
                accounting,
                ServerHandler.getHostnameForServer(conn, dbServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresDatabase from the system.
     */
    public static void removePostgresDatabase(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresDatabase(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessPostgresDatabase(conn, source, "removePostgresDatabase", pkey);

            removePostgresDatabase(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresDatabase from the system.
     */
    public static void removePostgresDatabase(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresDatabase(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Remove the database entry
            String accounting=getBusinessForPostgresDatabase(conn, pkey);
            int aoServer=getAOServerForPostgresDatabase(conn, pkey);
            conn.executeUpdate("delete from postgres_databases where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_DATABASES,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresServerUser from the system.
     */
    public static void removePostgresServerUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresServerUser(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessPostgresServerUser(conn, source, "removePostgresServerUser", pkey);

            String username=getUsernameForPostgresServerUser(conn, pkey);
            if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("Not allowed to remove PostgresUser for user '"+PostgresUser.POSTGRES+'\'');

            // Get the details for later use
            int aoServer=getAOServerForPostgresServerUser(conn, pkey);
            String accounting=getBusinessForPostgresServerUser(conn, pkey);

            // Make sure that this is not the DBA for any databases
            int count=conn.executeIntQuery("select count(*) from postgres_databases where datdba=?", pkey);
            if(count>0) throw new SQLException("PostgresServerUser #"+pkey+" cannot be removed because it is the datdba for "+count+(count==1?" database":" databases"));

            // Remove the postgres_server_user
            conn.executeUpdate("delete from postgres_server_users where pkey=?", pkey);

            // Notify all clients of the updates
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_SERVER_USERS,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                true
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresUser from the system.
     */
    public static void removePostgresUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            checkAccessPostgresUser(conn, source, "removePostgresUser", username);

            removePostgresUser(conn, invalidateList, username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a PostgresUser from the system.
     */
    public static void removePostgresUser(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removePostgresUser(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("Not allowed to remove PostgresUser named '"+PostgresUser.POSTGRES+'\'');
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);

            // Remove the postgres_server_user
            IntList aoServers=conn.executeIntListQuery("select ps.ao_server from postgres_server_users psu, postgres_servers ps where psu.username=? and psu.postgres_server=ps.pkey", username);
            if(aoServers.size()>0) {
                conn.executeUpdate("delete from postgres_server_users where username=?", username);
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.POSTGRES_SERVER_USERS,
                    accounting,
                    aoServers,
                    false
                );
            }

            // Remove the postgres_user
            conn.executeUpdate("delete from postgres_users where username=?", username);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_USERS,
                accounting,
                BusinessHandler.getServersForBusiness(conn, accounting),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setPostgresDatabaseBackupRetention(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        short days
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "setPostgresDatabaseBackupRetention(MasterDatabaseConnection,RequestSource,InvalidateList,int,short)", null);
        try {
            // Security checks
            checkAccessPostgresDatabase(conn, source, "setPostgresDatabaseBackupRetention", pkey);

            // Update the database
            conn.executeUpdate(
                "update postgres_databases set backup_retention=?::smallint where pkey=?",
                days,
                pkey
            );

            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_DATABASES,
                getBusinessForPostgresDatabase(conn, pkey),
                ServerHandler.getHostnameForServer(conn, getAOServerForPostgresDatabase(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets a PostgreSQL password.
     */
    public static void setPostgresServerUserPassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        int postgres_server_user,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "setPostgresServerUserPassword(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            BusinessHandler.checkPermission(conn, source, "setPostgresServerUserPassword", AOServPermission.Permission.set_postgres_server_user_password);
            checkAccessPostgresServerUser(conn, source, "setPostgresServerUserPassword", postgres_server_user);
            if(isPostgresServerUserDisabled(conn, postgres_server_user)) throw new SQLException("Unable to set PostgresServerUser password, account disabled: "+postgres_server_user);

            // Get the server for the user
            int aoServer=getAOServerForPostgresServerUser(conn, postgres_server_user);
            String username=getUsernameForPostgresServerUser(conn, postgres_server_user);

            // No setting the super user password
            if(username.equals(PostgresUser.POSTGRES)) throw new SQLException("The PostgreSQL "+PostgresUser.POSTGRES+" password may not be set.");

            // Perform the password check here, too.
            if(password!=PostgresUser.NO_PASSWORD) {
                PasswordChecker.Result[] results = PostgresUser.checkPassword(Locale.getDefault(), username, password);
                if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
            }

            // Contact the daemon for the update
            DaemonHandler.getDaemonConnector(conn, aoServer).setPostgresUserPassword(postgres_server_user, password);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setPostgresServerUserPredisablePassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int psu,
        String password
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "setPostgresServerUserPredisablePassword(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessPostgresServerUser(conn, source, "setPostgresServerUserPredisablePassword", psu);
            if(password==null) {
                if(isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to clear PostgresServerUser predisable password, account disabled: "+psu);
            } else {
                if(!isPostgresServerUserDisabled(conn, psu)) throw new SQLException("Unable to set PostgresServerUser predisable password, account not disabled: "+psu);
            }

            // Update the database
            conn.executeUpdate(
                "update postgres_server_users set predisable_password=? where pkey=?",
                password,
                psu
            );
            
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.POSTGRES_SERVER_USERS,
                getBusinessForPostgresServerUser(conn, psu),
                ServerHandler.getHostnameForServer(conn, getAOServerForPostgresServerUser(conn, psu)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void waitForPostgresDatabaseRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "waitForPostgresDatabaseRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForPostgresDatabaseRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresDatabaseRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void waitForPostgresServerRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "waitForPostgresServerRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForPostgresServerRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresServerRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void waitForPostgresUserRebuild(
        MasterDatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "waitForPostgresUserRebuild(MasterDatabaseConnection,RequestSource,int)", Integer.valueOf(aoServer));
        try {
            ServerHandler.checkAccessServer(conn, source, "waitForPostgresUserRebuild", aoServer);
            ServerHandler.waitForInvalidates(aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).waitForPostgresUserRebuild();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForPostgresDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getBusinessForPostgresDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  postgres_databases pd,\n"
                + "  postgres_server_users psu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  pd.pkey=?\n"
                + "  and pd.datdba=psu.pkey\n"
                + "  and psu.username=un.username\n"
                + "  and un.package=pk.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPackageForPostgresBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPackageForPostgresBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeIntQuery(
                "select package from postgres_backups where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPostgresServerForPostgresBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPostgresServerForPostgresBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeIntQuery(
                "select postgres_server from postgres_backups where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPackageForPostgresDatabase(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPackageForPostgresDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  pk.pkey\n"
                + "from\n"
                + "  postgres_databases pd,\n"
                + "  postgres_server_users psu,\n"
                + "  usernames un,\n"
                + "  packages pk\n"
                + "where\n"
                + "  pd.pkey=?\n"
                + "  and pd.datdba=psu.pkey\n"
                + "  and psu.username=un.username\n"
                + "  and un.package=pk.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForPostgresServerUser(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getBusinessForPostgresServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from postgres_server_users psu, usernames un, packages pk where psu.username=un.username and un.package=pk.name and psu.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForPostgresServer(MasterDatabaseConnection conn, int postgresServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getAOServerForPostgresServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from postgres_servers where pkey=?", postgresServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPortForPostgresServer(MasterDatabaseConnection conn, int postgresServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPortForPostgresServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select nb.port from postgres_servers ps, net_binds nb where ps.pkey=? and ps.net_bind=nb.pkey", postgresServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getMinorVersionForPostgresServer(MasterDatabaseConnection conn, int postgresServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getMinorVersionForPostgresServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  pv.minor_version\n"
                + "from\n"
                + "  postgres_servers ps,\n"
                + "  postgres_versions pv\n"
                + "where\n"
                + "  ps.pkey=?\n"
                + "  and ps.version=pv.version",
                postgresServer
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPostgresServerForPostgresDatabase(MasterDatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPostgresServerForPostgresDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select postgres_server from postgres_databases where pkey=?",
                postgresDatabase
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getPostgresServerForPostgresServerUser(MasterDatabaseConnection conn, int postgres_server_user) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getPostgresServerForPostgresServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select postgres_server from postgres_server_users where pkey=?", postgres_server_user);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForPostgresDatabase(MasterDatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getAOServerForPostgresDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  ps.ao_server\n"
                + "from\n"
                + "  postgres_databases pd,\n"
                + "  postgres_servers ps\n"
                + "where\n"
                + "  pd.pkey=?\n"
                + "  and pd.postgres_server=ps.pkey",
                postgresDatabase
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDatDbaForPostgresDatabase(MasterDatabaseConnection conn, int postgresDatabase) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getDatDbaForPostgresDatabase(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  datdba\n"
                + "from\n"
                + "  postgres_databases\n"
                + "where\n"
                + "  pkey=?",
                postgresDatabase
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForPostgresServerUser(MasterDatabaseConnection conn, int postgres_server_user) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "getAOServerForPostgresServerUser(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  ps.ao_server\n"
                + "from\n"
                + "  postgres_server_users psu,\n"
                + "  postgres_servers ps\n"
                + "where\n"
                + "  psu.pkey=?\n"
                + "  and psu.postgres_server=ps.pkey",
                postgres_server_user
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeExpiredPostgresBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "removeExpiredPostgresBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("non-master user "+username+" not allowed to removeExpiredPostgresBackups");
            ServerHandler.checkAccessServer(conn, source, "removeExpiredPostgresBackups", aoServer);

            // Get the list of pkeys that should be removed
            IntList pkeys=backupConn.executeIntListQuery(
                "select\n"
                + "  pb.pkey\n"
                + "from\n"
                + "  postgres_servers ps,\n"
                + "  postgres_backups pb\n"
                + "where\n"
                + "  ps.ao_server=?\n"
                + "  and ps.pkey=pb.postgres_server\n"
                + "  and now()>=(pb.end_time+(pb.backup_retention || ' days')::interval)",
                aoServer
            );
            
            // Remove each file
            int size=pkeys.size();
            for(int c=0;c<size;c++) {
                removePostgresBackup(
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

    public static void restartPostgreSQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int postgresServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "restartPostgreSQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "postgresql");
            if(!canControl) throw new SQLException("Not allowed to restart PostgreSQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).restartPostgres(postgresServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void startPostgreSQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int postgresServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "startPostgreSQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "postgresql");
            if(!canControl) throw new SQLException("Not allowed to start PostgreSQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).startPostgreSQL(postgresServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void stopPostgreSQL(
        MasterDatabaseConnection conn,
        RequestSource source,
        int postgresServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PostgresHandler.class, "stopPostgreSQL(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForPostgresServer(conn, postgresServer);
            boolean canControl=BusinessHandler.canControl(conn, source, aoServer, "postgresql");
            if(!canControl) throw new SQLException("Not allowed to stop PostgreSQL on "+aoServer);
            DaemonHandler.getDaemonConnector(conn, aoServer).stopPostgreSQL(postgresServer);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
