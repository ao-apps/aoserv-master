package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLDatabaseTable;
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.MySQLServerUser;
import com.aoindustries.aoserv.client.MySQLUser;
import com.aoindustries.aoserv.client.PasswordChecker;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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

    private MySQLHandler() {
    }

    private final static Map<Integer,Boolean> disabledMySQLServerUsers=new HashMap<Integer,Boolean>();
    private final static Map<String,Boolean> disabledMySQLUsers=new HashMap<String,Boolean>();

    public static void checkAccessMySQLDatabase(DatabaseConnection conn, RequestSource source, String action, int mysql_database) throws IOException, SQLException {
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
    }

    public static void checkAccessMySQLDBUser(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        checkAccessMySQLDatabase(conn, source, action, getMySQLDatabaseForMySQLDBUser(conn, pkey));
        checkAccessMySQLServerUser(conn, source, action, getMySQLServerUserForMySQLDBUser(conn, pkey));
    }

    public static void checkAccessMySQLServerUser(DatabaseConnection conn, RequestSource source, String action, int mysql_server_user) throws IOException, SQLException {
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
    }

    public static void checkAccessMySQLServer(DatabaseConnection conn, RequestSource source, String action, int mysql_server) throws IOException, SQLException {
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
    }

    public static void checkAccessMySQLUser(DatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
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
                    throw new SQLException(message);
                }
            }
        } else {
            UsernameHandler.checkAccessUsername(conn, source, action, username);
        }
    }

    /**
     * Adds a MySQL database to the system.
     */
    public static int addMySQLDatabase(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name,
        int mysqlServer,
        String packageName
    ) throws IOException, SQLException {
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
            + "  ?\n"
            + ")",
            pkey,
            name,
            mysqlServer,
            packageName
        );

        // Notify all clients of the update, the server will detect this change and automatically add the database
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_DATABASES,
            accounting,
            aoServer,
            false
        );
        return pkey;
    }

    /**
     * Grants a MySQLServerUser access to a MySQLMasterDatabase.getDatabase().
     */
    public static int addMySQLDBUser(
        DatabaseConnection conn,
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
        boolean canExecute,
        boolean canEvent,
        boolean canTrigger
    ) throws IOException, SQLException {
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
            "insert into mysql_db_users values(?,?,?,?,?,?,?,?,?,false,false,?,?,?,?,?,?,?,?,?,?,?)",
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
            canExecute,
            canEvent,
            canTrigger
        );

        // Notify all clients of the update, the server will detect this change and automatically update MySQL
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_DB_USERS,
            getBusinessForMySQLServerUser(conn, mysql_server_user),
            getAOServerForMySQLServer(conn, dbServer),
            false
        );
        return pkey;
    }

    /**
     * Adds a MySQL server user.
     */
    public static int addMySQLServerUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        int mysqlServer,
        String host
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_SERVER_USERS,
            accounting,
            aoServer,
            true
        );
        return pkey;
    }

    /**
     * Adds a MySQL user.
     */
    public static void addMySQLUser(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_USERS,
            UsernameHandler.getBusinessForUsername(conn, username),
            InvalidateList.allServers,
            true
        );
    }

    public static void disableMySQLServerUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_SERVER_USERS,
            getBusinessForMySQLServerUser(conn, pkey),
            getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, pkey)),
            false
        );
    }

    public static void disableMySQLUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_USERS,
            UsernameHandler.getBusinessForUsername(conn, username),
            UsernameHandler.getServersForUsername(conn, username),
            false
        );
    }

    /**
     * Dumps a MySQL database
     */
    public static void dumpMySQLDatabase(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int dbPKey
    ) throws IOException, SQLException {
        checkAccessMySQLDatabase(conn, source, "dumpMySQLDatabase", dbPKey);

        int mysqlServer=getMySQLServerForMySQLDatabase(conn, dbPKey);
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).dumpMySQLDatabase(dbPKey, out);
    }

    public static void enableMySQLServerUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_SERVER_USERS,
            UsernameHandler.getBusinessForUsername(conn, mu),
            getAOServerForMySQLServer(conn, getMySQLServerForMySQLServerUser(conn, pkey)),
            false
        );
    }

    public static void enableMySQLUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_USERS,
            UsernameHandler.getBusinessForUsername(conn, username),
            UsernameHandler.getServersForUsername(conn, username),
            false
        );
    }

    /**
     * Generates a unique MySQL database name.
     */
    public static String generateMySQLDatabaseName(
        DatabaseConnection conn,
        String template_base,
        String template_added
    ) throws IOException, SQLException {
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
    }

    private static final Object reservedWordLock=new Object();
    private static List<String> reservedWordCache;
    public static List<String> getReservedWords(DatabaseConnection conn) throws IOException, SQLException {
        synchronized(reservedWordLock) {
            if(reservedWordCache==null) {
                // Load the list of reserved words
                reservedWordCache=conn.executeStringListQuery("select word from mysql_reserved_words");
            }
            return reservedWordCache;
        }
    }

    public static int getDisableLogForMySQLServerUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql_server_users where pkey=?", pkey);
    }

    public static int getDisableLogForMySQLUser(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeIntQuery("select coalesce(disable_log, -1) from mysql_users where username=?", username);
    }

    public static String getUsernameForMySQLServerUser(DatabaseConnection conn, int msu) throws IOException, SQLException {
        return conn.executeStringQuery("select username from mysql_server_users where pkey=?", msu);
    }

    public static String getMySQLDatabaseName(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select name from mysql_databases where pkey=?", pkey);
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        switch(tableID) {
            case MYSQL_RESERVED_WORDS :
                synchronized(reservedWordLock) {
                    reservedWordCache=null;
                }
                break;
            case MYSQL_SERVER_USERS :
                synchronized(MySQLHandler.class) {
                    disabledMySQLServerUsers.clear();
                }
                break;
            case MYSQL_USERS :
                synchronized(MySQLHandler.class) {
                    disabledMySQLUsers.clear();
                }
                break;
        }
    }

    public static boolean isMySQLServerUserDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
	    synchronized(MySQLHandler.class) {
            Integer I=Integer.valueOf(pkey);
            Boolean O=disabledMySQLServerUsers.get(I);
            if(O!=null) return O.booleanValue();
            boolean isDisabled=getDisableLogForMySQLServerUser(conn, pkey)!=-1;
            disabledMySQLServerUsers.put(I, isDisabled?Boolean.TRUE:Boolean.FALSE);
            return isDisabled;
	    }
    }

    public static boolean isMySQLUser(DatabaseConnection conn, String username) throws IOException, SQLException {
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
    }

    public static boolean isMySQLUserDisabled(DatabaseConnection conn, String username) throws IOException, SQLException {
	    synchronized(MySQLHandler.class) {
            Boolean O=disabledMySQLUsers.get(username);
            if(O!=null) return O.booleanValue();
            boolean isDisabled=getDisableLogForMySQLUser(conn, username)!=-1;
            disabledMySQLUsers.put(username, isDisabled);
            return isDisabled;
	    }
    }

    /**
     * Determines if a MySQL database name is available.
     */
    public static boolean isMySQLDatabaseNameAvailable(
        DatabaseConnection conn,
        RequestSource source,
        String name,
        int mysqlServer
    ) throws IOException, SQLException {
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        ServerHandler.checkAccessServer(conn, source, "isMySQLDatabaseNameAvailable", aoServer);
        return conn.executeBooleanQuery("select (select pkey from mysql_databases where name=? and mysql_server=?) is null", name, mysqlServer);
    }

    public static boolean isMySQLServerUserPasswordSet(
        DatabaseConnection conn,
        RequestSource source,
        int msu
    ) throws IOException, SQLException {
        checkAccessMySQLServerUser(conn, source, "isMySQLServerUserPasswordSet", msu);
        if(isMySQLServerUserDisabled(conn, msu)) throw new SQLException("Unable to determine if the MySQLServerUser password is set, account disabled: "+msu);
        String username=getUsernameForMySQLServerUser(conn, msu);
        int mysqlServer=getMySQLServerForMySQLServerUser(conn, msu);
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        String password=DaemonHandler.getDaemonConnector(conn, aoServer).getEncryptedMySQLUserPassword(mysqlServer, username);
        return !MySQLUser.NO_PASSWORD_DB_VALUE.equals(password);
    }

    /**
     * Removes a MySQLDatabase from the system.
     */
    public static void removeMySQLDatabase(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        checkAccessMySQLDatabase(conn, source, "removeMySQLDatabase", pkey);

        removeMySQLDatabase(conn, invalidateList, pkey);
    }

    /**
     * Removes a MySQLDatabase from the system.
     */
    public static void removeMySQLDatabase(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
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
            + "  and mdu.mysql_server_user=msu.pkey\n"
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
            SchemaTable.TableID.MYSQL_DATABASES,
            accounting,
            aoServer,
            false
        );
        if(dbUserAccounts.size()>0) invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_DB_USERS,
            dbUserAccounts,
            aoServer,
            false
        );
    }

    /**
     * Removes a MySQLDBUser from the system.
     */
    public static void removeMySQLDBUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        checkAccessMySQLDBUser(conn, source, "removeMySQLDBUser", pkey);

        // Remove the mysql_db_user
        String accounting=getBusinessForMySQLDBUser(conn, pkey);
        int mysqlServer=getMySQLServerForMySQLDBUser(conn, pkey);
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        conn.executeUpdate("delete from mysql_db_users where pkey=?", pkey);

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_DB_USERS,
            accounting,
            aoServer,
            false
        );
    }

    /**
     * Removes a MySQLServerUser from the system.
     */
    public static void removeMySQLServerUser(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        checkAccessMySQLServerUser(conn, source, "removeMySQLServerUser", pkey);

        String username=getUsernameForMySQLServerUser(conn, pkey);
        if(username.equals(MySQLUser.ROOT)) throw new SQLException("Not allowed to remove MySQLServerUser for user '"+MySQLUser.ROOT+'\'');

        // Remove the mysql_db_user
        boolean dbUsersExist=conn.executeBooleanQuery("select (select pkey from mysql_db_users where mysql_server_user=? limit 1) is not null", pkey);
        if(dbUsersExist) conn.executeUpdate("delete from mysql_db_users where mysql_server_user=?", pkey);

        // Remove the mysql_server_user
        String accounting=getBusinessForMySQLServerUser(conn, pkey);
        int mysqlServer=getMySQLServerForMySQLServerUser(conn, pkey);
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        conn.executeUpdate("delete from mysql_server_users where pkey=?", pkey);

        // Notify all clients of the updates
        if(dbUsersExist) invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_DB_USERS,
            accounting,
            aoServer,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.MYSQL_SERVER_USERS,
            accounting,
            aoServer,
            true
        );
    }

    /**
     * Removes a MySQLUser from the system.
     */
    public static void removeMySQLUser(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        checkAccessMySQLUser(conn, source, "removeMySQLUser", username);

        removeMySQLUser(conn, invalidateList, username);
    }

    /**
     * Removes a MySQLUser from the system.
     */
    public static void removeMySQLUser(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
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
            + "  and msu.pkey=mdu.mysql_server_user\n"
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
                + "      and msu.pkey=mdu.mysql_server_user"
                + "  )",
                username
            );
            for(int mysqlServer : dbUserServers) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.MYSQL_DB_USERS,
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
                    SchemaTable.TableID.MYSQL_SERVER_USERS,
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
            SchemaTable.TableID.MYSQL_USERS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    /**
     * Sets a MySQL password.
     */
    public static void setMySQLServerUserPassword(
        DatabaseConnection conn,
        RequestSource source,
        int mysql_server_user,
        String password
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setMySQLServerUserPassword", AOServPermission.Permission.set_mysql_server_user_password);
        checkAccessMySQLServerUser(conn, source, "setMySQLServerUserPassword", mysql_server_user);
        if(isMySQLServerUserDisabled(conn, mysql_server_user)) throw new SQLException("Unable to set MySQLServerUser password, account disabled: "+mysql_server_user);

        // Get the server, username for the user
        String username=getUsernameForMySQLServerUser(conn, mysql_server_user);

        // No setting the super user password
        if(username.equals(MySQLUser.ROOT)) throw new SQLException("The MySQL "+MySQLUser.ROOT+" password may not be set.");

        // Perform the password check here, too.
        if(password!=null && password.length()==0) password=MySQLUser.NO_PASSWORD;
        if(password!=MySQLUser.NO_PASSWORD) {
            PasswordChecker.Result[] results = MySQLUser.checkPassword(Locale.getDefault(), username, password);
            if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
        }

        // Contact the daemon for the update
        int mysqlServer=getMySQLServerForMySQLServerUser(conn, mysql_server_user);
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        DaemonHandler.getDaemonConnector(
            conn,
            aoServer
        ).setMySQLUserPassword(mysqlServer, username, password);
    }

    public static void setMySQLServerUserPredisablePassword(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int msu,
        String password
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.MYSQL_SERVER_USERS,
            getBusinessForMySQLServerUser(conn, msu),
            aoServer,
            false
        );
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLDatabaseRebuild(
        DatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "waitForMySQLDatabaseRebuild", aoServer);
        ServerHandler.waitForInvalidates(aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDatabaseRebuild();
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLDBUserRebuild(
        DatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "waitForMySQLDBUserRebuild", aoServer);
        ServerHandler.waitForInvalidates(aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLDBUserRebuild();
    }

    /**
     * Waits for any pending or processing MySQL database config rebuild to complete.
     */
    public static void waitForMySQLUserRebuild(
        DatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        ServerHandler.checkAccessServer(conn, source, "waitForMySQLUserRebuild", aoServer);
        ServerHandler.waitForInvalidates(aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).waitForMySQLUserRebuild();
    }

    public static String getBusinessForMySQLDatabase(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select pk.accounting from mysql_databases md, packages pk where md.package=pk.name and md.pkey=?", pkey);
    }

    public static String getBusinessForMySQLDBUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
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
            + "  and mdu.mysql_server_user=msu.pkey\n"
            + "  and msu.username=un.username\n"
            + "  and un.package=pk.name",
            pkey
        );
    }

    public static String getBusinessForMySQLServerUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
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
    }

    public static int getPackageForMySQLDatabase(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select pk.pkey from mysql_databases md, packages pk where md.pkey=? and md.package=pk.name", pkey);
    }

    public static IntList getMySQLServerUsersForMySQLUser(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeIntListQuery("select pkey from mysql_server_users where username=?", username);
    }

    public static int getMySQLServerForMySQLDatabase(DatabaseConnection conn, int mysql_database) throws IOException, SQLException {
        return conn.executeIntQuery("select mysql_server from mysql_databases where pkey=?", mysql_database);
    }

    public static int getAOServerForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        return conn.executeIntQuery("select ao_server from mysql_servers where pkey=?", mysqlServer);
    }

    public static String getPackageForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        return conn.executeStringQuery("select package from mysql_servers where pkey=?", mysqlServer);
    }

    public static int getPortForMySQLServer(DatabaseConnection conn, int mysqlServer) throws IOException, SQLException {
        return conn.executeIntQuery("select nb.port from mysql_servers ms inner join net_binds nb on ms.net_bind=nb.pkey where ms.pkey=?", mysqlServer);
    }

    public static int getMySQLServerForFailoverMySQLReplication(DatabaseConnection conn, int failoverMySQLReplication) throws IOException, SQLException {
        return conn.executeIntQuery("select mysql_server from failover_mysql_replications where pkey=?", failoverMySQLReplication);
    }

    public static int getMySQLServerForMySQLDBUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select msu.mysql_server from mysql_db_users mdu, mysql_server_users msu where mdu.pkey=? and mdu.mysql_server_user=msu.pkey", pkey);
    }

    public static int getMySQLDatabaseForMySQLDBUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select mysql_database from mysql_db_users where pkey=?", pkey);
    }

    public static int getMySQLServerUserForMySQLDBUser(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select mysql_user from mysql_db_users where pkey=?", pkey);
    }

    public static int getMySQLServerForMySQLServerUser(DatabaseConnection conn, int mysql_server_user) throws IOException, SQLException {
        return conn.executeIntQuery("select mysql_server from mysql_server_users where pkey=?", mysql_server_user);
    }

    public static void restartMySQL(
        DatabaseConnection conn,
        RequestSource source,
        int mysqlServer
    ) throws IOException, SQLException {
        int aoServer=getAOServerForMySQLServer(conn, mysqlServer);
        boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
        if(!canControl) throw new SQLException("Not allowed to restart MySQL on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).restartMySQL(mysqlServer);
    }

    public static void startMySQL(
        DatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
        if(!canControl) throw new SQLException("Not allowed to start MySQL on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).startMySQL();
    }

    public static void stopMySQL(
        DatabaseConnection conn,
        RequestSource source,
        int aoServer
    ) throws IOException, SQLException {
        boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_mysql");
        if(!canControl) throw new SQLException("Not allowed to stop MySQL on "+aoServer);
        DaemonHandler.getDaemonConnector(conn, aoServer).stopMySQL();
    }
    
    public static void getMasterStatus(
        DatabaseConnection conn,
        RequestSource source,
        int mysqlServer,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "getMasterStatus", AOServPermission.Permission.get_mysql_master_status);
        // Check access
        checkAccessMySQLServer(conn, source, "getMasterStatus", mysqlServer);
        int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
        if(DaemonHandler.isDaemonAvailable(aoServer)) {
            try {
                MySQLServer.MasterStatus masterStatus = DaemonHandler.getDaemonConnector(conn, aoServer).getMySQLMasterStatus(
                    mysqlServer
                );
                if(masterStatus==null) out.writeByte(AOServProtocol.DONE);
                else {
                    out.writeByte(AOServProtocol.NEXT);
                    out.writeNullUTF(masterStatus.getFile());
                    out.writeNullUTF(masterStatus.getPosition());
                }
            } catch(IOException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                throw err;
            }
        } else {
            throw new IOException("Server unavailable");
        }
    }

    public static void getSlaveStatus(
        DatabaseConnection conn,
        RequestSource source,
        int failoverMySQLReplication,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "getSlaveStatus", AOServPermission.Permission.get_mysql_slave_status);
        // Check access
        int mysqlServer = getMySQLServerForFailoverMySQLReplication(conn, failoverMySQLReplication);
        checkAccessMySQLServer(conn, source, "getSlaveStatus", mysqlServer);
        int failoverServer = conn.executeIntQuery("select bp.ao_server from failover_mysql_replications fmr inner join failover_file_replications ffr on fmr.replication=ffr.pkey inner join backup_partitions bp on ffr.backup_partition=bp.pkey where fmr.pkey=?", failoverMySQLReplication);
        if(DaemonHandler.isDaemonAvailable(failoverServer)) {
            try {
                String toPath = conn.executeStringQuery("select bp.path from failover_mysql_replications fmr inner join failover_file_replications ffr on fmr.replication=ffr.pkey inner join backup_partitions bp on ffr.backup_partition=bp.pkey where fmr.pkey=?", failoverMySQLReplication);
                int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
                int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
                if(osv==-1) throw new SQLException("Unknown operating_system_version for aoServer: "+aoServer);
                FailoverMySQLReplication.SlaveStatus slaveStatus = DaemonHandler.getDaemonConnector(conn, failoverServer).getMySQLSlaveStatus(
                    toPath+"/"+ServerHandler.getHostnameForAOServer(conn, aoServer),
                    osv,
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

    public static void getTableStatus(
        DatabaseConnection conn,
        RequestSource source,
        int mysqlDatabase,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "getTableStatus", AOServPermission.Permission.get_mysql_table_status);
        // Check access
        checkAccessMySQLDatabase(conn, source, "getTableStatus", mysqlDatabase);
        int mysqlServer = getMySQLServerForMySQLDatabase(conn, mysqlDatabase);
        int aoServer = getAOServerForMySQLServer(conn, mysqlServer);
        if(DaemonHandler.isDaemonAvailable(aoServer)) {
            List<MySQLDatabase.TableStatus> tableStatuses;
            try {
                tableStatuses = DaemonHandler.getDaemonConnector(conn, aoServer).getMySQLTableStatus(mysqlDatabase);
            } catch(IOException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                throw err;
            }
            out.writeByte(AOServProtocol.NEXT);
            int size = tableStatuses.size();
            out.writeCompressedInt(size);
            for(int c=0;c<size;c++) {
                MySQLDatabase.TableStatus tableStatus = tableStatuses.get(c);
                out.writeUTF(tableStatus.getName());
                out.writeNullEnum(tableStatus.getEngine());
                out.writeNullInteger(tableStatus.getVersion());
                out.writeNullEnum(tableStatus.getRowFormat());
                out.writeNullLong(tableStatus.getRows());
                out.writeNullLong(tableStatus.getAvgRowLength());
                out.writeNullLong(tableStatus.getDataLength());
                out.writeNullLong(tableStatus.getMaxDataLength());
                out.writeNullLong(tableStatus.getIndexLength());
                out.writeNullLong(tableStatus.getDataFree());
                out.writeNullLong(tableStatus.getAutoIncrement());
                out.writeNullUTF(tableStatus.getCreateTime());
                out.writeNullUTF(tableStatus.getUpdateTime());
                out.writeNullUTF(tableStatus.getCheckTime());
                out.writeNullEnum(tableStatus.getCollation());
                out.writeNullUTF(tableStatus.getChecksum());
                out.writeNullUTF(tableStatus.getCreateOptions());
                out.writeNullUTF(tableStatus.getComment());
            }
        } else {
            throw new IOException("Server unavailable");
        }
    }
}
