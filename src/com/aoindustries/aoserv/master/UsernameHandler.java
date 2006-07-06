package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>UsernameHandler</code> handles all the accesses to the <code>usernames</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class UsernameHandler {

    private final static Map<String,Boolean> disabledUsernames=new HashMap<String,Boolean>();
    private final static Map<String,String> usernameBusinesses=new HashMap<String,String>();

    public static boolean canAccessUsername(MasterDatabaseConnection conn, RequestSource source, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "canAccessUsername(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            return PackageHandler.canAccessPackage(conn, source, getPackageForUsername(conn, username));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessUsername(MasterDatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UsernameHandler.class, "checkAccessUsername(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            if(!canAccessUsername(conn, source, username)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access username: action='"
                    +action
                    +"', username="
                    +username
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void addUsername(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String packageName, 
        String username,
        boolean avoidSecurityChecks
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "addUsername(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,boolean)", null);
        try {
            if(!Username.isValidUsername(username)) throw new SQLException("Invalid username: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add Username for user '"+LinuxAccount.MAIL+'\'');

            if(!avoidSecurityChecks) {
                PackageHandler.checkAccessPackage(conn, source, "addUsername", packageName);
                if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Username '"+username+"', Package disabled: "+packageName);

                // Make sure people don't create @hostname.com usernames for domains they cannot control
                int atPos=username.lastIndexOf('@');
                if(atPos!=-1) {
                    String hostname=username.substring(atPos+1);
                    if(hostname.length()>0) MasterServer.checkAccessHostname(conn, source, "addUsername", hostname);
                }
            }

            conn.executeUpdate(
                "insert into usernames values(?,?,null)",
                username,
                packageName
            );

            // Notify all clients of the update
            String accounting=PackageHandler.getBusinessForPackage(conn, packageName);
            invalidateList.addTable(conn, SchemaTable.USERNAMES, accounting, InvalidateList.allServers, false);
            //invalidateList.addTable(conn, SchemaTable.PACKAGES, accounting, null);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableUsername(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "disableUsername(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isUsernameDisabled(conn, username)) throw new SQLException("Username is already disabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableUsername", disableLog, false);
            checkAccessUsername(conn, source, "disableUsername", username);
            if(
                InterBaseHandler.isInterBaseUser(conn, username)
                && !InterBaseHandler.isInterBaseUserDisabled(conn, username)
            ) throw new SQLException("Cannot disable Username '"+username+"': InterBaseUser not disabled: "+username);
            if(
                LinuxAccountHandler.isLinuxAccount(conn, username)
                && !LinuxAccountHandler.isLinuxAccountDisabled(conn, username)
            ) throw new SQLException("Cannot disable Username '"+username+"': LinuxAccount not disabled: "+username);
            if(
                MySQLHandler.isMySQLUser(conn, username)
                && !MySQLHandler.isMySQLUserDisabled(conn, username)
            ) throw new SQLException("Cannot disable Username '"+username+"': MySQLUser not disabled: "+username);
            if(
                PostgresHandler.isPostgresUser(conn, username)
                && !PostgresHandler.isPostgresUserDisabled(conn, username)
            ) throw new SQLException("Cannot disable Username '"+username+"': PostgresUser not disabled: "+username);

            conn.executeUpdate(
                "update usernames set disable_log=? where username=?",
                disableLog,
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.USERNAMES,
                getBusinessForUsername(conn, username),
                getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableUsername(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "enableUsername(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForUsername(conn, username);
            if(disableLog==-1) throw new SQLException("Username is already enabled: "+username);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableUsername", disableLog, true);
            checkAccessUsername(conn, source, "enableUsername", username);
            String pk=getPackageForUsername(conn, username);
            if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable Username '"+username+"', Package not enabled: "+pk);

            conn.executeUpdate(
                "update usernames set disable_log=null where username=?",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.USERNAMES,
                getBusinessForUsername(conn, username),
                getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForUsername(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "getDisableLogForUsername(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from usernames where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, UsernameHandler.class, "invalidateTable(int)", null);
        try {
            if(tableID==SchemaTable.USERNAMES) {
                synchronized(disabledUsernames) {
                    disabledUsernames.clear();
                }
                synchronized(usernameBusinesses) {
                    usernameBusinesses.clear();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isUsernameAvailable(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "isUsernameAvailable(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery("select (select username from usernames where username=?) is null", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isUsernameDisabled(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "isUsernameDisabled(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(disabledUsernames) {
		Boolean O=disabledUsernames.get(username);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForUsername(conn, username)!=-1;
		disabledUsernames.put(username, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeUsername(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UsernameHandler.class, "removeUsername(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            if(username.equals(source.getUsername())) throw new SQLException("Not allowed to remove self: "+username);
            checkAccessUsername(conn, source, "removeUsername", username);

            removeUsername(conn, invalidateList, username);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void removeUsername(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "removeUsername(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove Username named '"+LinuxAccount.MAIL+'\'');

            String accounting=getBusinessForUsername(conn, username);

            conn.executeUpdate("delete from usernames where username=?", username);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.USERNAMES, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForUsername(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, UsernameHandler.class, "getBusinessForUsername(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(usernameBusinesses) {
		String O=usernameBusinesses.get(username);
		if(O!=null) return O;
		String accounting=conn.executeStringQuery("select pk.accounting from usernames un, packages pk where un.username=? and un.package=pk.name", username);
		usernameBusinesses.put(username, accounting);
		return accounting;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getPackageForUsername(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "getPackageForUsername(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery("select package from usernames where username=?", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getServersForUsername(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "getServersForUsername(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                "select\n"
                + "  bs.server\n"
                + "from\n"
                + "  usernames un,\n"
                + "  packages pk,\n"
                + "  business_servers bs\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bs.accounting",
                username
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getUsernamesForPackage(MasterDatabaseConnection conn, String name) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "getUsernamesForPackage(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringListQuery("select username from usernames where package=?", name);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canUsernameAccessServer(MasterDatabaseConnection conn, String username, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "canUsernameAccessServer(MasterDatabaseConnection,String,int)", null);
        try {
            return conn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      un.username\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk,\n"
                + "      business_servers bs\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk.name\n"
                + "      and pk.accounting=bs.accounting\n"
                + "      and bs.server=?\n"
                + "    limit 1\n"
                + "  )\n"
                + "  is not null\n",
                username,
                server
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void checkUsernameAccessServer(MasterDatabaseConnection conn, RequestSource source, String action, String username, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, UsernameHandler.class, "checkUsernameAccessServer(MasterDatabaseConnection,RequestSource,String,String,int)", null);
        try {
            if(!canUsernameAccessServer(conn, username, server)) {
                String message=
                "username="
                +username
                +" is not allowed to access server.pkey="
                +server
                +": action='"
                +action
                +"'"
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}