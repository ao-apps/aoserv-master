/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.Username;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>UsernameHandler</code> handles all the accesses to the <code>usernames</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class UsernameHandler {

    private UsernameHandler() {
    }

    private final static Map<String,Boolean> disabledUsernames=new HashMap<>();
    private final static Map<String,AccountingCode> usernameBusinesses=new HashMap<>();

    public static boolean canAccessUsername(DatabaseConnection conn, RequestSource source, String username) throws IOException, SQLException {
        return PackageHandler.canAccessPackage(conn, source, getPackageForUsername(conn, username));
    }

    public static void checkAccessUsername(DatabaseConnection conn, RequestSource source, String action, String username) throws IOException, SQLException {
        if(!canAccessUsername(conn, source, username)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access username: action='"
                +action
                +"', username="
                +username
            ;
            throw new SQLException(message);
        }
    }

    public static void addUsername(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String packageName, 
        String username,
        boolean avoidSecurityChecks
    ) throws IOException, SQLException {
        String check = Username.checkUsername(username);
        if(check!=null) throw new SQLException(check);
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
        AccountingCode accounting = PackageHandler.getBusinessForPackage(conn, packageName);
        invalidateList.addTable(conn, SchemaTable.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
        //invalidateList.addTable(conn, SchemaTable.TableID.PACKAGES, accounting, null);
    }

    public static void disableUsername(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        if(isUsernameDisabled(conn, username)) throw new SQLException("Username is already disabled: "+username);
        BusinessHandler.checkAccessDisableLog(conn, source, "disableUsername", disableLog, false);
        checkAccessUsername(conn, source, "disableUsername", username);
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
            SchemaTable.TableID.USERNAMES,
            getBusinessForUsername(conn, username),
            getServersForUsername(conn, username),
            false
        );
    }

    public static void enableUsername(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
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
            SchemaTable.TableID.USERNAMES,
            getBusinessForUsername(conn, username),
            getServersForUsername(conn, username),
            false
        );
    }

    public static int getDisableLogForUsername(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeIntQuery("select coalesce(disable_log, -1) from usernames where username=?", username);
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        if(tableID==SchemaTable.TableID.USERNAMES) {
            synchronized(disabledUsernames) {
                disabledUsernames.clear();
            }
            synchronized(usernameBusinesses) {
                usernameBusinesses.clear();
            }
        }
    }

    public static boolean isUsernameAvailable(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeBooleanQuery("select (select username from usernames where username=?) is null", username);
    }

    public static boolean isUsernameDisabled(DatabaseConnection conn, String username) throws IOException, SQLException {
	    synchronized(disabledUsernames) {
            Boolean O=disabledUsernames.get(username);
            if(O!=null) return O.booleanValue();
            boolean isDisabled=getDisableLogForUsername(conn, username)!=-1;
            disabledUsernames.put(username, isDisabled);
            return isDisabled;
	    }
    }

    public static void removeUsername(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        if(username.equals(source.getUsername())) throw new SQLException("Not allowed to remove self: "+username);
        checkAccessUsername(conn, source, "removeUsername", username);

        removeUsername(conn, invalidateList, username);
    }

    public static void removeUsername(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove Username named '"+LinuxAccount.MAIL+'\'');

        AccountingCode accounting = getBusinessForUsername(conn, username);

        conn.executeUpdate("delete from usernames where username=?", username);

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.USERNAMES, accounting, InvalidateList.allServers, false);
    }

    public static AccountingCode getBusinessForUsername(DatabaseConnection conn, String username) throws IOException, SQLException {
        synchronized(usernameBusinesses) {
            AccountingCode O=usernameBusinesses.get(username);
            if(O!=null) return O;
            AccountingCode accounting = conn.executeObjectQuery(
                ObjectFactories.accountingCodeFactory,
                "select pk.accounting from usernames un, packages pk where un.username=? and un.package=pk.name",
                username
            );
            usernameBusinesses.put(username, accounting);
            return accounting;
        }
    }

    public static String getPackageForUsername(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeStringQuery("select package from usernames where username=?", username);
    }

    public static IntList getServersForUsername(DatabaseConnection conn, String username) throws IOException, SQLException {
        return conn.executeIntListQuery(
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
    }

    public static List<String> getUsernamesForPackage(DatabaseConnection conn, String name) throws IOException, SQLException {
        return conn.executeStringListQuery("select username from usernames where package=?", name);
    }

    public static boolean canUsernameAccessServer(DatabaseConnection conn, String username, int server) throws IOException, SQLException {
        return conn.executeBooleanQuery(
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
    }
    
    public static void checkUsernameAccessServer(DatabaseConnection conn, RequestSource source, String action, String username, int server) throws IOException, SQLException {
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
            throw new SQLException(message);
        }
    }
}