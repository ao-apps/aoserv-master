package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServConnectorFactory;
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.security.AccountNotFoundException;
import com.aoindustries.security.BadPasswordException;
import com.aoindustries.security.IncompleteLoginException;
import com.aoindustries.security.LoginException;
import com.aoindustries.sql.Database;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.sql.ResultSetHandler;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of <code>AOServConnectorFactory</code> that operates directly on
 * the master database.
 *
 * @author  AO Industries, Inc.
 */
final public class DatabaseConnectorFactory implements AOServConnectorFactory<DatabaseConnector,DatabaseConnectorFactory> {

    private static final ObjectFactory<String> stringFactory = new ObjectFactory<String>() {
        public String createObject(ResultSet result) throws SQLException {
            return result.getString(1);
        }
    };

    final Database database;

    private final Object masterHostsLock = new Object();
    private Map<String,Set<String>> masterHosts;

    private final Object enabledMasterUsersLock = new Object();
    private Set<String> enabledMasterUsers;

    private final Object enabledDaemonUsersLock = new Object();
    private Set<String> enabledDaemonUsers;

    private final Object enabledBusinessAdministratorsLock = new Object();
    private Set<String> enabledBusinessAdministrators;

    public DatabaseConnectorFactory(Database database) {
        this.database = database;
    }

    /**
     * Determines if the provided user is a master user.  A master user has a row in
     * master_users table but no master_servers restrictions.  A master user has
     * no filters applied.
     */
    boolean isEnabledMasterUser(String username) throws IOException, SQLException {
        synchronized(enabledMasterUsersLock) {
            if(enabledMasterUsers==null) {
                enabledMasterUsers=database.executeObjectSetQuery(
                    stringFactory,
                    "select\n"
                    + "  mu.username\n"
                    + "from\n"
                    + "  master_users mu\n"
                    + "  inner join business_administrators ba on mu.username=ba.username\n"
                    + "where\n"
                    + "  mu.is_active\n"
                    + "  and ba.disable_log is null\n"
                    + "  and (select ms.pkey from master_servers ms where mu.username=ms.username limit 1) is null"
                );
            }
            return enabledMasterUsers.contains(username);
        }
    }

    /**
     * Determines if the provided user is a daemon user.  A daemon user has a row in
     * master_users table and at least one row in master_servers restrictions.
     * A daemon user has filters applied by server access.
     */
    boolean isEnabledDaemonUser(String username) throws IOException, SQLException {
        synchronized(enabledDaemonUsersLock) {
            if(enabledDaemonUsers==null) {
                enabledDaemonUsers=database.executeObjectSetQuery(
                    stringFactory,
                    "select\n"
                    + "  mu.username\n"
                    + "from\n"
                    + "  master_users mu\n"
                    + "  inner join business_administrators ba on mu.username=ba.username\n"
                    + "where\n"
                    + "  mu.is_active\n"
                    + "  and ba.disable_log is null\n"
                    + "  and (select ms.pkey from master_servers ms where mu.username=ms.username limit 1) is not null"
                );
            }
            return enabledDaemonUsers.contains(username);
        }
    }

    /**
     * Determines if the provided username is an enabled business administrator.
     * Master users and daemons are also considered administrators.
     */
    boolean isEnabledBusinessAdministrator(String username) throws IOException, SQLException {
        synchronized(enabledBusinessAdministratorsLock) {
            if(enabledBusinessAdministrators==null) {
                enabledBusinessAdministrators=database.executeObjectSetQuery(
                    stringFactory,
                    "select username from business_administrators where disable_log is null"
                );
            }
            return enabledBusinessAdministrators.contains(username);
        }
    }

    // TODO: Call from central invalidation system
    /*void invalidateTable(SchemaTableName tableName) {
        if(tableName==SchemaTableName.master_hosts) {
            synchronized(masterHostsLock) {
                masterHosts=null;
            }
        }
        if(tableName==SchemaTableName.business_administrators || tableName==SchemaTableName.master_users || tableName==SchemaTableName.master_servers) {
            synchronized(enabledMasterUsersLock) {
                enabledMasterUsers=null;
            }
            synchronized(enabledDaemonUsersLock) {
                enabledDaemonUsers=null;
            }
        }
        if(tableName==SchemaTableName.business_administrators) {
            synchronized(enabledBusinessAdministratorsLock) {
                enabledBusinessAdministrators=null;
            }
        }
    }*/

    boolean canSwitchUser(String authenticatedAs, String connectAs) throws IOException, SQLException {
        return database.executeBooleanQuery(
            "select\n"
            // Must have can_switch_users enabled
            + "  (select can_switch_users from business_administrators where username=?)\n"
            // Cannot switch within same business
            + "  and (select accounting from usernames where username=?)!=(select accounting from usernames where username=?)\n"
            // Must be switching to a subaccount
            + "  and is_business_or_parent(\n"
            + "    (select accounting from usernames where username=?),\n"
            + "    (select accounting from usernames where username=?)\n"
            + "  )",
            authenticatedAs,
            authenticatedAs,
            connectAs,
            authenticatedAs,
            connectAs
        );
    }

    /**
     * Gets the hosts that are allowed for the provided username.
     */
    boolean isHostAllowed(String username, String host) throws IOException, SQLException {
        Set<String> hosts;
        synchronized(masterHostsLock) {
            if(masterHosts==null) {
                final Map<String,Set<String>> table=new HashMap<String,Set<String>>();
                database.executeQuery(
                    new ResultSetHandler() {
                        public void handleResultSet(ResultSet result) throws SQLException {
                            String un=result.getString(1);
                            String ho=result.getString(2);
                            Set<String> sv=table.get(un);
                            if(sv==null) table.put(un, sv=new HashSet<String>());
                            sv.add(ho);
                        }
                    },
                    "select mh.username, mh.host from master_hosts mh, master_users mu where mh.username=mu.username and mu.is_active"
                );
                masterHosts = table;
            }
            hosts=masterHosts.get(username);
        }
        return
            hosts==null // Allow from anywhere if no hosts are provided
            || hosts.contains(host)
        ;
    }

    public DatabaseConnector newConnector(Locale locale, String connectAs, String authenticateAs, String password, String daemonServer) throws LoginException, RemoteException {
        try {
            // Handle the authentication
            if(connectAs.length()==0)      throw new IncompleteLoginException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.connectAs.empty"));
            if(authenticateAs.length()==0) throw new IncompleteLoginException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.authenticateAs.empty"));
            if(password.length()==0)       throw new IncompleteLoginException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.password.empty"));

            String correctCrypted = database.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                false,
                "select password from business_administrators where username=?",
                authenticateAs
            );
            if(correctCrypted==null) throw new AccountNotFoundException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.accountNotFound"));
            if(!BusinessAdministrator.passwordMatches(password, correctCrypted)) throw new BadPasswordException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.badPassword"));

            if(!isEnabledBusinessAdministrator(authenticateAs)) throw new AccountDisabledException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.accountDisabled"));

            String remoteHost = RemoteServer.getClientHost();
            if(!isHostAllowed(authenticateAs, remoteHost)) throw new LoginException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.hostNotAllowed", remoteHost, authenticateAs));

            // If connectAs is not authenticateAs, must be authenticated with switch user permissions
            if(
                !connectAs.equals(authenticateAs)
                && !canSwitchUser(authenticateAs, connectAs)
            ) {
                throw new LoginException(ApplicationResources.accessor.getMessage(locale, "DatabaseConnectorFactory.createConnector.switchUserNotAllowed", authenticateAs, connectAs));
            }

            // Let them in
            return new DatabaseConnector(this, locale, connectAs);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(ServerNotActiveException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
}
