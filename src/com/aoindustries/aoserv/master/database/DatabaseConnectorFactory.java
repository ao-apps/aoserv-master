package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.AOServConnectorFactory;
import com.aoindustries.aoserv.client.AOServConnectorFactoryCache;
import com.aoindustries.aoserv.client.cache.CachedConnectorFactory;
import com.aoindustries.aoserv.client.validator.DomainName;
import com.aoindustries.aoserv.client.validator.HashedPassword;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.security.AccountNotFoundException;
import com.aoindustries.security.BadPasswordException;
import com.aoindustries.security.IncompleteLoginException;
import com.aoindustries.security.LoginException;
import com.aoindustries.sql.Database;
import com.aoindustries.sql.DatabaseCallable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.sql.ResultSetHandler;
import com.aoindustries.util.WrappedException;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
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

    private static final ObjectFactory<UserId> userIdFactory = new ObjectFactory<UserId>() {
        public UserId createObject(ResultSet result) throws SQLException {
            try {
                return UserId.valueOf(result.getString(1)).intern();
            } catch(ValidationException ex) {
                SQLException sqlEx = new SQLException(ex.getMessage());
                sqlEx.initCause(ex);
                throw sqlEx;
            }
        }
    };

    final Database database;

    private final Object masterHostsLock = new Object();
    private Map<UserId,Set<InetAddress>> masterHosts;

    private final Object enabledMasterUsersLock = new Object();
    private Set<UserId> enabledMasterUsers;

    private final Object enabledDaemonUsersLock = new Object();
    private Set<UserId> enabledDaemonUsers;

    private final Object enabledBusinessAdministratorsLock = new Object();
    private Set<UserId> enabledBusinessAdministrators;

    /**
     * An unrestricted, shared, cached database connector is available for use by any of the
     * individual user connectors.  This should be used sparingly and carefully, and should only
     * be used to query information.  This connector is read-only.
     */
    final protected AOServConnector<?,?> rootConnector;

    public DatabaseConnectorFactory(Database database, UserId rootUserId, String rootPassword) throws LoginException, RemoteException {
        this.database = database;
        this.rootConnector = new CachedConnectorFactory(this).newConnector(Locale.getDefault(), rootUserId, rootUserId, rootPassword, null, true);
    }

    /**
     * Determines if the provided user is a master user.  A master user has a row in
     * master_users table but no master_servers restrictions.  A master user has
     * no filters applied.
     */
    boolean isEnabledMasterUser(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledMasterUsersLock) {
            if(enabledMasterUsers==null) {
                enabledMasterUsers=db.executeObjectSetQuery(
                    userIdFactory,
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
    boolean isEnabledDaemonUser(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledDaemonUsersLock) {
            if(enabledDaemonUsers==null) {
                enabledDaemonUsers=db.executeObjectSetQuery(
                    userIdFactory,
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
    boolean isEnabledBusinessAdministrator(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledBusinessAdministratorsLock) {
            if(enabledBusinessAdministrators==null) {
                enabledBusinessAdministrators=db.executeObjectSetQuery(
                    userIdFactory,
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

    boolean canSwitchUser(DatabaseConnection db, UserId authenticatedAs, UserId connectAs) throws SQLException {
        return db.executeBooleanQuery(
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
            authenticatedAs.toString(),
            authenticatedAs.toString(),
            connectAs.toString(),
            authenticatedAs.toString(),
            connectAs.toString()
        );
    }

    /**
     * Gets the hosts that are allowed for the provided username.
     */
    boolean isHostAllowed(DatabaseConnection db, UserId username, InetAddress host) throws SQLException {
        Set<InetAddress> hosts;
        synchronized(masterHostsLock) {
            if(masterHosts==null) {
                final Map<UserId,Set<InetAddress>> table=new HashMap<UserId,Set<InetAddress>>();
                db.executeQuery(
                    new ResultSetHandler() {
                        public void handleResultSet(ResultSet result) throws SQLException {
                            try {
                                UserId un=UserId.valueOf(result.getString(1)).intern();
                                InetAddress ho=InetAddress.valueOf(result.getString(2)).intern();
                                Set<InetAddress> sv=table.get(un);
                                if(sv==null) table.put(un, sv=Collections.singleton(ho));
                                else {
                                    if(sv.size()==1) {
                                        Set<InetAddress> newSV = new HashSet<InetAddress>();
                                        newSV.add(sv.iterator().next());
                                        table.put(un, sv = newSV);
                                    }
                                    sv.add(ho);
                                }
                            } catch(ValidationException ex) {
                                SQLException sqlEx = new SQLException(ex.getMessage());
                                sqlEx.initCause(ex);
                                throw sqlEx;
                            }
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

    private final AOServConnectorFactoryCache<DatabaseConnector,DatabaseConnectorFactory> connectors = new AOServConnectorFactoryCache<DatabaseConnector,DatabaseConnectorFactory>();

    @Override
    public DatabaseConnector getConnector(Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer, boolean readOnly) throws LoginException, RemoteException {
        synchronized(connectors) {
            DatabaseConnector connector = connectors.get(connectAs, authenticateAs, password, daemonServer, readOnly);
            if(connector!=null) {
                connector.setLocale(locale);
            } else {
                connector = newConnector(
                    locale,
                    connectAs,
                    authenticateAs,
                    password,
                    daemonServer,
                    readOnly
                );
            }
            return connector;
        }
    }

    @Override
    public DatabaseConnector newConnector(final Locale locale, final UserId connectAs, final UserId authenticateAs, final String password, final DomainName daemonServer, final boolean readOnly) throws LoginException, RemoteException {
        try {
            return database.executeTransaction(
                new DatabaseCallable<DatabaseConnector>() {
                    @Override
                    public DatabaseConnector call(DatabaseConnection db) throws SQLException {
                        try {
                            return newConnector(db, locale, connectAs, authenticateAs, password, daemonServer, readOnly);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        } catch(LoginException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable wrapped = err.getCause();
            if(wrapped instanceof RemoteException) throw (RemoteException)wrapped;
            if(wrapped instanceof LoginException) throw (LoginException)wrapped;
            if(wrapped instanceof RemoteException) throw (RemoteException)wrapped;
            throw err;
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    protected DatabaseConnector newConnector(DatabaseConnection db, Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer, boolean readOnly) throws RemoteException, LoginException, SQLException {
        try {
            // Handle the authentication
            if(connectAs==null)      throw new IncompleteLoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.connectAs.empty"));
            if(authenticateAs==null) throw new IncompleteLoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.authenticateAs.null"));
            if(password==null)       throw new IncompleteLoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.password.null"));
            if(password.length()==0) throw new IncompleteLoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.password.empty"));

            String correctCrypted = db.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                false,
                "select password from business_administrators where username=?",
                authenticateAs
            );
            if(correctCrypted==null) throw new AccountNotFoundException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountNotFound"));
            if(!HashedPassword.valueOf(correctCrypted).passwordMatches(password)) throw new BadPasswordException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.badPassword"));

            if(!isEnabledBusinessAdministrator(db, authenticateAs)) throw new AccountDisabledException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));

            InetAddress remoteHost;
            try {
                remoteHost = InetAddress.valueOf(RemoteServer.getClientHost());
            } catch(ServerNotActiveException err) {
                remoteHost = InetAddress.LOOPBACK;
            }
            if(!isHostAllowed(db, authenticateAs, remoteHost)) throw new LoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.hostNotAllowed", remoteHost, authenticateAs));

            // If connectAs is not authenticateAs, must be authenticated with switch user permissions
            if(
                !connectAs.equals(authenticateAs)
                && !canSwitchUser(db, authenticateAs, connectAs)
            ) {
                throw new LoginException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.switchUserNotAllowed", authenticateAs, connectAs));
            }

            // Let them in
            synchronized(connectors) {
                DatabaseConnector connector = new DatabaseConnector(this, locale, connectAs, authenticateAs, password, readOnly);
                connectors.put(
                    connectAs,
                    authenticateAs,
                    password,
                    daemonServer,
                    readOnly,
                    connector
                );
                return connector;
            }
        } catch(ValidationException err) {
            throw new RemoteException(err.getLocalizedMessage(), err);
        }
    }
}
