/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.cache.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.aoserv.master.*;
import com.aoindustries.security.*;
import com.aoindustries.sql.Database;
import com.aoindustries.sql.DatabaseCallable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.DatabaseRunnable;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.sql.ResultSetHandler;
import com.aoindustries.util.WrappedException;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of <code>AOServConnectorFactory</code> that operates directly on
 * the master database.
 *
 * @author  AO Industries, Inc.
 */
final public class DatabaseConnectorFactory implements AOServConnectorFactory {

    // <editor-fold defaultstate="collapsed" desc="Connector-less Data Access">
    private static final ObjectFactory<UserId> userIdFactory = new ObjectFactory<UserId>() {
        @Override
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
                        @Override
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

    private final Object enabledMasterUsersLock = new Object();
    private Set<UserId> enabledMasterUsers;
    /**
     * Determines if the provided user is a master user.  A master user has a row in
     * master_users table but no master_servers restrictions.  A master user has
     * no filters applied.
     */
    boolean isEnabledMasterUser(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledMasterUsersLock) {
            if(enabledMasterUsers==null) {
                enabledMasterUsers=db.executeObjectCollectionQuery(
                    new HashSet<UserId>(),
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

    private final Object enabledDaemonUsersLock = new Object();
    private Set<UserId> enabledDaemonUsers;
    /**
     * Determines if the provided user is a daemon user.  A daemon user has a row in
     * master_users table and at least one row in master_servers restrictions.
     * A daemon user has filters applied by server access.
     */
    boolean isEnabledDaemonUser(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledDaemonUsersLock) {
            if(enabledDaemonUsers==null) {
                enabledDaemonUsers=db.executeObjectCollectionQuery(
                    new HashSet<UserId>(),
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

    private final Object enabledBusinessAdministratorsLock = new Object();
    private Set<UserId> enabledBusinessAdministrators;
    /**
     * Determines if the provided username is an enabled business administrator.
     * Master users and daemons are also considered administrators.
     */
    boolean isEnabledBusinessAdministrator(DatabaseConnection db, UserId username) throws SQLException {
        synchronized(enabledBusinessAdministratorsLock) {
            if(enabledBusinessAdministrators==null) {
                enabledBusinessAdministrators=db.executeObjectCollectionQuery(
                    new HashSet<UserId>(),
                    userIdFactory,
                    "select username from business_administrators where disable_log is null"
                );
            }
            return enabledBusinessAdministrators.contains(username);
        }
    }

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
     * Invalidates local caches.
     */
    private void invalidateLocalCaches(EnumSet<ServiceName> services) {
        boolean master_hosts = services.contains(ServiceName.master_hosts);
        boolean master_users = services.contains(ServiceName.master_users);
        boolean business_administrators = services.contains(ServiceName.business_administrators);
        boolean master_servers = services.contains(ServiceName.master_servers);
        if(master_hosts || master_users) {
            synchronized(masterHostsLock) {
                masterHosts=null;
            }
        }
        if(business_administrators || master_users || master_servers) {
            synchronized(enabledMasterUsersLock) {
                enabledMasterUsers=null;
            }
        }
        if(business_administrators || master_users || master_servers) {
            synchronized(enabledDaemonUsersLock) {
                enabledDaemonUsers=null;
            }
        }
        if(business_administrators) {
            synchronized(enabledBusinessAdministratorsLock) {
                enabledBusinessAdministrators=null;
            }
        }
    }
    // </editor-fold>

    /**
     * An unrestricted, shared, cached database connector is available for use by any of the
     * individual user connectors.  This should be used sparingly and carefully, and should only
     * be used to query information.  This connector is read-only.  Also, it doesn't change the
     * ThreadLocale any so that messages are generated in the expected locale.
     */
    final protected AOServConnector rootConnector;

    public DatabaseConnectorFactory(Database database, UserId rootUserId, String rootPassword) throws LoginException, RemoteException {
        this.database = database;
        this.rootConnector = new CachedConnectorFactory(this).newConnector(Locale.getDefault(), rootUserId, rootUserId, rootPassword, null);
    }

    // <editor-fold defaultstate="collapsed" desc="Connector Creation">
    private final AOServConnectorFactoryCache<DatabaseConnector> connectors = new AOServConnectorFactoryCache<DatabaseConnector>();

    @Override
    public DatabaseConnector getConnector(Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer) throws LoginException, RemoteException {
        synchronized(connectors) {
            DatabaseConnector connector = connectors.get(connectAs, authenticateAs, password, daemonServer);
            if(connector!=null) {
                connector.setLocale(locale);
            } else {
                connector = newConnector(
                    locale,
                    connectAs,
                    authenticateAs,
                    password,
                    daemonServer
                );
            }
            return connector;
        }
    }

    @Override
    public DatabaseConnector newConnector(final Locale locale, final UserId connectAs, final UserId authenticateAs, final String password, final DomainName daemonServer) throws LoginException, RemoteException {
        try {
            return database.executeTransaction(
                new DatabaseCallable<DatabaseConnector>() {
                    @Override
                    public DatabaseConnector call(DatabaseConnection db) throws SQLException {
                        try {
                            return newConnector(db, locale, connectAs, authenticateAs, password, daemonServer);
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
            throw err;
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    protected DatabaseConnector newConnector(DatabaseConnection db, Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer) throws RemoteException, LoginException, SQLException {
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
                DatabaseConnector connector = new DatabaseConnector(this, locale, connectAs, authenticateAs, password);
                connectors.put(
                    connectAs,
                    authenticateAs,
                    password,
                    daemonServer,
                    connector
                );
                return connector;
            }
        } catch(ValidationException err) {
            throw new RemoteException(err.getLocalizedMessage(), err);
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Invalidate Set Management">

    /**
     * Invalidates local caches.
     * Adds the invalidations from the provided connector to all other connectors.
     * Adds queued invalidations from other connectors to this.
     * @return the combined invalidations from <code>invalidateSet</code> plus any other queued.
     */
    public Set<ServiceName> addInvalidateSet(final DatabaseConnector conn, final InvalidateSet invalidateSet) throws SQLException, RemoteException {
        // Create the list of all services modified
        final EnumSet<ServiceName> services = EnumSet.copyOf(invalidateSet.serverSets.keySet());
        services.addAll(invalidateSet.businessSets.keySet());

        // Invalidate local caches
        invalidateLocalCaches(services);
        DaemonHandler.invalidateServices(services);

        final List<Integer> addServers = new ArrayList<Integer>();
        final EnumSet<ServiceName> otherConnectorInvalidates = EnumSet.noneOf(ServiceName.class);
        try {
            database.executeTransaction(
                new DatabaseRunnable() {
                    @Override
                    public void run(DatabaseConnection db) {
                        try {
                            AOServerService rootAoServers = rootConnector.getAoServers();
                            BusinessAdministratorService rootBusinessAdministrators = rootConnector.getBusinessAdministrators();
                            BusinessService rootBusinesses = rootConnector.getBusinesses();
                            ServerService rootServers = rootConnector.getServers();
                            // Also send the signal to any failover parent
                            for(ServiceName service : services) {
                                Set<Integer> affectedServers = invalidateSet.getAffectedServers(service);
                                if(affectedServers!=null) {
                                    for(Integer server : affectedServers) {
                                        AOServer aoServer = rootAoServers.filterUnique(AOServer.COLUMN_SERVER, server);
                                        if(aoServer!=null) {
                                            AOServer failoverServer = aoServer.getFailoverServer();
                                            if(failoverServer!=null) {
                                                Integer failoverKey = failoverServer.getKey();
                                                if(!affectedServers.contains(failoverKey)) addServers.add(failoverKey);
                                            }
                                        }
                                    }
                                    if(!addServers.isEmpty()) {
                                        affectedServers.addAll(addServers);
                                        addServers.clear();
                                    }
                                }
                            }
                            synchronized(connectors) {
                                // Add to all other connectors
                                for(DatabaseConnector otherConn : connectors) {
                                    if(otherConn!=conn) {
                                        for(ServiceName service : services) {
                                            BusinessAdministrator otherBusinessAdministrator = null;
                                            // Filter by business
                                            Set<AccountingCode> affectedBusinesses = invalidateSet.getAffectedBusinesses(service);
                                            boolean businessMatches;
                                            if(affectedBusinesses==null || affectedBusinesses.contains(null)) businessMatches=true;
                                            else {
                                                businessMatches=false;
                                                if(otherBusinessAdministrator==null) otherBusinessAdministrator = rootBusinessAdministrators.get(otherConn.getConnectAs());
                                                for(AccountingCode affectedBusiness : affectedBusinesses) {
                                                    if(otherBusinessAdministrator.canAccessBusiness(rootBusinesses.get(affectedBusiness))) {
                                                        businessMatches=true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if(businessMatches) {
                                                // Filter by server
                                                Set<Integer> affectedServers = invalidateSet.getAffectedServers(service);
                                                boolean serverMatches;
                                                if(affectedServers==null || affectedServers.contains(null)) serverMatches=true;
                                                else {
                                                    serverMatches=false;
                                                    if(otherBusinessAdministrator==null) otherBusinessAdministrator = rootBusinessAdministrators.get(otherConn.getConnectAs());
                                                    for(int affectedServer : affectedServers) {
                                                        if(otherBusinessAdministrator.canAccessServer(rootServers.get(affectedServer))) {
                                                            serverMatches=true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                // Send the invalidate through
                                                if(serverMatches) otherConnectorInvalidates.add(service);
                                            }
                                        }
                                        if(!otherConnectorInvalidates.isEmpty()) {
                                            otherConn.servicesInvalidated(otherConnectorInvalidates);
                                            otherConnectorInvalidates.clear();
                                        }
                                    }
                                }
                                // Adds queued invalidations from other connectors to this.
                                conn.clearInvalidatedServices(services);
                            }
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw err;
        }
        return services;
    }
    // </editor-fold>
}
