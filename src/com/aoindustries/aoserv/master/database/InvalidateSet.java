/*
 * Copyright 2001-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

/**
 * In the request lifecycle, service invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the service
 * updates.  For effeciency, each server and accounting code will only be notified once per service per
 * request.
 *
 * @author  AO Industries, Inc.
 */
final public class InvalidateSet {

    EnumMap<ServiceName,Set<Integer>> serverSets = new EnumMap<ServiceName, Set<Integer>>(ServiceName.class);
    EnumMap<ServiceName,Set<AccountingCode>> businessSets = new EnumMap<ServiceName, Set<AccountingCode>>(ServiceName.class);

    public void clear() {
        // Clear the servers
        for(Set<Integer> serverSet : serverSets.values()) serverSet.clear();
        for(Set<AccountingCode> businessSet : businessSets.values()) businessSet.clear();
    }

    /**
     * Invalidation will apply to all servers
     *
     * @param business if null invalidation will apply to all businesses
     */
    public void add(
        ServiceName service,
        AccountingCode business
    ) {
        addBusiness(service, business);
        addServer(service, null);
    }

    /**
     * @param business if null invalidation will apply to all businesses
     * @param server if null invalidation will apply to all servers
     */
    public void add(
        ServiceName service,
        AccountingCode business,
        Integer server
    ) {
        addBusiness(service, business);
        addServer(service, server);
    }

    /**
     * Invalidation will apply to all servers.
     *
     * @param businesses if null invalidation will apply to all businesses
     */
    public void add(
        ServiceName service,
        Collection<AccountingCode> businesses
    ) {
        addBusinesses(service, businesses);
        addServer(service, null);
    }

    /**
     * @param businesses if null invalidation will apply to all businesses
     * @param server if null invalidation will apply to all servers
     */
    public void add(
        ServiceName service,
        Collection<AccountingCode> businesses,
        Integer server
    ) {
        addBusinesses(service, businesses);
        addServer(service, server);
    }

    /**
     * @param business if null invalidation will apply to all businesses
     * @param servers if null invalidation will apply to all servers
     */
    public void add(
        ServiceName service,
        AccountingCode business,
        Collection<Integer> servers
    ) {
        addBusiness(service, business);
        addServers(service, servers);
    }

    /**
     * @param businesses if null invalidation will apply to all businesses
     * @param servers if null invalidation will apply to all servers
     */
    public void add(
        ServiceName service,
        Collection<AccountingCode> businesses,
        Collection<Integer> servers
    ) {
        addBusinesses(service, businesses);
        addServers(service, servers);
    }

    /**
     * Add to the business sets
     */
    private void addBusiness(ServiceName service, AccountingCode accounting) {
        Set<AccountingCode> businessSet=businessSets.get(service);
        if(businessSet==null) {
            businessSet=new HashSet<AccountingCode>();
            businessSets.put(service, businessSet);
        }
        if(!businessSet.contains(null)) {
            if(accounting==null) {
                businessSet.clear();
                businessSet.add(null);
            } else {
                businessSet.add(accounting);
            }
        }
    }

    /**
     * Add to the business sets
     */
    private void addBusinesses(ServiceName service, Collection<AccountingCode> businesses) {
        Set<AccountingCode> businessSet=businessSets.get(service);
        if(businessSet==null) {
            businessSet=new HashSet<AccountingCode>();
            businessSets.put(service, businessSet);
        }
        if(!businessSet.contains(null)) {
            if(businesses==null) {
                businessSet.clear();
                businessSet.add(null);
            } else {
                for(AccountingCode accounting : businesses) {
                    if(accounting==null) throw new IllegalArgumentException("accounting is null");
                    businessSet.add(accounting);
                }
            }
        }
    }

    /**
     * Add to the server sets
     */
    private void addServer(ServiceName service, Integer pkey) {
        Set<Integer> serverSet=serverSets.get(service);
        if(serverSet==null) {
            serverSet=new HashSet<Integer>();
            serverSets.put(service, serverSet);
        }
        if(!serverSet.contains(null)) {
            if(pkey==null) {
                serverSet.clear();
                serverSet.add(null);
            } else {
                serverSet.add(pkey);
            }
        }
    }

    /**
     * Add to the server sets
     */
    private void addServers(ServiceName service, Collection<Integer> servers) {
        Set<Integer> serverSet=serverSets.get(service);
        if(serverSet==null) {
            serverSet=new HashSet<Integer>();
            serverSets.put(service, serverSet);
        }
        if(!serverSet.contains(null)) {
            if(servers==null) {
                serverSet.clear();
                serverSet.add(null);
            } else {
                for(Integer pkey : servers) {
                    if(pkey==null) throw new IllegalArgumentException("pkey is null");
                    serverSet.add(pkey);
                }
            }
        }
    }

    // Gets the set of affected business for the provided service
    // or null if all affected.
    public Set<AccountingCode> getAffectedBusinesses(ServiceName service) {
        return businessSets.get(service);
    }

    // Gets the set of affected servers for the provided service
    // or null if all affected.
    public Set<Integer> getAffectedServers(ServiceName service) {
        return serverSets.get(service);
    }

    /*
    public boolean isInvalid(ServiceName service) {
        return serverLists.containsKey(service) || businessLists.containsKey(service);
    }
     */
}
