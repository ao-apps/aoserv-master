package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServService;
import com.aoindustries.aoserv.client.AOServServiceUtils;
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.ServiceName;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.table.Table;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseService<K extends Comparable<K>,V extends AOServObject<K,V>> implements AOServService<DatabaseConnector,DatabaseConnectorFactory,K,V> {

    /**
     * The joins used for the business tree.
     */
    static final String
        BU1_PARENTS_JOIN=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting,\n",
        BU1_PARENTS_JOIN_NO_COMMA=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting\n",
        BU2_PARENTS_JOIN=
              "      businesses bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+"\n"
            + "      left join businesses bu8 on bu7.parent=bu8.accounting\n"
            + "      left join businesses bu9 on bu8.parent=bu9.accounting\n"
            + "      left join businesses bu10 on bu9.parent=bu10.accounting\n"
            + "      left join businesses bu11 on bu10.parent=bu11.accounting\n"
            + "      left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+" on bu11.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".accounting,\n"
    ;

    /**
     * The where clauses that accompany the joins.
     */
    static final String
        UN_BU1_PARENTS_WHERE=
              "    un.accounting=bu1.accounting\n"
            + "    or un.accounting=bu1.parent\n"
            + "    or un.accounting=bu2.parent\n"
            + "    or un.accounting=bu3.parent\n"
            + "    or un.accounting=bu4.parent\n"
            + "    or un.accounting=bu5.parent\n"
            + "    or un.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN1_BU1_PARENTS_OR_WHERE=
              "    or un1.accounting=bu1.accounting\n"
            + "    or un1.accounting=bu1.parent\n"
            + "    or un1.accounting=bu2.parent\n"
            + "    or un1.accounting=bu3.parent\n"
            + "    or un1.accounting=bu4.parent\n"
            + "    or un1.accounting=bu5.parent\n"
            + "    or un1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN1_BU1_PARENTS_WHERE=
              "    un1.accounting=bu1.accounting\n"
            + "    or un1.accounting=bu1.parent\n"
            + "    or un1.accounting=bu2.parent\n"
            + "    or un1.accounting=bu3.parent\n"
            + "    or un1.accounting=bu4.parent\n"
            + "    or un1.accounting=bu5.parent\n"
            + "    or un1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN2_BU2_PARENTS_OR_WHERE=
              "        or un2.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or un2.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or un2.accounting=bu8.parent\n"
            + "        or un2.accounting=bu9.parent\n"
            + "        or un2.accounting=bu10.parent\n"
            + "        or un2.accounting=bu11.parent\n"
            + "        or un2.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n",
        UN3_BU2_PARENTS_WHERE=
              "        un3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or un3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or un3.accounting=bu8.parent\n"
            + "        or un3.accounting=bu9.parent\n"
            + "        or un3.accounting=bu10.parent\n"
            + "        or un3.accounting=bu11.parent\n"
            + "        or un3.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n"
    ;

    /**
     * Gets the int value or <code>-1</code> if <code>null</code>.
     */
    protected static int getNullableInt(ResultSet result, int pos) throws SQLException {
        int i = result.getInt(pos);
        return result.wasNull() ? -1 : i;
    }

    /**
     * Gets the float value or <code>Float.NaN</code> if <code>null</code>.
     */
    protected static float getNullableFloat(ResultSet result, int pos) throws SQLException {
        float f = result.getInt(pos);
        return result.wasNull() ? Float.NaN : f;
    }

    final DatabaseConnector connector;
    final ServiceName serviceName;
    final Table<V> table;
    final Map<K,V> map;

    DatabaseService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        this.connector = connector;
        serviceName = AOServServiceUtils.findServiceNameByAnnotation(getClass());
        table = new AOServServiceUtils.AnnotationTable<K,V>(this, valueClass);
        map = new AOServServiceUtils.ServiceMap<K,V>(this, keyClass, valueClass);
    }

    @Override
    final public String toString() {
        return getServiceName().getDisplay();
    }

    final public DatabaseConnector getConnector() {
        return connector;
    }

    @Override
    final public Set<V> getSet() throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return AOServServiceUtils.unmodifiableSet(getSetMaster());
                case DAEMON : return AOServServiceUtils.unmodifiableSet(getSetDaemon());
                case BUSINESS : return AOServServiceUtils.unmodifiableSet(getSetBusiness());
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Gets the unfiltered set.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetMaster() throws IOException, SQLException;

    /**
     * Gets the set filtered by server access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetDaemon() throws IOException, SQLException;

    /**
     * Gets the sets filtered by business access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetBusiness() throws IOException, SQLException;

    /**
     * Implemented as a call to <code>getSize</code> with sorting performed by <code>TreeSet</code>.
     */
    final public SortedSet<V> getSortedSet() throws RemoteException {
        return Collections.unmodifiableSortedSet(new TreeSet<V>(getSet()));
    }

    final public ServiceName getServiceName() {
        return serviceName;
    }

    final public Table<V> getTable() {
        return table;
    }

    final public Map<K,V> getMap() {
        return map;
    }

    final public V get(K key) throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return getMaster(key);
                case DAEMON : return getDaemon(key);
                case BUSINESS : return getBusiness(key);
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getMaster(K key) throws IOException, SQLException {
        for(V obj : getSetMaster()) if(obj.getKey().equals(key)) return obj;
        return null;
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getDaemon(K key) throws IOException, SQLException {
        for(V obj : getSetDaemon()) if(obj.getKey().equals(key)) return obj;
        return null;
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getBusiness(K key) throws IOException, SQLException {
        for(V obj : getSetBusiness()) if(obj.getKey().equals(key)) return obj;
        return null;
    }

    final public boolean isEmpty() throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return isEmptyMaster();
                case DAEMON : return isEmptyDaemon();
                case BUSINESS : return isEmptyBusiness();
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a call to <code>getSizeMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyMaster() throws IOException, SQLException {
        return getSizeMaster()==0;
    }

    /**
     * Implemented as a call to <code>getSizeDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyDaemon() throws IOException, SQLException {
        return getSizeDaemon()==0;
    }

    /**
     * Implemented as a call to <code>getSizeBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyBusiness() throws IOException, SQLException {
        return getSizeBusiness()==0;
    }

    final public int getSize() throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return getSizeMaster();
                case DAEMON : return getSizeDaemon();
                case BUSINESS : return getSizeBusiness();
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a call to <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeMaster() throws IOException, SQLException {
        return getSetMaster().size();
    }

    /**
     * Implemented as a call to <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeDaemon() throws IOException, SQLException {
        return getSetDaemon().size();
    }

    /**
     * Implemented as a call to <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeBusiness() throws IOException, SQLException {
        return getSetBusiness().size();
    }
}
