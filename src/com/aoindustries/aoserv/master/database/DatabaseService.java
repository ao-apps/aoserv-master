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
import com.aoindustries.aoserv.client.IndexedSet;
import com.aoindustries.aoserv.client.MethodColumn;
import com.aoindustries.aoserv.client.ServiceName;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.table.IndexType;
import com.aoindustries.table.Table;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    //final Class<K> keyClass;
    final ServiceName serviceName;
    final AOServServiceUtils.AnnotationTable<K,V> table;
    final Map<K,V> map;

    DatabaseService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        this.connector = connector;
        //this.keyClass = keyClass;
        serviceName = AOServServiceUtils.findServiceNameByAnnotation(getClass());
        table = new AOServServiceUtils.AnnotationTable<K,V>(this, valueClass);
        map = new AOServServiceUtils.ServiceMap<K,V>(this, keyClass, valueClass);
    }

    @Override
    final public String toString() {
        return getServiceName().toString(connector.getLocale());
    }

    final public DatabaseConnector getConnector() {
        return connector;
    }

    final public boolean isAoServObjectServiceSettable() {
        return true;
    }

    @Override
    final public IndexedSet<V> getSet() throws RemoteException {
        try {
            Set<V> set;
            switch(connector.getAccountType()) {
                case MASTER : set = getSetMaster(); break;
                case DAEMON : set = getSetDaemon(); break;
                case BUSINESS : set = getSetBusiness(); break;
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
            return IndexedSet.wrap(set);
        } catch(RemoteException err) {
            throw err;
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

    final public ServiceName getServiceName() {
        return serviceName;
    }

    final public Table<MethodColumn,V> getTable() {
        return table;
    }

    final public Map<K,V> getMap() {
        return map;
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
        } catch(RemoteException err) {
            throw err;
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
        } catch(RemoteException err) {
            throw err;
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

    final public V get(K key) throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return getMaster(key);
                case DAEMON : return getDaemon(key);
                case BUSINESS : return getBusiness(key);
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(RemoteException err) {
            throw err;
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    private V get(K key, Set<V> set) {
        for(V obj : set) if(obj.getKey().equals(key)) return obj;
        return null;
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getMaster(K key) throws IOException, SQLException {
        return get(key, getSetMaster());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getDaemon(K key) throws IOException, SQLException {
        return get(key, getSetDaemon());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getBusiness(K key) throws IOException, SQLException {
        return get(key, getSetBusiness());
    }

    final public V filterUnique(String columnName, Object value) throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return filterUniqueMaster(columnName, value);
                case DAEMON : return filterUniqueDaemon(columnName, value);
                case BUSINESS : return filterUniqueBusiness(columnName, value);
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(RemoteException err) {
            throw err;
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    private V filterUnique(String columnName, Object value, Set<V> set) throws IOException, SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        IndexType indexType = methodColumn.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+columnName);
        Method method = methodColumn.getMethod();
        assert AOServServiceUtils.classesMatch(value.getClass(), method.getReturnType()) : "value class and return type mismatch: "+value.getClass().getName()+"!="+method.getReturnType().getName();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            V foundObj = null;
            for(V obj : set) {
                if(value.equals(method.invoke(obj))) {
                    if(!assertsEnabled) return obj;
                    assert foundObj==null : "Duplicate value in unique column "+getServiceName()+"."+columnName+": "+value;
                    foundObj = obj;
                }
            }
            return foundObj;
        } catch(IllegalAccessException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(InvocationTargetException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueMaster(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetMaster());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueDaemon(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetDaemon());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueBusiness(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetBusiness());
    }

    final public IndexedSet<V> filterUniqueSet(String columnName, Set<?> values) throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return filterUniqueSetMaster(columnName, values);
                case DAEMON : return filterUniqueSetDaemon(columnName, values);
                case BUSINESS : return filterUniqueSetBusiness(columnName, values);
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(RemoteException err) {
            throw err;
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    private IndexedSet<V> filterUniqueSet(String columnName, Set<?> values, Set<V> set) throws IOException, SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        IndexType indexType = methodColumn.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+columnName);
        Method method = methodColumn.getMethod();
        try {
            Set<V> results = new HashSet<V>();
            for(V obj : set) {
                Object retVal = method.invoke(obj);
                if(retVal!=null && values.contains(retVal)) if(!results.add(obj)) throw new AssertionError("Duplicate value in unique column "+getServiceName()+"."+columnName+": "+retVal);
            }
            return IndexedSet.wrap(results);
        } catch(IllegalAccessException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(InvocationTargetException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetMaster(String columnName, Set<?> values) throws IOException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetMaster());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetDaemon(String columnName, Set<?> values) throws IOException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetDaemon());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetBusiness(String columnName, Set<?> values) throws IOException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetBusiness());
    }

    final public IndexedSet<V> filterIndexed(String columnName, Object value) throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER : return filterIndexedMaster(columnName, value);
                case DAEMON : return filterIndexedDaemon(columnName, value);
                case BUSINESS : return filterIndexedBusiness(columnName, value);
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(RemoteException err) {
            throw err;
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    private IndexedSet<V> filterIndexed(String columnName, Object value, Set<V> set) throws IOException, SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        if(methodColumn.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+columnName);
        Method method = methodColumn.getMethod();
        assert AOServServiceUtils.classesMatch(value.getClass(), method.getReturnType()) : "value class and return type mismatch: "+value.getClass().getName()+"!="+method.getReturnType().getName();
        try {
            Set<V> results = new HashSet<V>();
            for(V obj : set) {
                if(value.equals(method.invoke(obj))) results.add(obj);
            }
            return IndexedSet.wrap(results);
        } catch(IllegalAccessException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(InvocationTargetException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedMaster(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterIndexed(columnName, value, getSetMaster());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedDaemon(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterIndexed(columnName, value, getSetDaemon());
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedBusiness(String columnName, Object value) throws IOException, SQLException {
        if(value==null) return null;
        return filterIndexed(columnName, value, getSetBusiness());
    }
}
