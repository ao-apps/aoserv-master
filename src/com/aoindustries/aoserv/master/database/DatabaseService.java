package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
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
import com.aoindustries.sql.DatabaseCallable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.table.IndexType;
import com.aoindustries.table.Table;
import com.aoindustries.util.WrappedException;
import com.aoindustries.util.i18n.Money;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Currency;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseService<K extends Comparable<K>,V extends AOServObject<K,V>> implements AOServService<DatabaseConnector,DatabaseConnectorFactory,K,V> {

    // <editor-fold defaultstate="collapsed" desc="Business Tree Joins">
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
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Utilities">
    /**
     * Gets the int value or <code>-1</code> if <code>null</code>.
     */
    /* TODO: used?
    protected static int getNullableInt(ResultSet result, int pos) throws SQLException {
        int i = result.getInt(pos);
        return result.wasNull() ? -1 : i;
    }
     */

    /**
     * Gets the float value or <code>Float.NaN</code> if <code>null</code>.
     */
    /* TODO: used?
    protected static float getNullableFloat(ResultSet result, int pos) throws SQLException {
        float f = result.getInt(pos);
        return result.wasNull() ? Float.NaN : f;
    }
     */

    /**
     * Gets a Money type from two columns of a <code>ResultSet</code>.  Supports
     * <code>null</code>.  If value is non-null then currency must also be non-null.
     */
    protected static Money getMoney(ResultSet result, String currencyColumnLabel, String valueColumnLabel) throws SQLException {
        BigDecimal value = result.getBigDecimal(valueColumnLabel);
        if(value==null) return null;
        String currencyCode = result.getString(currencyColumnLabel);
        if(currencyCode==null) throw new SQLException(currencyColumnLabel+"==null && "+valueColumnLabel+"!=null");
        return new Money(Currency.getInstance(currencyCode), value);
    }

    protected static void addOptionalInInteger(StringBuilder sql, String sqlPrefix, Set<? extends AOServObject<Integer,?>> set, String sqlSuffix) {
        if(!set.isEmpty()) {
            sql.append(sqlPrefix);
            boolean didOne = false;
            for(AOServObject<Integer,?> obj : set) {
                if(didOne) sql.append(',');
                else didOne = true;
                Integer key = obj.getKey();
                sql.append(key==null ? "null" : key.toString());
            }
            sql.append(sqlSuffix);
        }
    }
    // </editor-fold>

    final DatabaseConnector connector;
    final ServiceName serviceName;
    final AOServServiceUtils.AnnotationTable<K,V> table;
    final Map<K,V> map;

    DatabaseService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        this.connector = connector;
        serviceName = AOServServiceUtils.findServiceNameByAnnotation(getClass());
        table = new AOServServiceUtils.AnnotationTable<K,V>(this, valueClass);
        map = new AOServServiceUtils.ServiceMap<K,V>(this, keyClass, valueClass);
    }

    @Override
    final public String toString() {
        return getServiceName().toString();
    }

    @Override
    final public DatabaseConnector getConnector() {
        return connector;
    }

    @Override
    final public boolean isAoServObjectServiceSettable() {
        return true;
    }

    // <editor-fold defaultstate="collapsed" desc="getSet">
    @Override
    final public IndexedSet<V> getSet() throws RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<IndexedSet<V>>() {
                    @Override
                    public IndexedSet<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return getSet(db);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected IndexedSet<V> getSet(DatabaseConnection db) throws RemoteException, SQLException {
        Set<V> set;
        switch(connector.getAccountType(db)) {
            case MASTER : set = getSetMaster(db); break;
            case DAEMON : set = getSetDaemon(db); break;
            case BUSINESS : set = getSetBusiness(db); break;
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
        return IndexedSet.wrap(set);
    }

    /**
     * Gets the unfiltered set.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetMaster(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the set filtered by server access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetDaemon(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the sets filtered by business access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected Set<V> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException;
    // </editor-fold>

    @Override
    final public ServiceName getServiceName() {
        return serviceName;
    }

    @Override
    final public Table<MethodColumn,V> getTable() {
        return table;
    }

    @Override
    final public Map<K,V> getMap() {
        return map;
    }

    // <editor-fold defaultstate="collapsed" desc="isEmpty">
    @Override
    final public boolean isEmpty() throws RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<Boolean>() {
                    @Override
                    public Boolean call(DatabaseConnection db) throws SQLException {
                        try {
                            return isEmpty(db);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected boolean isEmpty(DatabaseConnection db) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return isEmptyMaster(db);
            case DAEMON : return isEmptyDaemon(db);
            case BUSINESS : return isEmptyBusiness(db);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    /**
     * Implemented as a call to <code>getSizeMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyMaster(DatabaseConnection db) throws RemoteException, SQLException {
        return getSizeMaster(db)==0;
    }

    /**
     * Implemented as a call to <code>getSizeDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        return getSizeDaemon(db)==0;
    }

    /**
     * Implemented as a call to <code>getSizeBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected boolean isEmptyBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        return getSizeBusiness(db)==0;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="getSize">
    @Override
    final public int getSize() throws RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<Integer>() {
                    @Override
                    public Integer call(DatabaseConnection db) throws SQLException {
                        try {
                            return getSize(db);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected int getSize(DatabaseConnection db) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return getSizeMaster(db);
            case DAEMON : return getSizeDaemon(db);
            case BUSINESS : return getSizeBusiness(db);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    /**
     * Implemented as a call to <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeMaster(DatabaseConnection db) throws RemoteException, SQLException {
        return getSetMaster(db).size();
    }

    /**
     * Implemented as a call to <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        return getSetDaemon(db).size();
    }

    /**
     * Implemented as a call to <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        return getSetBusiness(db).size();
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="get">
    @Override
    final public V get(final K key) throws RemoteException, NoSuchElementException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<V>() {
                    @Override
                    public V call(DatabaseConnection db) throws SQLException, NoSuchElementException {
                        try {
                            return get(db, key);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected V get(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        switch(connector.getAccountType(db)) {
            case MASTER : return getMaster(db, key);
            case DAEMON : return getDaemon(db, key);
            case BUSINESS : return getBusiness(db, key);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private V get(K key, Set<V> set) throws NoSuchElementException {
        for(V obj : set) if(obj.getKey().equals(key)) return obj;
        throw new NoSuchElementException("service="+getServiceName().name()+", key="+key);
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getMaster(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getSetMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getDaemon(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getSetDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getBusiness(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getSetBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterUnique">
    @Override
    final public V filterUnique(final String columnName, final Object value) throws RemoteException, RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<V>() {
                    @Override
                    public V call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterUnique(db, columnName, value);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected V filterUnique(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterUniqueMaster(db, columnName, value);
            case DAEMON : return filterUniqueDaemon(db, columnName, value);
            case BUSINESS : return filterUniqueBusiness(db, columnName, value);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private V filterUnique(String columnName, Object value, Set<V> set) throws SQLException {
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
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueMaster(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueDaemon(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueBusiness(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getSetBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterUniqueSet">
    @Override
    final public IndexedSet<V> filterUniqueSet(final String columnName, final Set<?> values) throws RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<IndexedSet<V>>() {
                    @Override
                    public IndexedSet<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterUniqueSet(db, columnName, values);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected IndexedSet<V> filterUniqueSet(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterUniqueSetMaster(db, columnName, values);
            case DAEMON : return filterUniqueSetDaemon(db, columnName, values);
            case BUSINESS : return filterUniqueSetBusiness(db, columnName, values);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private IndexedSet<V> filterUniqueSet(String columnName, Set<?> values, Set<V> set) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        IndexType indexType = methodColumn.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+columnName);
        Method method = methodColumn.getMethod();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            Set<Object> seenValues = assertsEnabled ? new HashSet<Object>(set.size()*4/3+1) : null;
            Set<V> results = new HashSet<V>();
            for(V obj : set) {
                Object retVal = method.invoke(obj);
                if(retVal!=null) {
                    if(assertsEnabled && !seenValues.add(retVal)) throw new AssertionError("Duplicate value in unique column "+getServiceName()+"."+columnName+": "+retVal);
                    if(values.contains(retVal) && !results.add(obj)) throw new AssertionError("Already in set: "+obj);
                }
            }
            return IndexedSet.wrap(results);
        } catch(IllegalAccessException err) {
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetMaster(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetDaemon(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterUniqueSetBusiness(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterUniqueSet(columnName, values, getSetBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexed">
    @Override
    final public IndexedSet<V> filterIndexed(final String columnName, final Object value) throws RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<IndexedSet<V>>() {
                    @Override
                    public IndexedSet<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexed(db, columnName, value);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected IndexedSet<V> filterIndexed(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterIndexedMaster(db, columnName, value);
            case DAEMON : return filterIndexedDaemon(db, columnName, value);
            case BUSINESS : return filterIndexedBusiness(db, columnName, value);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private IndexedSet<V> filterIndexed(String columnName, Object value, Set<V> set) throws SQLException {
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
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedMaster(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return IndexedSet.emptyIndexedSet();
        return filterIndexed(columnName, value, getSetMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedDaemon(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return IndexedSet.emptyIndexedSet();
        return filterIndexed(columnName, value, getSetDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedBusiness(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return IndexedSet.emptyIndexedSet();
        return filterIndexed(columnName, value, getSetBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexedSet">
    @Override
    final public IndexedSet<V> filterIndexedSet(final String columnName, final Set<?> values) throws RemoteException, RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<IndexedSet<V>>() {
                    @Override
                    public IndexedSet<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexedSet(db, columnName, values);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected IndexedSet<V> filterIndexedSet(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterIndexedSetMaster(db, columnName, values);
            case DAEMON : return filterIndexedSetDaemon(db, columnName, values);
            case BUSINESS : return filterIndexedSetBusiness(db, columnName, values);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private IndexedSet<V> filterIndexedSet(String columnName, Set<?> values, Set<V> set) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        if(methodColumn.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+columnName);
        Method method = methodColumn.getMethod();
        try {
            Set<V> results = new HashSet<V>();
            for(V obj : set) {
                Object retVal = method.invoke(obj);
                if(retVal!=null && values.contains(retVal)) if(!results.add(obj)) throw new AssertionError("Already in set: "+obj);
            }
            return IndexedSet.wrap(results);
        } catch(IllegalAccessException err) {
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedSetMaster(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterIndexedSet(columnName, values, getSetMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedSetDaemon(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterIndexedSet(columnName, values, getSetDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getSetBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected IndexedSet<V> filterIndexedSetBusiness(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return IndexedSet.emptyIndexedSet();
        return filterIndexedSet(columnName, values, getSetBusiness(db));
    }
    // </editor-fold>
}
