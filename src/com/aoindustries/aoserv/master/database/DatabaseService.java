/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.sql.DatabaseCallable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.table.IndexType;
import com.aoindustries.util.WrappedException;
import com.aoindustries.util.i18n.Money;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Currency;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseService<
    K extends Comparable<K>,
    V extends AOServObject<K>
> extends AbstractService<K,V> {

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

    /**
     * Null-safe conversion from String to AccountingCode.
     */
    protected static AccountingCode getAccountingCode(String accounting) throws ValidationException {
        return accounting==null ? null : AccountingCode.valueOf(accounting);
    }

    /**
     * Null-safe conversion from String to Email.
     */
    protected static Email getEmail(String email) throws ValidationException {
        return email==null ? null : Email.valueOf(email);
    }

    /**
     * Null-safe conversion from String to UserId.
     */
    protected static UserId getUserId(String userid) throws ValidationException {
        return userid==null ? null : UserId.valueOf(userid);
    }

    /**
     * Null-safe conversion from String to InetAddress.
     */
    protected static InetAddress getInetAddress(String address) throws ValidationException {
        return address==null ? null : InetAddress.valueOf(address);
    }

    /**
     * Null-safe conversion from String to DomainName.
     */
    protected static DomainName getDomainName(String domain) throws ValidationException {
        return domain==null ? null : DomainName.valueOf(domain);
    }

    protected static void addOptionalInInteger(StringBuilder sql, String sqlPrefix, Collection<? extends AOServObject<Integer>> objs, String sqlSuffix) {
        if(!objs.isEmpty()) {
            sql.append(sqlPrefix);
            boolean didOne = false;
            for(AOServObject<Integer> obj : objs) {
                if(didOne) sql.append(',');
                else didOne = true;
                Integer key = obj.getKey();
                if(key==null) throw new AssertionError("null key");
                sql.append(key.toString());
            }
            sql.append(sqlSuffix);
        }
    }

    /*
    protected static void addOptionalInIntegers(StringBuilder sql, String sqlPrefix, Collection<? extends Collection<? extends AOServObject<Integer>>> lists, String sqlSuffix) {
        boolean isEmpty = true;
        for(Collection<? extends AOServObject<Integer>> list : lists) {
            if(!list.isEmpty()) {
                isEmpty = false;
                break;
            }
        }
        if(!isEmpty) {
            sql.append(sqlPrefix);
            boolean didOne = false;
            for(Collection<? extends AOServObject<Integer>> list : lists) {
                for(AOServObject<Integer> obj : list) {
                    if(didOne) sql.append(',');
                    else didOne = true;
                    Integer key = obj.getKey();
                    if(key==null) throw new AssertionError("null key");
                    sql.append(key.toString());
                }
            }
            sql.append(sqlSuffix);
        }
    }*/
    // </editor-fold>

    final DatabaseConnector connector;

    DatabaseService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        super(keyClass, valueClass);
        this.connector = connector;
    }

    @Override
    final public DatabaseConnector getConnector() {
        return connector;
    }

    // <editor-fold defaultstate="collapsed" desc="getSet">
    @Override
    final public IndexedSet<V> getSet() throws RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return getList(db);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
            return IndexedSet.wrap(getServiceName(), list);
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected ArrayList<V> getList(DatabaseConnection db) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return getListMaster(db);
            case DAEMON : return getListDaemon(db);
            case BUSINESS : return getListBusiness(db);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    /**
     * Gets the unfiltered set.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListMaster(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the set filtered by server access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListDaemon(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the sets filtered by business access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException;
    // </editor-fold>

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
     * Implemented as a call to <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeMaster(DatabaseConnection db) throws RemoteException, SQLException {
        return getListMaster(db).size();
    }

    /**
     * Implemented as a call to <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        return getListDaemon(db).size();
    }

    /**
     * Implemented as a call to <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected int getSizeBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        return getListBusiness(db).size();
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

    private V get(K key, ArrayList<V> list) throws NoSuchElementException {
        for(V obj : list) if(obj.getKey().equals(key)) return obj;
        throw new NoSuchElementException("service="+getServiceName()+", key="+key);
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getMaster(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getListMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getDaemon(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getListDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V getBusiness(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        return get(key, getListBusiness(db));
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

    private V filterUnique(String columnName, Object value, ArrayList<V> list) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        IndexType indexType = methodColumn.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+columnName);
        Method method = methodColumn.getMethod();
        assert AOServServiceUtils.classesMatch(value.getClass(), method.getReturnType()) : "value class and return type mismatch: "+value.getClass().getName()+"!="+method.getReturnType().getName();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            V foundObj = null;
            for(V obj : list) {
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
     * Implemented as a sequential scan of all objects returned by <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueMaster(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getListMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueDaemon(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getListDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected V filterUniqueBusiness(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        return filterUnique(columnName, value, getListBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterUniqueSet">
    @Override
    final public IndexedSet<V> filterUniqueSet(final String columnName, final Set<?> values) throws RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterUniqueSet(db, columnName, values);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
            return IndexedSet.wrap(getServiceName(), list);
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected ArrayList<V> filterUniqueSet(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterUniqueSetMaster(db, columnName, values);
            case DAEMON : return filterUniqueSetDaemon(db, columnName, values);
            case BUSINESS : return filterUniqueSetBusiness(db, columnName, values);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private ArrayList<V> filterUniqueSet(String columnName, Set<?> values, ArrayList<V> list) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        IndexType indexType = methodColumn.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+columnName);
        Method method = methodColumn.getMethod();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            Set<Object> seenValues = assertsEnabled ? new HashSet<Object>(list.size()*4/3+1) : null;
            ArrayList<V> results = new ArrayList<V>(list.size());
            for(V obj : list) {
                Object retVal = method.invoke(obj);
                if(retVal!=null) {
                    if(assertsEnabled && !seenValues.add(retVal)) throw new AssertionError("Duplicate value in unique column "+getServiceName()+"."+columnName+": "+retVal);
                    if(values.contains(retVal)) results.add(obj);
                }
            }
            return results;
        } catch(IllegalAccessException err) {
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterUniqueSetMaster(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterUniqueSet(columnName, values, getListMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterUniqueSetDaemon(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterUniqueSet(columnName, values, getListDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterUniqueSetBusiness(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterUniqueSet(columnName, values, getListBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexed">
    @Override
    final public IndexedSet<V> filterIndexed(final String columnName, final Object value) throws RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexed(db, columnName, value);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
            return IndexedSet.wrap(getServiceName(), list);
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected ArrayList<V> filterIndexed(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterIndexedMaster(db, columnName, value);
            case DAEMON : return filterIndexedDaemon(db, columnName, value);
            case BUSINESS : return filterIndexedBusiness(db, columnName, value);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private ArrayList<V> filterIndexed(String columnName, Object value, ArrayList<V> list) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        if(methodColumn.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+columnName);
        Method method = methodColumn.getMethod();
        assert AOServServiceUtils.classesMatch(value.getClass(), method.getReturnType()) : "value class and return type mismatch: "+value.getClass().getName()+"!="+method.getReturnType().getName();
        try {
            ArrayList<V> results = new ArrayList<V>(list.size());
            for(V obj : list) {
                if(value.equals(method.invoke(obj))) results.add(obj);
            }
            return results;
        } catch(IllegalAccessException err) {
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedMaster(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return new ArrayList<V>(0);
        return filterIndexed(columnName, value, getListMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedDaemon(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return new ArrayList<V>(0);
        return filterIndexed(columnName, value, getListDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedBusiness(DatabaseConnection db, String columnName, Object value) throws RemoteException, SQLException {
        if(value==null) return new ArrayList<V>(0);
        return filterIndexed(columnName, value, getListBusiness(db));
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexedSet">
    @Override
    final public IndexedSet<V> filterIndexedSet(final String columnName, final Set<?> values) throws RemoteException, RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexedSet(db, columnName, values);
                        } catch(RemoteException err) {
                            throw new WrappedException(err);
                        }
                    }
                }
            );
            return IndexedSet.wrap(getServiceName(), list);
        } catch(WrappedException err) {
            Throwable cause = err.getCause();
            if(cause instanceof RemoteException) throw (RemoteException)cause;
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    final protected ArrayList<V> filterIndexedSet(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return filterIndexedSetMaster(db, columnName, values);
            case DAEMON : return filterIndexedSetDaemon(db, columnName, values);
            case BUSINESS : return filterIndexedSetBusiness(db, columnName, values);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    private ArrayList<V> filterIndexedSet(String columnName, Set<?> values, ArrayList<V> list) throws SQLException {
        MethodColumn methodColumn = table.getColumn(columnName);
        if(methodColumn.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+columnName);
        Method method = methodColumn.getMethod();
        try {
            ArrayList<V> results = new ArrayList<V>();
            for(V obj : list) {
                Object retVal = method.invoke(obj);
                if(retVal!=null && values.contains(retVal)) results.add(obj);
            }
            return results;
        } catch(IllegalAccessException err) {
            throw new SQLException(err);
        } catch(InvocationTargetException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListMaster</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedSetMaster(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterIndexedSet(columnName, values, getListMaster(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListDaemon</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedSetDaemon(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterIndexedSet(columnName, values, getListDaemon(db));
    }

    /**
     * Implemented as a sequential scan of all objects returned by <code>getListBusiness</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     */
    protected ArrayList<V> filterIndexedSetBusiness(DatabaseConnection db, String columnName, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        return filterIndexedSet(columnName, values, getListBusiness(db));
    }
    // </editor-fold>
}
