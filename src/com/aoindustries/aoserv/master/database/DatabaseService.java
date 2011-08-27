/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
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

    // <editor-fold defaultstate="collapsed" desc="Utilities">
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

    abstract protected ArrayList<V> getList(DatabaseConnection db) throws RemoteException, SQLException;
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

    /**
     * Implemented as a call to <code>getSize</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see  #getSize(com.aoindustries.sql.DatabaseConnection)
     */
    protected boolean isEmpty(DatabaseConnection db) throws RemoteException, SQLException {
        return getSize(db)==0;
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

    /**
     * Implemented as a call to <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see  #getList(db)
     */
    protected int getSize(DatabaseConnection db) throws RemoteException, SQLException {
        return getList(db).size();
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

    /**
     * Implemented as a sequential scan of all objects returned by <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see #getList(db)
     */
    protected V get(DatabaseConnection db, K key) throws RemoteException, SQLException, NoSuchElementException {
        ArrayList<V> list = getList(db);
        for(V obj : list) if(obj.getKey().equals(key)) return obj;
        throw new NoSuchElementException("service="+getServiceName()+", key="+key);
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterUnique">
    @Override
    final public V filterUnique(final MethodColumn column, final Object value) throws RemoteException, RemoteException {
        try {
            return connector.factory.database.executeTransaction(
                new DatabaseCallable<V>() {
                    @Override
                    public V call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterUnique(db, column, value);
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

    /**
     * Implemented as a sequential scan of all objects returned by <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see #getList(db)
     */
    protected V filterUnique(DatabaseConnection db, MethodColumn column, Object value) throws RemoteException, SQLException {
        if(value==null) return null;
        ArrayList<V> list = getList(db);
        IndexType indexType = column.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+column);
        Method method = column.getMethod();
        assert AOServServiceUtils.classesMatch(value.getClass(), method.getReturnType()) : "value class and return type mismatch: "+value.getClass().getName()+"!="+method.getReturnType().getName();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            V foundObj = null;
            for(V obj : list) {
                if(value.equals(method.invoke(obj))) {
                    if(!assertsEnabled) return obj;
                    assert foundObj==null : "Duplicate value in unique column "+getServiceName()+"."+column+": "+value;
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
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterUniqueSet">
    @Override
    final public IndexedSet<V> filterUniqueSet(final MethodColumn column, final Set<?> values) throws RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterUniqueSet(db, column, values);
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

    /**
     * Implemented as a sequential scan of all objects returned by <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see  #getList(db)
     */
    protected ArrayList<V> filterUniqueSet(DatabaseConnection db, MethodColumn column, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        ArrayList<V> list = getList(db);
        IndexType indexType = column.getIndexType();
        if(indexType!=IndexType.PRIMARY_KEY && indexType!=IndexType.UNIQUE) throw new IllegalArgumentException("Column neither primary key nor unique: "+column);
        Method method = column.getMethod();
        try {
            boolean assertsEnabled = false;
            assert assertsEnabled = true; // Intentional side effect!!!
            Set<Object> seenValues = assertsEnabled ? new HashSet<Object>(list.size()*4/3+1) : null;
            ArrayList<V> results = new ArrayList<V>(list.size());
            for(V obj : list) {
                Object retVal = method.invoke(obj);
                if(retVal!=null) {
                    if(assertsEnabled && !seenValues.add(retVal)) throw new AssertionError("Duplicate value in unique column "+getServiceName()+"."+column+": "+retVal);
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
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexed">
    @Override
    final public IndexedSet<V> filterIndexed(final MethodColumn column, final Object value) throws RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexed(db, column, value);
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

    /**
     * Implemented as a sequential scan of all objects returned by <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see  #getList(db)
     */
    protected ArrayList<V> filterIndexed(DatabaseConnection db, MethodColumn column, Object value) throws RemoteException, SQLException {
        if(value==null) return new ArrayList<V>(0);
        ArrayList<V> list = getList(db);
        if(column.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+column);
        Method method = column.getMethod();
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
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="filterIndexedSet">
    @Override
    final public IndexedSet<V> filterIndexedSet(final MethodColumn column, final Set<?> values) throws RemoteException, RemoteException {
        try {
            ArrayList<V> list = connector.factory.database.executeTransaction(
                new DatabaseCallable<ArrayList<V>>() {
                    @Override
                    public ArrayList<V> call(DatabaseConnection db) throws SQLException {
                        try {
                            return filterIndexedSet(db, column, values);
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

    /**
     * Implemented as a sequential scan of all objects returned by <code>getList</code>.
     * Subclasses should only provide more efficient implementations when required for performance reasons.
     *
     * @see  #getList(db)
     */
    protected ArrayList<V> filterIndexedSet(DatabaseConnection db, MethodColumn column, Set<?> values) throws RemoteException, SQLException {
        if(values==null || values.isEmpty()) return new ArrayList<V>(0);
        ArrayList<V> list = getList(db);
        if(column.getIndexType()!=IndexType.INDEXED) throw new IllegalArgumentException("Column not indexed: "+column);
        Method method = column.getMethod();
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
    // </editor-fold>
}
