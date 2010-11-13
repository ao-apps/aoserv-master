/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.DatabaseConnection;
import java.sql.SQLException;
import java.util.Set;

/**
 * A service where the table contains the same data for all users.
 *
 * @author  AO Industries, Inc.
 */
abstract class DatabasePublicService<
    K extends Comparable<K>,
    V extends AOServObject<K,V> & Comparable<V> & DtoFactory<?>
> extends DatabaseService<K,V> {

    DatabasePublicService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        super(connector, keyClass, valueClass);
    }

    /**
     * @see  #getPublicSet(DatabaseConnection)
     */
    @Override
    final protected Set<V> getSetMaster(DatabaseConnection db) throws SQLException {
        return getPublicSet(db);
    }

    /**
     * @see  #getPublicSet(DatabaseConnection)
     */
    @Override
    final protected Set<V> getSetDaemon(DatabaseConnection db) throws SQLException {
        return getPublicSet(db);
    }

    /**
     * @see  #getPublicSet(DatabaseConnection)
     */
    @Override
    final protected Set<V> getSetBusiness(DatabaseConnection db) throws SQLException {
        return getPublicSet(db);
    }

    /**
     * All accounts types use this method to retrieve the rows.
     */
    abstract protected Set<V> getPublicSet(DatabaseConnection db) throws SQLException;
}
