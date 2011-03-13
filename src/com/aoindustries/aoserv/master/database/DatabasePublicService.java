/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.DatabaseConnection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * A service where the table contains the same data for all users.
 *
 * @author  AO Industries, Inc.
 */
abstract class DatabasePublicService<
    K extends Comparable<K>,
    V extends AOServObject<K>
> extends DatabaseService<K,V> {

    DatabasePublicService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        super(connector, keyClass, valueClass);
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected ArrayList<V> getListMaster(DatabaseConnection db) throws SQLException {
        return getPublicList(db);
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected ArrayList<V> getListDaemon(DatabaseConnection db) throws SQLException {
        return getPublicList(db);
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected ArrayList<V> getListBusiness(DatabaseConnection db) throws SQLException {
        return getPublicList(db);
    }

    /**
     * All accounts types use this method to retrieve the rows.
     */
    abstract protected ArrayList<V> getPublicList(DatabaseConnection db) throws SQLException;
}
