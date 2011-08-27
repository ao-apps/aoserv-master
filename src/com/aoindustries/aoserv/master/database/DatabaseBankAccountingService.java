/*
 * Copyright 2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.DatabaseConnection;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * A service that only provides rows to master users who have access to bank accounting.
 *
 * @author  AO Industries, Inc.
 */
abstract class DatabaseBankAccountingService<
    K extends Comparable<K>,
    V extends AOServObject<K>
> extends DatabaseAccountTypeService<K,V> {

    DatabaseBankAccountingService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        super(connector, keyClass, valueClass);
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected List<V> getListMaster(DatabaseConnection db) throws SQLException, RemoteException {
        if(connector.factory.getRootConnector().getMasterUsers().get(connector.getSwitchUser()).getCanAccessBankAccount()) {
            return getListBankAccounting(db);
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected List<V> getListDaemon(DatabaseConnection db) {
        return Collections.emptyList();
    }

    /**
     * @see  #getPublicList(DatabaseConnection)
     */
    @Override
    final protected List<V> getListBusiness(DatabaseConnection db) {
        return Collections.emptyList();
    }

    /**
     * Called only for those who are master users with bank accounting.
     */
    abstract protected List<V> getListBankAccounting(DatabaseConnection db) throws SQLException;
}
