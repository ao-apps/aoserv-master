package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectAccountingCodeKey;
import com.aoindustries.aoserv.client.AOServServiceAccountingCodeKey;
import com.aoindustries.aoserv.client.validator.AccountingCode;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceAccountingCodeKey<V extends AOServObjectAccountingCodeKey<V>> extends DatabaseService<AccountingCode,V> implements AOServServiceAccountingCodeKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceAccountingCodeKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, AccountingCode.class, clazz);
    }
}
