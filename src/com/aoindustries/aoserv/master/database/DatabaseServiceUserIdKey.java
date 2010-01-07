package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectUserIdKey;
import com.aoindustries.aoserv.client.AOServServiceUserIdKey;
import com.aoindustries.aoserv.client.validator.UserId;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceUserIdKey<V extends AOServObjectUserIdKey<V>> extends DatabaseService<UserId,V> implements AOServServiceUserIdKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceUserIdKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, UserId.class, clazz);
    }
}
