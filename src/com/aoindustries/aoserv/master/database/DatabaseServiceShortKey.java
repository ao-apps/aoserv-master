package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectShortKey;
import com.aoindustries.aoserv.client.AOServServiceShortKey;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceShortKey<V extends AOServObjectShortKey<V>> extends DatabaseService<Short,V> implements AOServServiceShortKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceShortKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, Short.class, clazz);
    }
}
