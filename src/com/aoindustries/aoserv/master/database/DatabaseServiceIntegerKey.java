package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectIntegerKey;
import com.aoindustries.aoserv.client.AOServServiceIntegerKey;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceIntegerKey<V extends AOServObjectIntegerKey<V>> extends DatabaseService<Integer,V> implements AOServServiceIntegerKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceIntegerKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, Integer.class, clazz);
    }
}
