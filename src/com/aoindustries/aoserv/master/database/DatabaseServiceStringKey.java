package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectStringKey;
import com.aoindustries.aoserv.client.AOServServiceStringKey;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceStringKey<V extends AOServObjectStringKey<V>> extends DatabaseService<String,V> implements AOServServiceStringKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceStringKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, String.class, clazz);
    }
}
