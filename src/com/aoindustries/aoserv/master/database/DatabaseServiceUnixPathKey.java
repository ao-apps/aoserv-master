package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectUnixPathKey;
import com.aoindustries.aoserv.client.AOServServiceUnixPathKey;
import com.aoindustries.aoserv.client.validator.UnixPath;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceUnixPathKey<V extends AOServObjectUnixPathKey<V>> extends DatabaseService<UnixPath,V> implements AOServServiceUnixPathKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceUnixPathKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, UnixPath.class, clazz);
    }
}
