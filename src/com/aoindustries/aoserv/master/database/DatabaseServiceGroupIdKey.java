package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectGroupIdKey;
import com.aoindustries.aoserv.client.AOServServiceGroupIdKey;
import com.aoindustries.aoserv.client.validator.GroupId;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceGroupIdKey<V extends AOServObjectGroupIdKey<V>> extends DatabaseService<GroupId,V> implements AOServServiceGroupIdKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceGroupIdKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, GroupId.class, clazz);
    }
}
