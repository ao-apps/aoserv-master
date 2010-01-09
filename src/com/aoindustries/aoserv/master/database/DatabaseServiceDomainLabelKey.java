package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObjectDomainLabelKey;
import com.aoindustries.aoserv.client.AOServServiceDomainLabelKey;
import com.aoindustries.aoserv.client.validator.DomainLabel;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServiceDomainLabelKey<V extends AOServObjectDomainLabelKey<V>> extends DatabaseService<DomainLabel,V> implements AOServServiceDomainLabelKey<DatabaseConnector,DatabaseConnectorFactory,V> {

    DatabaseServiceDomainLabelKey(DatabaseConnector connector, Class<V> clazz) {
        super(connector, DomainLabel.class, clazz);
    }
}
