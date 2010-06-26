/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.NetDeviceID;
import com.aoindustries.aoserv.client.NetDeviceIDService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetDeviceIDService extends DatabasePublicService<String,NetDeviceID> implements NetDeviceIDService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetDeviceID> objectFactory = new AutoObjectFactory<NetDeviceID>(NetDeviceID.class, this);

    DatabaseNetDeviceIDService(DatabaseConnector connector) {
        super(connector, String.class, NetDeviceID.class);
    }

    @Override
    protected Set<NetDeviceID> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<NetDeviceID>(),
            objectFactory,
            "select * from net_device_ids"
        );
    }
}
