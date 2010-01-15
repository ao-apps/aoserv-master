package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.NetDeviceID;
import com.aoindustries.aoserv.client.NetDeviceIDService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetDeviceIDService extends DatabaseServiceStringKey<NetDeviceID> implements NetDeviceIDService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetDeviceID> objectFactory = new AutoObjectFactory<NetDeviceID>(NetDeviceID.class, this);

    DatabaseNetDeviceIDService(DatabaseConnector connector) {
        super(connector, NetDeviceID.class);
    }

    protected Set<NetDeviceID> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from net_device_ids"
        );
    }

    protected Set<NetDeviceID> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from net_device_ids"
        );
    }

    protected Set<NetDeviceID> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from net_device_ids"
        );
    }
}
