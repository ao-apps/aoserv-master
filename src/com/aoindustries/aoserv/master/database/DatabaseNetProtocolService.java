package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.NetProtocol;
import com.aoindustries.aoserv.client.NetProtocolService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetProtocolService extends DatabaseServiceStringKey<NetProtocol> implements NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetProtocol> objectFactory = new AutoObjectFactory<NetProtocol>(NetProtocol.class, this);

    DatabaseNetProtocolService(DatabaseConnector connector) {
        super(connector, NetProtocol.class);
    }

    protected Set<NetProtocol> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from net_protocols"
        );
    }

    protected Set<NetProtocol> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from net_protocols"
        );
    }

    protected Set<NetProtocol> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from net_protocols"
        );
    }
}
