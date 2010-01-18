package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.DnsType;
import com.aoindustries.aoserv.client.DnsTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsTypeService extends DatabasePublicService<String,DnsType> implements DnsTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DnsType> objectFactory = new AutoObjectFactory<DnsType>(DnsType.class, this);

    DatabaseDnsTypeService(DatabaseConnector connector) {
        super(connector, String.class, DnsType.class);
    }

    protected Set<DnsType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from dns_types"
        );
    }
}
