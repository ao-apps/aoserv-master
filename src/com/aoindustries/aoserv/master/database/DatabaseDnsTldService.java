package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.DnsTld;
import com.aoindustries.aoserv.client.DnsTldService;
import com.aoindustries.aoserv.client.validator.DomainName;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsTldService extends DatabasePublicService<DomainName,DnsTld> implements DnsTldService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DnsTld> objectFactory = new AutoObjectFactory<DnsTld>(DnsTld.class, this);

    DatabaseDnsTldService(DatabaseConnector connector) {
        super(connector, DomainName.class, DnsTld.class);
    }

    protected Set<DnsTld> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from dns_tlds"
        );
    }
}
