/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsTldService extends DatabaseService<DomainName,DnsTld> implements DnsTldService {

    private final ObjectFactory<DnsTld> objectFactory = new AutoObjectFactory<DnsTld>(DnsTld.class, connector);

    DatabaseDnsTldService(DatabaseConnector connector) {
        super(connector, DomainName.class, DnsTld.class);
    }

    @Override
    protected List<DnsTld> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DnsTld>(),
            objectFactory,
            "select * from dns_tlds"
        );
    }
}
