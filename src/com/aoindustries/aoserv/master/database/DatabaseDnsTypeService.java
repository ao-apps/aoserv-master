/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsTypeService extends DatabaseService<String,DnsType> implements DnsTypeService {

    private final ObjectFactory<DnsType> objectFactory = new AutoObjectFactory<DnsType>(DnsType.class, connector);

    DatabaseDnsTypeService(DatabaseConnector connector) {
        super(connector, String.class, DnsType.class);
    }

    @Override
    protected List<DnsType> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DnsType>(),
            objectFactory,
            "select * from dns_types"
        );
    }
}
