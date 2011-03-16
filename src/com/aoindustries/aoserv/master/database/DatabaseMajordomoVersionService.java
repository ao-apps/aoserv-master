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

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMajordomoVersionService extends DatabaseService<String,MajordomoVersion> implements MajordomoVersionService {

    private final ObjectFactory<MajordomoVersion> objectFactory = new AutoObjectFactory<MajordomoVersion>(MajordomoVersion.class, connector);

    DatabaseMajordomoVersionService(DatabaseConnector connector) {
        super(connector, String.class, MajordomoVersion.class);
    }

    @Override
    protected ArrayList<MajordomoVersion> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MajordomoVersion>(),
            objectFactory,
            "select\n"
            + "  version,\n"
            + "  (extract(epoch from created)*1000)::int8 as created\n"
            + "from\n"
            + "  majordomo_versions"
        );
    }
}
