package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.OperatingSystemVersionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import com.aoindustries.util.HashCodeComparator;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemVersionService extends DatabasePublicService<Integer,OperatingSystemVersion> implements OperatingSystemVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<OperatingSystemVersion> objectFactory = new AutoObjectFactory<OperatingSystemVersion>(OperatingSystemVersion.class, this);

    DatabaseOperatingSystemVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, OperatingSystemVersion.class);
    }

    @Override
    protected Set<OperatingSystemVersion> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<OperatingSystemVersion>(HashCodeComparator.getInstance()),
            objectFactory,
            "select * from operating_system_versions order by pkey"
        );
    }
}
