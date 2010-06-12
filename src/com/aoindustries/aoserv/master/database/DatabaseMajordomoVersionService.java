package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MajordomoVersion;
import com.aoindustries.aoserv.client.MajordomoVersionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMajordomoVersionService extends DatabasePublicService<String,MajordomoVersion> implements MajordomoVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MajordomoVersion> objectFactory = new AutoObjectFactory<MajordomoVersion>(MajordomoVersion.class, this);

    DatabaseMajordomoVersionService(DatabaseConnector connector) {
        super(connector, String.class, MajordomoVersion.class);
    }

    protected Set<MajordomoVersion> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from majordomo_versions"
        );
    }
}
