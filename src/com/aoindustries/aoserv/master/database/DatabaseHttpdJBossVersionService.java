package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.HttpdJBossVersion;
import com.aoindustries.aoserv.client.HttpdJBossVersionService;
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
final class DatabaseHttpdJBossVersionService extends DatabasePublicService<Integer,HttpdJBossVersion> implements HttpdJBossVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdJBossVersion> objectFactory = new AutoObjectFactory<HttpdJBossVersion>(HttpdJBossVersion.class, this);

    DatabaseHttpdJBossVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdJBossVersion.class);
    }

    @Override
    protected Set<HttpdJBossVersion> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<HttpdJBossVersion>(HashCodeComparator.getInstance()),
            objectFactory,
            "select * from httpd_jboss_versions order by version"
        );
    }
}
