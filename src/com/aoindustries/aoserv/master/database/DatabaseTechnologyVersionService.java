package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TechnologyVersion;
import com.aoindustries.aoserv.client.TechnologyVersionService;
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
final class DatabaseTechnologyVersionService extends DatabaseService<Integer,TechnologyVersion> implements TechnologyVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TechnologyVersion> objectFactory = new AutoObjectFactory<TechnologyVersion>(TechnologyVersion.class, this);

    DatabaseTechnologyVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, TechnologyVersion.class);
    }

    @Override
    protected Set<TechnologyVersion> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<TechnologyVersion>(HashCodeComparator.getInstance()),
            objectFactory,
            "select * from technology_versions order by pkey"
        );
    }

    @Override
    protected Set<TechnologyVersion> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<TechnologyVersion>(HashCodeComparator.getInstance()),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  updated,\n"
            + "  null,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions\n"
            + "order by\n"
            + "  pkey"
        );
    }

    @Override
    protected Set<TechnologyVersion> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<TechnologyVersion>(HashCodeComparator.getInstance()),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  updated,\n"
            + "  null,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions\n"
            + "order by\n"
            + "  pkey"
        );
    }
}
