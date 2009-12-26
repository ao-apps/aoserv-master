package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.TechnologyVersion;
import com.aoindustries.aoserv.client.TechnologyVersionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyVersionService extends DatabaseServiceIntegerKey<TechnologyVersion> implements TechnologyVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TechnologyVersion> objectFactory = new AutoObjectFactory<TechnologyVersion>(TechnologyVersion.class, this);

    DatabaseTechnologyVersionService(DatabaseConnector connector) {
        super(connector, TechnologyVersion.class);
    }

    protected Set<TechnologyVersion> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technology_versions"
        );
    }

    protected Set<TechnologyVersion> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  updated,\n"
            + "  ?,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions",
            AOServObject.FILTERED
        );
    }

    protected Set<TechnologyVersion> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  updated,\n"
            + "  ?,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions",
            AOServObject.FILTERED
        );
    }
}
