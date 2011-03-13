/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyVersionService extends DatabaseService<Integer,TechnologyVersion> implements TechnologyVersionService {

    private final ObjectFactory<TechnologyVersion> objectFactory = new ObjectFactory<TechnologyVersion>() {
        @Override
        public TechnologyVersion createObject(ResultSet result) throws SQLException {
            try {
                return new TechnologyVersion(
                    connector,
                    result.getInt("pkey"),
                    result.getString("name"),
                    result.getString("version"),
                    result.getLong("updated"),
                    getUserId(result.getString("owner")),
                    result.getInt("operating_system_version")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseTechnologyVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, TechnologyVersion.class);
    }

    @Override
    protected ArrayList<TechnologyVersion> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TechnologyVersion>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  (extract(epoch from updated)*1000)::int8 as updated,\n"
            + "  owner,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions"
        );
    }

    @Override
    protected ArrayList<TechnologyVersion> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TechnologyVersion>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  (extract(epoch from updated)*1000)::int8 as updated,\n"
            + "  null as owner,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions"
        );
    }

    @Override
    protected ArrayList<TechnologyVersion> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TechnologyVersion>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  name,\n"
            + "  version,\n"
            + "  (extract(epoch from updated)*1000)::int8 as updated,\n"
            + "  null as owner,\n"
            + "  operating_system_version\n"
            + "from\n"
            + "  technology_versions"
        );
    }
}
