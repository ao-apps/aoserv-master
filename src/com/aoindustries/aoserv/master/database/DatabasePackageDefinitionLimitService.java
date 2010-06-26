/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.PackageDefinitionLimit;
import com.aoindustries.aoserv.client.PackageDefinitionLimitService;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageDefinitionLimitService extends DatabaseService<Integer,PackageDefinitionLimit> implements PackageDefinitionLimitService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageDefinitionLimit> objectFactory = new ObjectFactory<PackageDefinitionLimit>() {
        @Override
        public PackageDefinitionLimit createObject(ResultSet result) throws SQLException {
            return new PackageDefinitionLimit(
                DatabasePackageDefinitionLimitService.this,
                result.getInt("pkey"),
                result.getInt("package_definition"),
                result.getString("resource_type"),
                (Integer)result.getObject("soft_limit"),
                (Integer)result.getObject("hard_limit"),
                getMoney(result, "currency", "additional_rate"),
                result.getString("additional_transaction_type")
            );
        }
    };

    DatabasePackageDefinitionLimitService(DatabaseConnector connector) {
        super(connector, Integer.class, PackageDefinitionLimit.class);
    }

    @Override
    protected Set<PackageDefinitionLimit> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<PackageDefinitionLimit>(),
            objectFactory,
            "select\n"
            + "  pdl.pkey,\n"
            + "  pdl.package_definition,\n"
            + "  pdl.resource_type,\n"
            + "  pdl.soft_limit,\n"
            + "  pdl.hard_limit,\n"
            + "  pd.currency,\n"
            + "  pdl.additional_rate,\n"
            + "  pdl.additional_transaction_type\n"
            + "from\n"
            + "  package_definition_limits pdl\n"
            + "  inner join package_definitions pd on pdl.package_definition=pd.pkey\n"
            + "order by\n"
            + "  pdl.pkey"
        );
    }

    @Override
    protected Set<PackageDefinitionLimit> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
        /*
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  pdl.pkey,\n"
            + "  pdl.package_definition,\n"
            + "  pdl.resource_type,\n"
            + "  pdl.soft_limit,\n"
            + "  pdl.hard_limit,\n"
            + "  pd.currency,\n"
            + "  pdl.additional_rate,\n"
            + "  pdl.additional_transaction_type\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  businesses bu,\n"
            + "  package_definition_limits pdl\n"
            + "  inner join package_definitions pd on pdl.package_definition=pd.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=bu.accounting\n"
            + "  and bu.package_definition=pdl.package_definition",
            connector.getConnectAs()
        );*/
    }

    @Override
    protected Set<PackageDefinitionLimit> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).getUsername().getBusiness().canSeePrices()) {
            return db.executeObjectSetQuery(
                new ArraySet<PackageDefinitionLimit>(),
                objectFactory,
                "select distinct\n"
                + "  pdl.pkey,\n"
                + "  pdl.package_definition,\n"
                + "  pdl.resource_type,\n"
                + "  pdl.soft_limit,\n"
                + "  pdl.hard_limit,\n"
                + "  pd.currency,\n"
                + "  pdl.additional_rate,\n"
                + "  pdl.additional_transaction_type\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  package_definition_businesses pdb,\n"
                + "  package_definitions pd\n"
                + "  inner join package_definition_limits pdl on pd.pkey=pdl.package_definition\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=pdb.accounting\n"
                + "  and (\n"
                + "    bu1.package_definition=pd.pkey\n"
                + "    or pdb.package_definition=pd.pkey\n"
                + "  )\n"
                + "order by\n"
                + "  pdl.pkey",
                connector.getConnectAs()
            );
        } else {
            return db.executeObjectSetQuery(
                new ArraySet<PackageDefinitionLimit>(),
                objectFactory,
                "select distinct\n"
                + "  pdl.pkey,\n"
                + "  pdl.package_definition,\n"
                + "  pdl.resource_type,\n"
                + "  pdl.soft_limit,\n"
                + "  pdl.hard_limit,\n"
                + "  pd.currency,\n"
                + "  null as additional_rate,\n"
                + "  null as additional_transaction_type\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  package_definition_businesses pdb,\n"
                + "  package_definitions pd\n"
                + "  inner join package_definition_limits pdl on pd.pkey=pdl.package_definition\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=pdb.accounting\n"
                + "  and (\n"
                + "    bu1.package_definition=pd.pkey\n"
                + "    or pdb.package_definition=pd.pkey\n"
                + "  )\n"
                + "order by\n"
                + "  pdl.pkey",
                connector.getConnectAs()
            );
        }
    }
}
