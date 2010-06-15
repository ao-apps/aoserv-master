/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.PackageDefinition;
import com.aoindustries.aoserv.client.PackageDefinitionService;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageDefinitionService extends DatabaseService<Integer,PackageDefinition> implements PackageDefinitionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageDefinition> objectFactory = new ObjectFactory<PackageDefinition>() {
        @Override
        public PackageDefinition createObject(ResultSet result) throws SQLException {
            return new PackageDefinition(
                DatabasePackageDefinitionService.this,
                result.getInt("pkey"),
                result.getString("category"),
                result.getString("name"),
                result.getString("version"),
                result.getString("display"),
                result.getString("description"),
                getMoney(result, "currency", "setup_fee"),
                result.getString("setup_fee_transaction_type"),
                getMoney(result, "currency", "monthly_rate"),
                result.getString("monthly_rate_transaction_type"),
                result.getBoolean("active"),
                result.getBoolean("approved")
            );
        }
    };

    DatabasePackageDefinitionService(DatabaseConnector connector) {
        super(connector, Integer.class, PackageDefinition.class);
    }

    @Override
    protected Set<PackageDefinition> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from package_definitions"
        );
    }

    @Override
    protected Set<PackageDefinition> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  pd.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  businesses bu,\n"
            + "  package_definitions pd\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=bu.accounting\n"
            + "  and bu.package_definition=pd.pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<PackageDefinition> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).getUsername().getBusiness().canSeePrices()) {
            return db.executeObjectSetQuery(
                objectFactory,
                "select distinct\n"
                + "  pd.*\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  package_definitions pd\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and (\n"
                + "    bu1.package_definition=pd.pkey\n"
                // + "    or bu1.accounting=pd.accounting\n"
                + "  )",
                connector.getConnectAs()
            );
        } else {
            return db.executeObjectSetQuery(
                objectFactory,
                "select distinct\n"
                + "  pd.pkey,\n"
                + "  pd.category,\n"
                + "  pd.name,\n"
                + "  pd.version,\n"
                + "  pd.display,\n"
                + "  pd.description,\n"
                + "  null as currency,\n"
                + "  null as setup_fee,\n"
                + "  null as setup_fee_transaction_type,\n"
                + "  null as monthly_rate,\n"
                + "  null as monthly_rate_transaction_type,\n"
                + "  pd.active,\n"
                + "  pd.approved\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  package_definitions pd\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and (\n"
                + "    bu1.package_definition=pd.pkey\n"
                // + "    or bu1.accounting=pd.accounting\n"
                + "  )",
                connector.getConnectAs()
            );
        }
    }
}
