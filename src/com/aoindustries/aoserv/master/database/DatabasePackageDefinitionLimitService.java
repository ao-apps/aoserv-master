/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageDefinitionLimitService extends DatabaseAccountTypeService<Integer,PackageDefinitionLimit> implements PackageDefinitionLimitService {

    private final ObjectFactory<PackageDefinitionLimit> objectFactory = new ObjectFactory<PackageDefinitionLimit>() {
        @Override
        public PackageDefinitionLimit createObject(ResultSet result) throws SQLException {
            return new PackageDefinitionLimit(
                connector,
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
    protected ArrayList<PackageDefinitionLimit> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PackageDefinitionLimit>(),
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
            + "  inner join package_definitions pd on pdl.package_definition=pd.pkey"
        );
    }

    @Override
    protected ArrayList<PackageDefinitionLimit> getListDaemon(DatabaseConnection db) {
        return new ArrayList<PackageDefinitionLimit>(0);
        /*
        return db.executeObjectCollectionQuery(
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
    protected ArrayList<PackageDefinitionLimit> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).getUsername().getBusiness().getCanSeePrices()) {
            return db.executeObjectCollectionQuery(
                new ArrayList<PackageDefinitionLimit>(),
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
                + "  )",
                connector.getConnectAs()
            );
        } else {
            return db.executeObjectCollectionQuery(
                new ArrayList<PackageDefinitionLimit>(),
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
                + "  )",
                connector.getConnectAs()
            );
        }
    }
}
