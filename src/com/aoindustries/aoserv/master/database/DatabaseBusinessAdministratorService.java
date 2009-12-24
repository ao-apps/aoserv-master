package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessAdministratorService extends DatabaseServiceStringKey<BusinessAdministrator> implements BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessAdministrator> objectFactory = new AutoObjectFactory<BusinessAdministrator>(BusinessAdministrator.class, this);

    DatabaseBusinessAdministratorService(DatabaseConnector connector) {
        super(connector, BusinessAdministrator.class);
    }

    protected Set<BusinessAdministrator> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from business_administrators"
        );
    }

    protected Set<BusinessAdministrator> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  ba.username,\n"
            + "  '"+BusinessAdministrator.NO_PASSWORD+"'::text,\n"
            + "  ba.name,\n"
            + "  ba.title,\n"
            + "  ba.birthday,\n"
            + "  ba.is_preferred,\n"
            + "  ba.private,\n"
            + "  ba.created,\n"
            + "  ba.work_phone,\n"
            + "  ba.home_phone,\n"
            + "  ba.cell_phone,\n"
            + "  ba.fax,\n"
            + "  ba.email,\n"
            + "  ba.address1,\n"
            + "  ba.address2,\n"
            + "  ba.city,\n"
            + "  ba.state,\n"
            + "  ba.country,\n"
            + "  ba.zip,\n"
            + "  ba.disable_log,\n"
            + "  ba.can_switch_users,\n"
            + "  null\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  usernames un,\n"
            + "  business_administrators ba\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=un.accounting\n"
            + "  and un.username=ba.username",
            connector.getConnectAs()
        );
    }

    protected Set<BusinessAdministrator> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ba.username,\n"
            + "  '"+BusinessAdministrator.NO_PASSWORD+"'::text,\n"
            + "  ba.name,\n"
            + "  ba.title,\n"
            + "  ba.birthday,\n"
            + "  ba.is_preferred,\n"
            + "  ba.private,\n"
            + "  ba.created,\n"
            + "  ba.work_phone,\n"
            + "  ba.home_phone,\n"
            + "  ba.cell_phone,\n"
            + "  ba.fax,\n"
            + "  ba.email,\n"
            + "  ba.address1,\n"
            + "  ba.address2,\n"
            + "  ba.city,\n"
            + "  ba.state,\n"
            + "  ba.country,\n"
            + "  ba.zip,\n"
            + "  ba.disable_log,\n"
            + "  ba.can_switch_users,\n"
            + "  ba.support_code\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  usernames un2,\n"
            + "  business_administrators ba\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    un2.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=un2.accounting\n"
            + "  and un2.username=ba.username",
            connector.getConnectAs()
        );
    }
}
