package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorService;
import com.aoindustries.aoserv.client.validator.HashedPassword;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessAdministratorService extends DatabaseService<UserId,BusinessAdministrator> implements BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessAdministrator> objectFactory = new AutoObjectFactory<BusinessAdministrator>(BusinessAdministrator.class, this);

    DatabaseBusinessAdministratorService(DatabaseConnector connector) {
        super(connector, UserId.class, BusinessAdministrator.class);
    }

    protected Set<BusinessAdministrator> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  username,\n"
            + "  password,\n"
            + "  full_name,\n"
            + "  title,\n"
            + "  birthday,\n"
            + "  is_preferred,\n"
            + "  private,\n"
            + "  created,\n"
            + "  work_phone,\n"
            + "  home_phone,\n"
            + "  cell_phone,\n"
            + "  fax,\n"
            + "  email,\n"
            + "  address1,\n"
            + "  address2,\n"
            + "  city,\n"
            + "  state,\n"
            + "  country,\n"
            + "  zip,\n"
            + "  disable_log,\n"
            + "  can_switch_users,\n"
            + "  support_code\n"
            + "from\n"
            + "  business_administrators"
        );
    }

    protected Set<BusinessAdministrator> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  ba.username,\n"
            + "  '"+HashedPassword.NO_PASSWORD+"'::text,\n"
            + "  ba.full_name,\n"
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
            + "  business_administrators ba\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=ba.accounting",
            connector.getConnectAs()
        );
    }

    protected Set<BusinessAdministrator> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ba.username,\n"
            + "  '"+HashedPassword.NO_PASSWORD+"'::text,\n"
            + "  ba.full_name,\n"
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
            + "  business_administrators ba\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    ba.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=ba.accounting",
            connector.getConnectAs()
        );
    }
}
