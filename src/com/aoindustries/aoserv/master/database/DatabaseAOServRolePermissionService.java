package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServRolePermission;
import com.aoindustries.aoserv.client.AOServRolePermissionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServRolePermissionService extends DatabaseService<Integer,AOServRolePermission> implements AOServRolePermissionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServRolePermission> objectFactory = new AutoObjectFactory<AOServRolePermission>(AOServRolePermission.class, this);

    DatabaseAOServRolePermissionService(DatabaseConnector connector) {
        super(connector, Integer.class, AOServRolePermission.class);
    }

    protected Set<AOServRolePermission> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from aoserv_role_permissions"
        );
    }

    /**
     * Can only see their own roles.
     */
    protected Set<AOServRolePermission> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  arp.*\n"
            + "from\n"
            + "  business_administrator_roles bar\n"
            + "  inner join aoserv_role_permissions arp on bar.role=arp.role\n"
            + "where\n"
            + "  bar.username=?",
            connector.getConnectAs()
        );
    }

    /**
     * Can see the roles owned by the business tree and their own roles.
     */
    protected Set<AOServRolePermission> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            // Business-based
            "select\n"
            + "  arp.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  aoserv_roles ar\n"
            + "  inner join aoserv_role_permissions arp on ar.pkey=arp.role\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=ar.accounting\n"
            // Individual role-based
            + "union select\n"
            + "  arp.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  business_administrators ba,\n"
            + "  business_administrator_roles bar,\n"
            + "  aoserv_role_permissions arp\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    ba.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=ba.accounting\n"
            + "  and ba.username=bar.username\n"
            + "  and bar.role=arp.role",
            connector.getConnectAs(),
            connector.getConnectAs()
        );
    }
}
