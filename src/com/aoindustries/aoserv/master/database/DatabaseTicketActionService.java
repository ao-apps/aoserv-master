package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketAction;
import com.aoindustries.aoserv.client.TicketActionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketActionService extends DatabaseService<Integer,TicketAction> implements TicketActionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketAction> objectFactory = new AutoObjectFactory<TicketAction>(TicketAction.class, this);

    DatabaseTicketActionService(DatabaseConnector connector) {
        super(connector, Integer.class, TicketAction.class);
    }

    protected Set<TicketAction> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  ticket,\n"
            + "  administrator,\n"
            + "  time,\n"
            + "  action_type,\n"
            + "  old_accounting,\n"
            + "  new_accounting,\n"
            + "  old_priority,\n"
            + "  new_priority,\n"
            + "  old_type,\n"
            + "  new_type,\n"
            + "  old_status,\n"
            + "  new_status,\n"
            + "  old_assigned_to,\n"
            + "  new_assigned_to,\n"
            + "  old_category,\n"
            + "  new_category,\n"
            + "  from_address,\n"
            + "  summary\n"
            + "from\n"
            + "  ticket_actions"
        );
    }

    protected Set<TicketAction> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    protected Set<TicketAction> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).isTicketAdmin()) {
            // If a ticket admin, can see all ticket_actions
            return db.executeObjectSetQuery(
                objectFactory,
                "select\n"
                + "  ta.pkey,\n"
                + "  ta.ticket,\n"
                + "  ta.administrator,\n"
                + "  ta.time,\n"
                + "  ta.action_type,\n"
                + "  ta.old_accounting,\n"
                + "  ta.new_accounting,\n"
                + "  ta.old_priority,\n"
                + "  ta.new_priority,\n"
                + "  ta.old_type,\n"
                + "  ta.new_type,\n"
                + "  ta.old_status,\n"
                + "  ta.new_status,\n"
                + "  ta.old_assigned_to,\n"
                + "  ta.new_assigned_to,\n"
                + "  ta.old_category,\n"
                + "  ta.new_category,\n"
                + "  ta.from_address,\n"
                + "  ta.summary\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  tickets ti,\n"
                + "  ticket_actions ta\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and (\n"
                + "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
                + "    or bu1.accounting=ti.brand\n" // Has access to brand
                + "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
                + "  )\n"
                + "  and ti.ticket_id=ta.ticket",
                connector.getConnectAs()
            );
        } else {
            // Can only see non-admin types and statuses
            return db.executeObjectSetQuery(
                objectFactory,
                "select\n"
                + "  ta.pkey,\n"
                + "  ta.ticket,\n"
                + "  ta.administrator,\n"
                + "  ta.time,\n"
                + "  ta.action_type,\n"
                + "  ta.old_accounting,\n"
                + "  ta.new_accounting,\n"
                + "  ta.old_priority,\n"
                + "  ta.new_priority,\n"
                + "  ta.old_type,\n"
                + "  ta.new_type,\n"
                + "  ta.old_status,\n"
                + "  ta.new_status,\n"
                + "  ta.old_assigned_to,\n"
                + "  ta.new_assigned_to,\n"
                + "  ta.old_category,\n"
                + "  ta.new_category,\n"
                + "  ta.from_address,\n"
                + "  ta.summary\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  tickets ti,\n"
                + "  ticket_actions ta,\n"
                + "  ticket_action_types tat\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=ti.accounting\n"
                + "  and ti.status not in ('junk', 'deleted')\n"
                + "  and ti.ticket_id=ta.ticket\n"
                + "  and ta.action_type=tat.type\n"
                + "  and not tat.visible_admin_only",
                connector.getConnectAs()
            );
        }
    }
}
