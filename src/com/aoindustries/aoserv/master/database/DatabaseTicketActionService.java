/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketActionService extends DatabaseAccountTypeService<Integer,TicketAction> implements TicketActionService {

    private final ObjectFactory<TicketAction> objectFactory = new ObjectFactory<TicketAction>() {
        @Override
        public TicketAction createObject(ResultSet result) throws SQLException {
            try {
                return new TicketAction(
                    connector,
                    result.getInt("pkey"),
                    result.getInt("ticket"),
                    UserId.valueOf(result.getString("administrator")),
                    result.getLong("time"),
                    result.getString("action_type"),
                    AccountingCode.valueOf(result.getString("old_accounting")),
                    AccountingCode.valueOf(result.getString("new_accounting")),
                    result.getString("old_priority"),
                    result.getString("new_priority"),
                    result.getString("old_type"),
                    result.getString("new_type"),
                    result.getString("old_status"),
                    result.getString("new_status"),
                    UserId.valueOf(result.getString("old_assigned_to")),
                    UserId.valueOf(result.getString("new_assigned_to")),
                    (Integer)result.getObject("old_category"),
                    (Integer)result.getObject("new_category"),
                    Email.valueOf(result.getString("from_address")),
                    result.getString("summary")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseTicketActionService(DatabaseConnector connector) {
        super(connector, Integer.class, TicketAction.class);
    }

    @Override
    protected ArrayList<TicketAction> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketAction>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  ticket,\n"
            + "  administrator,\n"
            + "  (extract(epoch from time)*1000)::int8 as time,\n"
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

    @Override
    protected ArrayList<TicketAction> getListDaemon(DatabaseConnection db) {
        return new ArrayList<TicketAction>(0);
    }

    @Override
    protected ArrayList<TicketAction> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.isTicketAdmin()) {
            // If a ticket admin, can see all ticket_actions
            return db.executeObjectCollectionQuery(
                new ArrayList<TicketAction>(),
                objectFactory,
                "select\n"
                + "  ta.pkey,\n"
                + "  ta.ticket,\n"
                + "  ta.administrator,\n"
                + "  (extract(epoch from ta.time)*1000)::int8 as time,\n"
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
                connector.getSwitchUser()
            );
        } else {
            // Can only see non-admin types and statuses
            return db.executeObjectCollectionQuery(
                new ArrayList<TicketAction>(),
                objectFactory,
                "select\n"
                + "  ta.pkey,\n"
                + "  ta.ticket,\n"
                + "  ta.administrator,\n"
                + "  (extract(epoch from ta.time)*1000)::int8 as time,\n"
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
                connector.getSwitchUser()
            );
        }
    }
}
