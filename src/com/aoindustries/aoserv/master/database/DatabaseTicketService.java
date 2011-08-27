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
final class DatabaseTicketService extends DatabaseAccountTypeService<Integer,Ticket> implements TicketService {

    private final ObjectFactory<Ticket> objectFactory = new ObjectFactory<Ticket>() {
        @Override
        public Ticket createObject(ResultSet result) throws SQLException {
            try {
                return new Ticket(
                    connector,
                    result.getInt("ticket_id"),
                    AccountingCode.valueOf(result.getString("brand")),
                    AccountingCode.valueOf(result.getString("reseller")),
                    AccountingCode.valueOf(result.getString("accounting")),
                    result.getString("language"),
                    UserId.valueOf(result.getString("created_by")),
                    (Integer)result.getObject("category"),
                    result.getString("ticket_type"),
                    Email.valueOf(result.getString("from_address")),
                    result.getString("summary"),
                    result.getLong("open_date"),
                    result.getString("client_priority"),
                    result.getString("admin_priority"),
                    result.getString("status"),
                    (Long)result.getObject("status_timeout"),
                    result.getString("contact_emails"),
                    result.getString("contact_phone_numbers")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseTicketService(DatabaseConnector connector) {
        super(connector, Integer.class, Ticket.class);
    }

    @Override
    protected ArrayList<Ticket> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Ticket>(),
            objectFactory,
            "select\n"
            + "  ticket_id,\n"
            + "  brand,\n"
            + "  reseller,\n"
            + "  accounting,\n"
            + "  language,\n"
            + "  created_by,\n"
            + "  category,\n"
            + "  ticket_type,\n"
            + "  from_address,\n"
            + "  summary,\n"
            + "  (extract(epoch from open_date)*1000)::int8 as open_date,\n"
            + "  client_priority,\n"
            + "  admin_priority,\n"
            + "  status,\n"
            + "  (extract(epoch from status_timeout)*1000)::int8 as status_timeout,\n"
            + "  contact_emails,\n"
            + "  contact_phone_numbers\n"
            + "from\n"
            + "  tickets"
        );
    }

    @Override
    protected ArrayList<Ticket> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Ticket>(),
            objectFactory,
            "select\n"
            + "  ti.ticket_id,\n"
            + "  ti.brand,\n"
            + "  ti.reseller,\n"
            + "  ti.accounting,\n"
            + "  ti.language,\n"
            + "  ti.created_by,\n"
            + "  ti.category,\n"
            + "  ti.ticket_type,\n"
            + "  ti.from_address,\n"
            + "  ti.summary,\n"
            + "  (extract(epoch from ti.open_date)*1000)::int8 as open_date,\n"
            + "  ti.client_priority,\n"
            + "  ti.admin_priority,\n"
            + "  ti.status,\n"
            + "  (extract(epoch from ti.status_timeout)*1000)::int8 as status_timeout,\n"
            + "  ti.contact_emails,\n"
            + "  ti.contact_phone_numbers\n"
            + "from\n"
            + "  usernames un,\n"
            + "  tickets ti\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=ti.brand\n"
            + "  and un.accounting=ti.accounting\n"
            + "  and ti.status in (?,?,?)\n"
            + "  and ti.ticket_type=?",
            connector.getSwitchUser(),
            TicketStatus.OPEN,
            TicketStatus.HOLD,
            TicketStatus.BOUNCED,
            TicketType.LOGS
        );
    }

    @Override
    protected ArrayList<Ticket> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.isTicketAdmin()) {
            return db.executeObjectCollectionQuery(
                new ArrayList<Ticket>(),
                objectFactory,
                "select distinct\n"
                + "  ti.ticket_id,\n"
                + "  ti.brand,\n"
                + "  ti.reseller,\n"
                + "  ti.accounting,\n"
                + "  ti.language,\n"
                + "  ti.created_by,\n"
                + "  ti.category,\n"
                + "  ti.ticket_type,\n"
                + "  ti.from_address,\n"
                + "  ti.summary,\n"
                + "  (extract(epoch from ti.open_date)*1000)::int8 as open_date,\n"
                + "  ti.client_priority,\n"
                + "  ti.admin_priority,\n"
                + "  ti.status,\n"
                + "  (extract(epoch from ti.status_timeout)*1000)::int8 as status_timeout,\n"
                + "  ti.contact_emails,\n"
                + "  ti.contact_phone_numbers\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  tickets ti\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and (\n"
                + "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
                + "    or bu1.accounting=ti.brand\n" // Has access to brand
                + "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
                + "  )",
                connector.getSwitchUser()
            );
        } else {
            return db.executeObjectCollectionQuery(
                new ArrayList<Ticket>(),
                objectFactory,
                "select\n"
                + "  ti.ticket_id,\n"
                + "  ti.brand,\n"
                + "  null,\n" // reseller
                + "  ti.accounting,\n"
                + "  ti.language,\n"
                + "  ti.created_by,\n"
                + "  ti.category,\n"
                + "  ti.ticket_type,\n"
                + "  ti.from_address,\n"
                + "  ti.summary,\n"
                + "  (extract(epoch from ti.open_date)*1000)::int8 as open_date,\n"
                + "  ti.client_priority,\n"
                + "  null,\n" // admin_priority
                + "  ti.status,\n"
                + "  (extract(epoch from ti.status_timeout)*1000)::int8 as status_timeout,\n"
                + "  ti.contact_emails,\n"
                + "  ti.contact_phone_numbers\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  tickets ti\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=ti.accounting\n"
                + "  and ti.status not in ('junk', 'deleted')",
                connector.getSwitchUser()
            );
        }
    }
}
