package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Ticket;
import com.aoindustries.aoserv.client.TicketService;
import com.aoindustries.aoserv.client.TicketStatus;
import com.aoindustries.aoserv.client.TicketType;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketService extends DatabaseServiceIntegerKey<Ticket> implements TicketService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Ticket> objectFactory = new AutoObjectFactory<Ticket>(Ticket.class, this);

    DatabaseTicketService(DatabaseConnector connector) {
        super(connector, Ticket.class);
    }

    protected Set<Ticket> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
            + "  open_date,\n"
            + "  client_priority,\n"
            + "  admin_priority,\n"
            + "  status,\n"
            + "  status_timeout,\n"
            + "  contact_emails,\n"
            + "  contact_phone_numbers\n"
            + "from\n"
            + "  tickets"
        );
    }

    protected Set<Ticket> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
            + "  ti.open_date,\n"
            + "  ti.client_priority,\n"
            + "  ti.admin_priority,\n"
            + "  ti.status,\n"
            + "  ti.status_timeout,\n"
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
            connector.getConnectAs(),
            TicketStatus.OPEN,
            TicketStatus.HOLD,
            TicketStatus.BOUNCED,
            TicketType.LOGS
        );
    }

    protected Set<Ticket> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).isTicketAdmin()) {
            return db.executeObjectSetQuery(
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
                + "  ti.open_date,\n"
                + "  ti.client_priority,\n"
                + "  ti.admin_priority,\n"
                + "  ti.status,\n"
                + "  ti.status_timeout,\n"
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
                connector.getConnectAs()
            );
        } else {
            return db.executeObjectSetQuery(
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
                + "  ti.open_date,\n"
                + "  ti.client_priority,\n"
                + "  null,\n" // admin_priority
                + "  ti.status,\n"
                + "  ti.status_timeout,\n"
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
                connector.getConnectAs()
            );
        }
    }
}
