/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketAssignmentService extends DatabaseService<Integer,TicketAssignment> implements TicketAssignmentService {

    private final ObjectFactory<TicketAssignment> objectFactory = new AutoObjectFactory<TicketAssignment>(TicketAssignment.class, connector);

    DatabaseTicketAssignmentService(DatabaseConnector connector) {
        super(connector, Integer.class, TicketAssignment.class);
    }

    @Override
    protected ArrayList<TicketAssignment> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketAssignment>(),
            objectFactory,
            "select pkey, ticket, reseller, administrator from ticket_assignments"
        );
    }

    /**
     * Daemons do not get any ticket assignment data.
     */
    @Override
    protected ArrayList<TicketAssignment> getListDaemon(DatabaseConnection db) {
        return new ArrayList<TicketAssignment>(0);
    }

    @Override
    protected ArrayList<TicketAssignment> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).isTicketAdmin()) {
            // Only ticket admin can see assignments
            return db.executeObjectCollectionQuery(
                new ArrayList<TicketAssignment>(),
                objectFactory,
                "select\n"
                + "  ta.pkey,\n"
                + "  ta.ticket,\n"
                + "  ta.reseller,\n"
                + "  ta.administrator\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  tickets ti,\n"
                + "  ticket_assignments ta\n"
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
                + "  and ti.pkey=ta.ticket",
                connector.getConnectAs()
            );
        } else {
            // Non-admins don't get any assignment details
            return new ArrayList<TicketAssignment>(0);
        }
    }
}
