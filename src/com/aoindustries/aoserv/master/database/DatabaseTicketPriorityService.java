package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketPriority;
import com.aoindustries.aoserv.client.TicketPriorityService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketPriorityService extends DatabaseServiceStringKey<TicketPriority> implements TicketPriorityService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketPriority> objectFactory = new AutoObjectFactory<TicketPriority>(TicketPriority.class, this);

    DatabaseTicketPriorityService(DatabaseConnector connector) {
        super(connector, TicketPriority.class);
    }

    protected Set<TicketPriority> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_priorities"
        );
    }

    protected Set<TicketPriority> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_priorities"
        );
    }

    protected Set<TicketPriority> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_priorities"
        );
    }
}
