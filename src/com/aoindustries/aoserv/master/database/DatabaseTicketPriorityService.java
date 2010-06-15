package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketPriority;
import com.aoindustries.aoserv.client.TicketPriorityService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketPriorityService extends DatabasePublicService<String,TicketPriority> implements TicketPriorityService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketPriority> objectFactory = new AutoObjectFactory<TicketPriority>(TicketPriority.class, this);

    DatabaseTicketPriorityService(DatabaseConnector connector) {
        super(connector, String.class, TicketPriority.class);
    }

    protected Set<TicketPriority> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_priorities"
        );
    }
}
