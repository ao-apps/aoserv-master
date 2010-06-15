package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketStatus;
import com.aoindustries.aoserv.client.TicketStatusService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketStatusService extends DatabasePublicService<String,TicketStatus> implements TicketStatusService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketStatus> objectFactory = new AutoObjectFactory<TicketStatus>(TicketStatus.class, this);

    DatabaseTicketStatusService(DatabaseConnector connector) {
        super(connector, String.class, TicketStatus.class);
    }

    protected Set<TicketStatus> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_statuses"
        );
    }
}
