/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketStatusService extends DatabasePublicService<String,TicketStatus> implements TicketStatusService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketStatus> objectFactory = new AutoObjectFactory<TicketStatus>(TicketStatus.class, connector);

    DatabaseTicketStatusService(DatabaseConnector connector) {
        super(connector, String.class, TicketStatus.class);
    }

    @Override
    protected ArrayList<TicketStatus> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketStatus>(),
            objectFactory,
            "select * from ticket_statuses"
        );
    }
}
