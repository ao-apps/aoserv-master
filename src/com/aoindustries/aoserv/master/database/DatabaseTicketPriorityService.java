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
final class DatabaseTicketPriorityService extends DatabasePublicService<String,TicketPriority> implements TicketPriorityService {

    private final ObjectFactory<TicketPriority> objectFactory = new AutoObjectFactory<TicketPriority>(TicketPriority.class, connector);

    DatabaseTicketPriorityService(DatabaseConnector connector) {
        super(connector, String.class, TicketPriority.class);
    }

    @Override
    protected ArrayList<TicketPriority> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketPriority>(),
            objectFactory,
            "select * from ticket_priorities"
        );
    }
}
