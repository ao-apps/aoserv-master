/*
 * Copyright 2010 by AO Industries, Inc.,
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
final class DatabaseTicketActionTypeService extends DatabasePublicService<String,TicketActionType> implements TicketActionTypeService {

    private final ObjectFactory<TicketActionType> objectFactory = new AutoObjectFactory<TicketActionType>(TicketActionType.class, connector);

    DatabaseTicketActionTypeService(DatabaseConnector connector) {
        super(connector, String.class, TicketActionType.class);
    }

    @Override
    protected ArrayList<TicketActionType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketActionType>(),
            objectFactory,
            "select * from ticket_action_types"
        );
    }
}
