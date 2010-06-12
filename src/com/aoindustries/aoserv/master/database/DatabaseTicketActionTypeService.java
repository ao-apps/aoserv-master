package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketActionType;
import com.aoindustries.aoserv.client.TicketActionTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketActionTypeService extends DatabasePublicService<String,TicketActionType> implements TicketActionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketActionType> objectFactory = new AutoObjectFactory<TicketActionType>(TicketActionType.class, this);

    DatabaseTicketActionTypeService(DatabaseConnector connector) {
        super(connector, String.class, TicketActionType.class);
    }

    protected Set<TicketActionType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_action_types"
        );
    }
}
