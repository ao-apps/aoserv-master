package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketType;
import com.aoindustries.aoserv.client.TicketTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketTypeService extends DatabasePublicService<String,TicketType> implements TicketTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketType> objectFactory = new AutoObjectFactory<TicketType>(TicketType.class, this);

    DatabaseTicketTypeService(DatabaseConnector connector) {
        super(connector, String.class, TicketType.class);
    }

    protected Set<TicketType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_types"
        );
    }
}
