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
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketTypeService extends DatabaseServiceStringKey<TicketType> implements TicketTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketType> objectFactory = new AutoObjectFactory<TicketType>(TicketType.class, this);

    DatabaseTicketTypeService(DatabaseConnector connector) {
        super(connector, TicketType.class);
    }

    protected Set<TicketType> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_types"
        );
    }

    protected Set<TicketType> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_types"
        );
    }

    protected Set<TicketType> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_types"
        );
    }
}
