package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TicketCategory;
import com.aoindustries.aoserv.client.TicketCategoryService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTicketCategoryService extends DatabaseServiceIntegerKey<TicketCategory> implements TicketCategoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TicketCategory> objectFactory = new AutoObjectFactory<TicketCategory>(TicketCategory.class, this);

    DatabaseTicketCategoryService(DatabaseConnector connector) {
        super(connector, TicketCategory.class);
    }

    protected Set<TicketCategory> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_categories"
        );
    }

    protected Set<TicketCategory> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_categories"
        );
    }

    protected Set<TicketCategory> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ticket_categories"
        );
    }
}
