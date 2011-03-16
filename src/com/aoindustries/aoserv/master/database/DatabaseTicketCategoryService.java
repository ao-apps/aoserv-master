/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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
final class DatabaseTicketCategoryService extends DatabaseService<Integer,TicketCategory> implements TicketCategoryService {

    private final ObjectFactory<TicketCategory> objectFactory = new AutoObjectFactory<TicketCategory>(TicketCategory.class, connector);

    DatabaseTicketCategoryService(DatabaseConnector connector) {
        super(connector, Integer.class, TicketCategory.class);
    }

    @Override
    protected ArrayList<TicketCategory> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TicketCategory>(),
            objectFactory,
            "select * from ticket_categories"
        );
    }
}
