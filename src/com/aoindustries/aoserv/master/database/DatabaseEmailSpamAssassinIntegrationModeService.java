/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseEmailSpamAssassinIntegrationModeService extends DatabaseService<String,EmailSpamAssassinIntegrationMode> implements EmailSpamAssassinIntegrationModeService {

    private final ObjectFactory<EmailSpamAssassinIntegrationMode> objectFactory = new AutoObjectFactory<EmailSpamAssassinIntegrationMode>(EmailSpamAssassinIntegrationMode.class, connector);

    DatabaseEmailSpamAssassinIntegrationModeService(DatabaseConnector connector) {
        super(connector, String.class, EmailSpamAssassinIntegrationMode.class);
    }

    @Override
    protected ArrayList<EmailSpamAssassinIntegrationMode> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<EmailSpamAssassinIntegrationMode>(),
            objectFactory,
            "select * from email_sa_integration_modes"
        );
    }
}
