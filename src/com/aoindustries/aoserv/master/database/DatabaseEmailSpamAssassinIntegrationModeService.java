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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailSpamAssassinIntegrationModeService extends DatabasePublicService<String,EmailSpamAssassinIntegrationMode> implements EmailSpamAssassinIntegrationModeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<EmailSpamAssassinIntegrationMode> objectFactory = new AutoObjectFactory<EmailSpamAssassinIntegrationMode>(EmailSpamAssassinIntegrationMode.class, this);

    DatabaseEmailSpamAssassinIntegrationModeService(DatabaseConnector connector) {
        super(connector, String.class, EmailSpamAssassinIntegrationMode.class);
    }

    @Override
    protected Set<EmailSpamAssassinIntegrationMode> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<EmailSpamAssassinIntegrationMode>(),
            objectFactory,
            "select * from email_sa_integration_modes"
        );
    }
}
