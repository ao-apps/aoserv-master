package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.EmailSpamAssassinIntegrationMode;
import com.aoindustries.aoserv.client.EmailSpamAssassinIntegrationModeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailSpamAssassinIntegrationModeService extends DatabasePublicService<String,EmailSpamAssassinIntegrationMode> implements EmailSpamAssassinIntegrationModeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<EmailSpamAssassinIntegrationMode> objectFactory = new AutoObjectFactory<EmailSpamAssassinIntegrationMode>(EmailSpamAssassinIntegrationMode.class, this);

    DatabaseEmailSpamAssassinIntegrationModeService(DatabaseConnector connector) {
        super(connector, String.class, EmailSpamAssassinIntegrationMode.class);
    }

    protected Set<EmailSpamAssassinIntegrationMode> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from email_sa_integration_modes"
        );
    }
}
