package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.EmailSmtpRelayType;
import com.aoindustries.aoserv.client.EmailSmtpRelayTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailSmtpRelayTypeService extends DatabasePublicService<String,EmailSmtpRelayType> implements EmailSmtpRelayTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<EmailSmtpRelayType> objectFactory = new AutoObjectFactory<EmailSmtpRelayType>(EmailSmtpRelayType.class, this);

    DatabaseEmailSmtpRelayTypeService(DatabaseConnector connector) {
        super(connector, String.class, EmailSmtpRelayType.class);
    }

    protected Set<EmailSmtpRelayType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from email_smtp_relay_types"
        );
    }
}
