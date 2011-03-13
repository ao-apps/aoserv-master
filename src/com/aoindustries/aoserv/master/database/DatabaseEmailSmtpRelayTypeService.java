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
final class DatabaseEmailSmtpRelayTypeService extends DatabasePublicService<String,EmailSmtpRelayType> implements EmailSmtpRelayTypeService {

    private final ObjectFactory<EmailSmtpRelayType> objectFactory = new AutoObjectFactory<EmailSmtpRelayType>(EmailSmtpRelayType.class, connector);

    DatabaseEmailSmtpRelayTypeService(DatabaseConnector connector) {
        super(connector, String.class, EmailSmtpRelayType.class);
    }

    @Override
    protected ArrayList<EmailSmtpRelayType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<EmailSmtpRelayType>(),
            objectFactory,
            "select * from email_smtp_relay_types"
        );
    }
}
