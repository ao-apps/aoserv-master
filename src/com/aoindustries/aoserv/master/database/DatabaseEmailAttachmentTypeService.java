package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.EmailAttachmentType;
import com.aoindustries.aoserv.client.EmailAttachmentTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailAttachmentTypeService extends DatabasePublicService<String,EmailAttachmentType> implements EmailAttachmentTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<EmailAttachmentType> objectFactory = new AutoObjectFactory<EmailAttachmentType>(EmailAttachmentType.class, this);

    DatabaseEmailAttachmentTypeService(DatabaseConnector connector) {
        super(connector, String.class, EmailAttachmentType.class);
    }

    protected Set<EmailAttachmentType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from email_attachment_types"
        );
    }
}
