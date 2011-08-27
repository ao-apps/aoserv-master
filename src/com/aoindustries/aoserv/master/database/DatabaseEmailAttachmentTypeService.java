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
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailAttachmentTypeService extends DatabaseService<String,EmailAttachmentType> implements EmailAttachmentTypeService {

    private final ObjectFactory<EmailAttachmentType> objectFactory = new AutoObjectFactory<EmailAttachmentType>(EmailAttachmentType.class, connector);

    DatabaseEmailAttachmentTypeService(DatabaseConnector connector) {
        super(connector, String.class, EmailAttachmentType.class);
    }

    @Override
    protected List<EmailAttachmentType> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<EmailAttachmentType>(),
            objectFactory,
            "select * from email_attachment_types"
        );
    }
}
