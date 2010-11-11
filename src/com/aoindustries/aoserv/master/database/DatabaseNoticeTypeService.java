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
final class DatabaseNoticeTypeService extends DatabasePublicService<String,NoticeType> implements NoticeTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NoticeType> objectFactory = new AutoObjectFactory<NoticeType>(NoticeType.class, this);

    DatabaseNoticeTypeService(DatabaseConnector connector) {
        super(connector, String.class, NoticeType.class);
    }

    @Override
    protected Set<NoticeType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<NoticeType>(),
            objectFactory,
            "select * from notice_types"
        );
    }
}
