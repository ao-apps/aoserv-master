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
final class DatabaseNoticeTypeService extends DatabasePublicService<String,NoticeType> implements NoticeTypeService {

    private final ObjectFactory<NoticeType> objectFactory = new AutoObjectFactory<NoticeType>(NoticeType.class, connector);

    DatabaseNoticeTypeService(DatabaseConnector connector) {
        super(connector, String.class, NoticeType.class);
    }

    @Override
    protected ArrayList<NoticeType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<NoticeType>(),
            objectFactory,
            "select * from notice_types"
        );
    }
}
