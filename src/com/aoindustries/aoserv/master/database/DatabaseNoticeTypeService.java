package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.NoticeType;
import com.aoindustries.aoserv.client.NoticeTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNoticeTypeService extends DatabasePublicService<String,NoticeType> implements NoticeTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NoticeType> objectFactory = new AutoObjectFactory<NoticeType>(NoticeType.class, this);

    DatabaseNoticeTypeService(DatabaseConnector connector) {
        super(connector, String.class, NoticeType.class);
    }

    protected Set<NoticeType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from notice_types"
        );
    }
}
