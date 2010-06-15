package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BackupRetention;
import com.aoindustries.aoserv.client.BackupRetentionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBackupRetentionService extends DatabasePublicService<Short,BackupRetention> implements BackupRetentionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BackupRetention> objectFactory = new AutoObjectFactory<BackupRetention>(BackupRetention.class, this);

    DatabaseBackupRetentionService(DatabaseConnector connector) {
        super(connector, Short.class, BackupRetention.class);
    }

    protected Set<BackupRetention> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from backup_retentions"
        );
    }
}
