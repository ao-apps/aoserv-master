package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.sql.DatabaseConnection;

/**
 * @author  AO Industries, Inc.
 */
public final class MasterDatabaseConnection extends DatabaseConnection {

    MasterDatabaseConnection(MasterDatabase database) {
        super(database);
    }
}
