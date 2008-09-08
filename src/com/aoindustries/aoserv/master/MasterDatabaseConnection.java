package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;

/**
 * @author  AO Industries, Inc.
 */
public final class MasterDatabaseConnection extends DatabaseConnection {

    MasterDatabaseConnection(MasterDatabase database) {
        super(database);
        Profiler.startProfile(Profiler.FAST, MasterDatabaseConnection.class, "<init>(MasterDatabase)", null);
        Profiler.endProfile(Profiler.FAST);
    }
}
