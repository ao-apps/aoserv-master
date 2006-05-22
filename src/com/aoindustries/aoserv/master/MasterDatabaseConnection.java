package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
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
