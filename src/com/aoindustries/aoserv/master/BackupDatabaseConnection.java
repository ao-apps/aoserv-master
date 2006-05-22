package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import java.sql.*;

/**
 * @author  AO Industries, Inc.
 */
public final class BackupDatabaseConnection extends DatabaseConnection {

    public static final int MAX_ABORTED_RETRIES=10;
    public static final long ABORTED_RETRY_DELAY=(long)5*1000;

    public static boolean isTransactionAbort(SQLException err) {
        while(err!=null) {
            Throwable T=err;
            while(T!=null) {
                String message=T.getMessage();
                if(message!=null && message.indexOf("aborted")!=-1) return true;
                T=T.getCause();
            }
            err=err.getNextException();
        }
        return false;
    }

    BackupDatabaseConnection(BackupDatabase database) {
        super(database);
        Profiler.startProfile(Profiler.FAST, BackupDatabaseConnection.class, "<init>(BackupDatabase)", null);
        Profiler.endProfile(Profiler.FAST);
    }
}
