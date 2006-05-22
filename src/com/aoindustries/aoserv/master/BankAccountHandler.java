package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 * The <code>BankAccountHandler</code> handles all the accesses to the bank tables.
 *
 * @author  AO Industries, Inc.
 */
final public class BankAccountHandler {

    public static void checkAccounting(
        MasterDatabaseConnection conn,
        RequestSource source,
        String action
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BankAccountHandler.class, "checkAccounting(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            if(!isAccounting(conn, source)) throw new SQLException("Accounting not allowed, '"+action+"'");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Gets all transactions for one account.
     */
    public static void getBankTransactionsAccount(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String account
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BankAccountHandler.class, "getBankTransactionsAccount(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            if(isBankAccounting(conn, source)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new BankTransaction(),
                    "select * from bank_transactions where bank_account=?",
                    account
                );
            } else {
                List<BankTransaction> emptyList = Collections.emptyList();
                MasterServer.writeObjects(source, out, provideProgress, emptyList);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkBankAccounting(MasterDatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BankAccountHandler.class, "checkBankAccounting(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            if(!isBankAccounting(conn, source)) throw new SQLException("Bank accounting not allowed, '"+action+"'");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isAccounting(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BankAccountHandler.class, "isAccounting(MasterDatabaseConnection,RequestSource)", null);
        try {
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            return mu!=null && mu.canAccessAccounting();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isBankAccounting(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BankAccountHandler.class, "isBankAccounting(MasterDatabaseConnection,RequestSource)", null);
        try {
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            return mu!=null && mu.canAccessBankAccount();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}