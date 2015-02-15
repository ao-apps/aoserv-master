/*
 * Copyright 2001-2013, 2015 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.BankTransaction;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * The <code>BankAccountHandler</code> handles all the accesses to the bank tables.
 *
 * @author  AO Industries, Inc.
 */
final public class BankAccountHandler {

    private BankAccountHandler() {
    }

    public static void checkAccounting(
        DatabaseConnection conn,
        RequestSource source,
        String action
    ) throws IOException, SQLException {
        if(!isAccounting(conn, source)) throw new SQLException("Accounting not allowed, '"+action+"'");
    }

    /**
     * Gets all transactions for one account.
     */
    public static void getBankTransactionsAccount(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String account
    ) throws IOException, SQLException {
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
    }

    public static void checkBankAccounting(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
        if(!isBankAccounting(conn, source)) throw new SQLException("Bank accounting not allowed, '"+action+"'");
    }

    public static boolean isAccounting(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
        return mu!=null && mu.canAccessAccounting();
    }

    public static boolean isBankAccounting(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
        return mu!=null && mu.canAccessBankAccount();
    }
}