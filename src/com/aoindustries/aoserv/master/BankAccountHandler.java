/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.accounting.BankTransaction;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
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

	// TODO: Move to an AccountingHandler or BillingHandler
	public static void checkIsAccounting(
		DatabaseConnection conn,
		RequestSource source,
		String action
	) throws IOException, SQLException {
		if(!isAccounting(conn, source)) throw new SQLException("Accounting not allowed, '"+action+"'");
	}

	/**
	 * Gets all transactions for one account.
	 */
	public static void getTransactionsForAccount(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		String account
	) throws IOException, SQLException {
		if(isBankAccounting(conn, source)) {
			// TODO: release conn before writing to out
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new BankTransaction(),
				"select * from accounting.\"BankTransaction\" where account=?",
				account
			);
		} else {
			List<BankTransaction> emptyList = Collections.emptyList();
			conn.releaseConnection();
			MasterServer.writeObjects(source, out, provideProgress, emptyList);
		}
	}

	public static void checkIsBankAccounting(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
		if(!isBankAccounting(conn, source)) throw new SQLException("Bank accounting not allowed, '"+action+"'");
	}

	// TODO: Move to an AccountingHandler or BillingHandler
	public static boolean isAccounting(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		User mu=MasterServer.getUser(conn, source.getCurrentAdministrator());
		return mu!=null && mu.canAccessAccounting();
	}

	public static boolean isBankAccounting(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		User mu=MasterServer.getUser(conn, source.getCurrentAdministrator());
		return mu!=null && mu.canAccessBankAccount();
	}
}
