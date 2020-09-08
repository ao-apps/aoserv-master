/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.accounting.BankTransaction;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
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
		StreamableOutput out,
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
			conn.close(); // Don't hold database connection while writing response
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
