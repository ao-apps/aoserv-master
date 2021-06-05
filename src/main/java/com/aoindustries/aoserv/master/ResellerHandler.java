/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2009-2013, 2015, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.client.account.Account;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>ResellerHandler</code> handles all the accesses to the reseller tables.
 *
 * @author  AO Industries, Inc.
 */
final public class ResellerHandler {

	private ResellerHandler() {
	}

	/**
	 * Gets the lowest-level reseller that is at or above the provided account.
	 * Will skip past reseller.Reseller that are flagged as auto-escalate.
	 */
	public static Account.Name getResellerForAccountAutoEscalate(
		DatabaseConnection conn,
		Account.Name originalAccount
	) throws IOException, SQLException {
		Account.Name account = originalAccount;
		while(account!=null) {
			if(conn.queryBoolean("select (select accounting from reseller.\"Reseller\" where accounting=? and not ticket_auto_escalate) is not null", account)) return account;
			account = AccountHandler.getParentAccount(conn, account);
		}
		throw new SQLException("Unable to find Reseller for Account: "+originalAccount);
	}
}
