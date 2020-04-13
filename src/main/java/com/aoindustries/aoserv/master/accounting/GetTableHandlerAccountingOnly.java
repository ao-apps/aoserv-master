/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019  AO Industries, Inc.
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
package com.aoindustries.aoserv.master.accounting;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.BankAccountHandler;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

/**
 * @author  AO Industries, Inc.
 */
interface GetTableHandlerAccountingOnly extends TableHandler.GetTableHandler {

	/**
	 * When is a {@link User master user} and has {@link User#canAccessBankAccount()},
	 * calls {@link #getTableAccounting(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.stream.StreamableOutput, boolean, com.aoindustries.aoserv.client.schema.Table.TableID, com.aoindustries.aoserv.client.master.User)}.
	 * Otherwise, writes an empty table.
	 *
	 * @see BankAccountHandler#isBankAccounting(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource)
	 */
	@Override
	default void getTable(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		boolean provideProgress,
		Table.TableID tableID,
		User masterUser,
		UserHost[] masterServers
	) throws IOException, SQLException {
		if(BankAccountHandler.isBankAccounting(conn, source)) {
			getTableAccounting(conn, source, out, provideProgress, tableID, masterUser);
		} else {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}
	}

	/**
	 * Handles a {@link User master user} request for the given accounting table,
	 * when has {@link User#canAccessBankAccount()}.
	 *
	 * @see BankAccountHandler#isBankAccounting(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource)
	 */
	void getTableAccounting(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		boolean provideProgress,
		Table.TableID tableID,
		User masterUser
	) throws IOException, SQLException;
}
