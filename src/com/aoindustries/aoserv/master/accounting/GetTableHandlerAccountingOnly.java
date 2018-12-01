/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
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
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

/**
 * @author  AO Industries, Inc.
 */
interface GetTableHandlerAccountingOnly extends TableHandler.GetTableHandler {

	/**
	 * When is a {@link User master user} and has {@link User#canAccessBankAccount()},
	 * calls {@link #getTableAccounting(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.client.schema.Table.TableID, com.aoindustries.aoserv.client.master.User)}.
	 * Otherwise, writes an empty table.
	 *
	 * @see BankAccountHandler#isBankAccounting(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource)
	 */
	@Override
	default void getTable(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
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
		CompressedDataOutputStream out,
		boolean provideProgress,
		Table.TableID tableID,
		User masterUser
	) throws IOException, SQLException;
}
