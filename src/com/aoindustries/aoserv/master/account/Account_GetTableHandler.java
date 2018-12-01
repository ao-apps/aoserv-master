/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.account;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Account_GetTableHandler implements TableHandler.GetTableHandler {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.BUSINESSES);
	}

	@Override
	public void getTable(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		Table.TableID tableID,
		User masterUser,
		UserHost[] masterServers
	) throws IOException, SQLException {
		UserId username = source.getUsername();
		if(masterUser != null) {
			assert masterServers != null;
			if(masterServers.length == 0) {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					new Account(),
					"select * from account.\"Account\""
				);
			} else {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					new Account(),
					"select distinct\n"
					+ "  bu.*\n"
					+ "from\n"
					+ "  master.\"UserHost\" ms,\n"
					+ "  account.\"AccountHost\" bs,\n"
					+ "  account.\"Account\" bu\n"
					+ "where\n"
					+ "  ms.username=?\n"
					+ "  and ms.server=bs.server\n"
					+ "  and bs.accounting=bu.accounting",
					username
				);
			}
		} else {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				new Account(),
				"select\n"
				+ "  bu1.*\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk,\n"
				+ TableHandler.BU1_PARENTS_JOIN_NO_COMMA
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk.name\n"
				+ "  and (\n"
				+ TableHandler.PK_BU1_PARENTS_WHERE
				+ "  )",
				username
			);
		}
	}
}
