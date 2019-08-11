/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.billing.Currency;
import com.aoindustries.aoserv.client.billing.NoticeLog;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class NoticeLog_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.NOTICE_LOG);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.AUTO,
				new NoticeLog(),
				"SELECT\n"
				+ "  nl.*,\n"
				// Protocol compatibility
				+ "  COALESCE(\n"
				+ "    (\n"
				+ "      SELECT\n"
				+ "        \"balance.value\"\n"
				+ "      FROM\n"
				+ "        billing.\"NoticeLog.balance\" nlb\n"
				+ "      WHERE\n"
				+ "        nl.id = nlb.\"noticeLog\"\n"
				+ "        AND nlb.\"balance.currency\" = ?\n"
				+ "    ),\n"
				+ "    '0.00'::numeric(9,2)\n"
				+ "  ) AS balance\n"
				+ "FROM\n"
				+ "  billing.\"NoticeLog\" nl",
				Currency.USD.getCurrencyCode()
			);
		} else {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.AUTO,
				new NoticeLog(),
				"SELECT\n"
				+ "  *,\n"
				// Protocol compatibility
				+ "  '0.00'::numeric(9,2) AS balance\n"
				+ "FROM\n"
				+ "  billing.\"NoticeLog\""
			);
		}
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.AUTO,
				new NoticeLog(),
				"SELECT\n"
				+ "  nl.*,\n"
				// Protocol compatibility
				+ "  COALESCE(\n"
				+ "    (\n"
				+ "      SELECT\n"
				+ "        \"balance.value\"\n"
				+ "      FROM\n"
				+ "        billing.\"NoticeLog.balance\" nlb\n"
				+ "      WHERE\n"
				+ "        nl.id = nlb.\"noticeLog\"\n"
				+ "        AND nlb.\"balance.currency\" = ?\n"
				+ "    ),\n"
				+ "    '0.00'::numeric(9,2)\n"
				+ "  ) AS balance\n"
				+ "FROM\n"
				+ "  account.\"User\" un,\n"
				+ "  billing.\"Package\" pk,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  billing.\"NoticeLog\" nl\n"
				+ "WHERE\n"
				+ "  un.username = ?\n"
				+ "  AND un.package = pk.name\n"
				+ "  AND (\n"
				+ TableHandler.PK_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  AND bu1.accounting = nl.accounting",
				Currency.USD.getCurrencyCode(),
				source.getCurrentAdministrator()
			);
		} else {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.AUTO,
				new NoticeLog(),
				"SELECT\n"
				+ "  nl.*,\n"
				// Protocol compatibility
				+ "  '0.00'::numeric(9,2) AS balance\n"
				+ "FROM\n"
				+ "  account.\"User\" un,\n"
				+ "  billing.\"Package\" pk,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  billing.\"NoticeLog\" nl\n"
				+ "WHERE\n"
				+ "  un.username = ?\n"
				+ "  AND un.package = pk.name\n"
				+ "  AND (\n"
				+ TableHandler.PK_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  AND bu1.accounting = nl.accounting",
				source.getCurrentAdministrator()
			);
		}
	}
}
