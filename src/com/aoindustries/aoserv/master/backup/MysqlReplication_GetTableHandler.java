/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.backup;

import com.aoindustries.aoserv.client.backup.MysqlReplication;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
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
public class MysqlReplication_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.FAILOVER_MYSQL_REPLICATIONS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new MysqlReplication(),
			"select * from backup.\"MysqlReplication\""
		); 
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new MysqlReplication(),
			"select\n"
			+ "  fmr.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  backup.\"FileReplication\" ffr,\n"
			+ "  backup.\"MysqlReplication\" fmr\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			+ "    (\n"
			// ao_server-based
			+ "      ms.server=fmr.ao_server\n"
			+ "    ) or (\n"
			// replication-based
			+ "      ms.server=ffr.server\n"
			+ "      and ffr.id=fmr.replication\n"
			+ "    )\n"
			+ "  )",
			source.getUsername()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new MysqlReplication(),
			"select distinct\n"
			+ "  fmr.*\n"
			+ "from\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  backup.\"FileReplication\" ffr,\n"
			+ "  backup.\"MysqlReplication\" fmr\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    (\n"
			// ao_server-based
			+ "      bs.server=fmr.ao_server\n"
			+ "    ) or (\n"
			// replication-based
			+ "      bs.server=ffr.server\n"
			+ "      and ffr.id=fmr.replication\n"
			+ "    )\n"
			+ "  )",
			source.getUsername()
		);
	}
}
