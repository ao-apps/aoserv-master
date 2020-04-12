/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.backup;

import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class FileReplication_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.FAILOVER_FILE_REPLICATIONS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new FileReplication(),
			"select\n"
			+ "  id,\n"
			+ "  server,\n"
			+ "  backup_partition,\n"
			+ "  max_bit_rate,\n"
			+ "  use_compression,\n"
			+ "  retention,\n"
			+ "  connect_address,\n"
			+ "  host(connect_from) as connect_from,\n"
			+ "  enabled,\n"
			+ "  quota_gid\n"
			+ "from\n"
			+ "  backup.\"FileReplication\""
		); 
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new FileReplication(),
			"select\n"
			+ "  ffr.id,\n"
			+ "  ffr.server,\n"
			+ "  ffr.backup_partition,\n"
			+ "  ffr.max_bit_rate,\n"
			+ "  ffr.use_compression,\n"
			+ "  ffr.retention,\n"
			+ "  ffr.connect_address,\n"
			+ "  host(ffr.connect_from) as connect_from,\n"
			+ "  ffr.enabled,\n"
			+ "  ffr.quota_gid\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  backup.\"FileReplication\" ffr\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=ffr.server",
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new FileReplication(),
			"select\n"
			+ "  ffr.id,\n"
			+ "  ffr.server,\n"
			+ "  ffr.backup_partition,\n"
			+ "  ffr.max_bit_rate,\n"
			+ "  ffr.use_compression,\n"
			+ "  ffr.retention,\n"
			+ "  ffr.connect_address,\n"
			+ "  host(ffr.connect_from) as connect_from,\n"
			+ "  ffr.enabled,\n"
			+ "  ffr.quota_gid\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  backup.\"FileReplication\" ffr\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and bs.server=ffr.server",
			source.getCurrentAdministrator()
		);
	}
}
