/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.backup;

import com.aoindustries.aoserv.client.backup.BackupPartition;
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
public class BackupPartition_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.BACKUP_PARTITIONS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new BackupPartition(),
			"select * from backup.\"BackupPartition\""
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
			new BackupPartition(),
			"select\n"
			+ "  bp.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  backup.\"BackupPartition\" bp\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			+ "    ms.server=bp.ao_server\n"
			+ "    or (\n"
			+ "      select\n"
			+ "        ffr.id\n"
			+ "      from\n"
			+ "        backup.\"FileReplication\" ffr\n"
			+ "        inner join backup.\"BackupPartition\" bp2 on ffr.backup_partition=bp2.id\n"
			+ "      where\n"
			+ "        ms.server=ffr.server\n"
			+ "        and bp.ao_server=bp2.ao_server\n"
			+ "      limit 1\n"
			+ "    ) is not null\n"
			+ "  )",
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
			new BackupPartition(),
			"select distinct\n"
			+ "  bp.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  backup.\"BackupPartition\" bp\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    bs.server=bp.ao_server\n"
			//+ "    or (\n"
			//+ "      select\n"
			//+ "        ffr.id\n"
			//+ "      from\n"
			//+ "        backup.\"FileReplication\" ffr\n"
			//+ "        inner join backup.\"BackupPartition\" bp2 on ffr.backup_partition=bp2.id\n"
			//+ "      where\n"
			//+ "        bs.server=ffr.server\n"
			//+ "        and bp.ao_server=bp2.ao_server\n"
			//+ "      limit 1\n"
			//+ "    ) is not null\n"
			+ "  )",
			source.getCurrentAdministrator()
		);
	}
}