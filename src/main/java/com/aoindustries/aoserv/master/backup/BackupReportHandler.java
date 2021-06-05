/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021  AO Industries, Inc.
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
package com.aoindustries.aoserv.master.backup;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.backup.BackupReport;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class BackupReportHandler {

	private BackupReportHandler() {}

	private static final String QUERY_MASTER =
		"select * from backup.\"BackupReport\"";

	private static final String QUERY_DAEMON =
		"select\n"
		+ "  br.*\n"
		+ "from\n"
		+ "  master.\"UserHost\" ms,\n"
		+ "  backup.\"BackupReport\" br\n"
		+ "where\n"
		+ "  ms.username=?\n"
		+ "  and ms.server=br.server";

	private static final String QUERY_ADMINISTRATOR =
		"select\n"
		+ "  br.*\n"
		+ "from\n"
		+ "  account.\"User\" un,\n"
		+ "  billing.\"Package\" pk1,\n"
		+ TableHandler.BU1_PARENTS_JOIN
		+ "  billing.\"Package\" pk2,\n"
		+ "  backup.\"BackupReport\" br\n"
		+ "where\n"
		+ "  un.username=?\n"
		+ "  and un.package=pk1.name\n"
		+ "  and (\n"
		+ TableHandler.PK1_BU1_PARENTS_WHERE
		+ "  )\n"
		+ "  and bu1.accounting=pk2.accounting\n"
		+ "  and pk2.id=br.package";

	static public class GetObject implements TableHandler.GetObjectHandler {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.BACKUP_REPORTS);
		}

		@Override
		public void getObject(DatabaseConnection conn, RequestSource source, StreamableInput in, StreamableOutput out, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			int backupReport = in.readCompressedInt();
			if(masterUser != null) {
				assert masterServers != null;
				if(masterServers.length == 0) {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						QUERY_MASTER + " where id=?",
						backupReport
					);
				} else {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						QUERY_DAEMON + "\n"
						+ "  and br.id=?",
						source.getCurrentAdministrator(),
						backupReport
					);
				}
			} else {
				MasterServer.writeObject(
					conn,
					source,
					out,
					new BackupReport(),
					QUERY_ADMINISTRATOR + "\n"
					+ "  and br.id=?",
					source.getCurrentAdministrator(),
					backupReport
				);
			}
		}
	}

	static public class GetTable extends TableHandler.GetTableHandlerByRole {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.BACKUP_REPORTS);
		}

		@Override
		protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new BackupReport(),
				QUERY_MASTER
			); 
		}

		@Override
		protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new BackupReport(),
				QUERY_DAEMON,
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
				CursorMode.FETCH,
				new BackupReport(),
				QUERY_ADMINISTRATOR,
				source.getCurrentAdministrator()
			);
		}
	}
}
