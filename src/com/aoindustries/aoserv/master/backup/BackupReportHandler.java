/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.backup;

import com.aoindustries.aoserv.client.backup.BackupReport;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
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
		+ "  account.\"Username\" un,\n"
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
		public void getObject(DatabaseConnection conn, RequestSource source, CompressedDataInputStream in, CompressedDataOutputStream out, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			int id = in.readCompressedInt();
			if(masterUser != null) {
				assert masterServers != null;
				if(masterServers.length == 0) {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						QUERY_MASTER + " where id=?",
						id
					);
				} else {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						QUERY_DAEMON + "\n"
						+ "  and br.id=?",
						source.getUsername(),
						id
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
					source.getUsername(),
					id
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
		protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
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
		protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new BackupReport(),
				QUERY_DAEMON,
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
				CursorMode.FETCH,
				new BackupReport(),
				QUERY_ADMINISTRATOR,
				source.getUsername()
			);
		}
	}
}
