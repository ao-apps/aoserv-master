/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.billing.Currency;
import com.aoindustries.aoserv.client.billing.Transaction;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
// TODO: TransactionService
public class TransactionHandler {

	private TransactionHandler() {}

	private static final String QUERY_MASTER =
		"select * from billing.\"Transaction\"";

	private static final String QUERY_ADMINISTRATOR =
		"select\n"
		+ "  tr.*\n"
		+ "from\n"
		+ "  account.\"User\" un1,\n"
		+ "  billing.\"Package\" pk1,\n"
		+ TableHandler.BU1_PARENTS_JOIN
		+ "  billing.\"Transaction\" tr\n"
		+ "where\n"
		+ "  un1.username=?\n"
		+ "  and un1.package=pk1.name\n"
		+ "  and (\n"
		+ TableHandler.PK1_BU1_PARENTS_WHERE
		+ "  )\n"
		+ "  and bu1.accounting=tr.accounting";

	static public class GetObject implements TableHandler.GetObjectHandler {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.TRANSACTIONS);
		}

		@Override
		public void getObject(DatabaseConnection conn, RequestSource source, CompressedDataInputStream in, CompressedDataOutputStream out, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			int transid = in.readCompressedInt();
			if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
				if(masterUser != null) {
					assert masterServers != null;
					if(masterServers.length == 0) {
						MasterServer.writeObject(
							conn,
							source,
							out,
							new Transaction(),
							QUERY_MASTER + " WHERE transid=? AND \"rate.currency\"=?",
							transid,
							Currency.USD.getCurrencyCode()
						);
					} else {
						out.writeShort(AoservProtocol.DONE);
					}
				} else {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new Transaction(),
						QUERY_ADMINISTRATOR + "\n"
						+ "  AND tr.transid=?\n"
						+ "  AND \"rate.currency\"=?",
						source.getCurrentAdministrator(),
						transid,
						Currency.USD.getCurrencyCode()
					);
				}
			} else {
				throw new IOException("GetObject for Transaction only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
			}
		}
	}

	static public class GetTable extends TableHandler.GetTableHandlerByRole {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.TRANSACTIONS);
		}

		@Override
		protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
			if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new Transaction(),
					QUERY_MASTER + " WHERE \"rate.currency\"=?",
					Currency.USD.getCurrencyCode()
				);
			} else {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new Transaction(),
					QUERY_MASTER
				);
			}
		}

		@Override
		protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}

		@Override
		protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
			if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new Transaction(),
					QUERY_ADMINISTRATOR + "\n"
					+ "  AND \"rate.currency\"=?",
					source.getCurrentAdministrator(),
					Currency.USD.getCurrencyCode()
				);
			} else {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.FETCH,
					new Transaction(),
					QUERY_ADMINISTRATOR,
					source.getCurrentAdministrator()
				);
			}
		}
	}
}
