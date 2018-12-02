/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.email.SpamMessage;
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
public class SpamMessageHandler {

	private SpamMessageHandler() {}

	private static final String QUERY_MASTER =
		"select * from email.\"SpamMessage\"";

	static public class GetObject implements TableHandler.GetObjectHandler {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.SPAM_EMAIL_MESSAGES);
		}

		@Override
		public void getObject(DatabaseConnection conn, RequestSource source, CompressedDataInputStream in, CompressedDataOutputStream out, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			int id = in.readCompressedInt();
			if(masterUser!=null && masterServers!=null && masterServers.length==0) {
				MasterServer.writeObject(
					conn,
					source,
					out,
					new SpamMessage(),
					QUERY_MASTER + " where id=?",
					id
				);
			} else {
				out.writeByte(AoservProtocol.DONE);
			}
		}
	}

	static public class GetTable extends TableHandler.GetTableHandlerByRole {

		@Override
		public Set<Table.TableID> getTableIds() {
			return EnumSet.of(Table.TableID.SPAM_EMAIL_MESSAGES);
		}

		@Override
		protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new SpamMessage(),
				QUERY_MASTER
			);
		}

		@Override
		protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}

		@Override
		protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}
	}
}
