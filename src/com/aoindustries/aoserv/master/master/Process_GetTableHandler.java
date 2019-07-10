/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.aoserv.client.master.Process;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.AccountUserHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Process_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.MASTER_PROCESSES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjectsSynced(
			source,
			out,
			provideProgress,
			Process_Manager.getSnapshot()
		);
	}

	private void getTableFiltered(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		List<Process> processesCopy = Process_Manager.getSnapshot();
		List<Process> filtered = new ArrayList<>();
		Iterator<Process> I = processesCopy.iterator();
		while(I.hasNext()) {
			Process process = I.next();
			com.aoindustries.aoserv.client.account.User.Name effectiveUser = process.getEffectiveUser();
			if(
				effectiveUser != null
				&& AccountUserHandler.canAccessUser(conn, source, effectiveUser)
			) filtered.add(process);
		}
		MasterServer.writeObjectsSynced(source, out, provideProgress, filtered);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		getTableFiltered(conn, source, out, provideProgress, tableID);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		getTableFiltered(conn, source, out, provideProgress, tableID);
	}
}
