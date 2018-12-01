/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.distribution.management;

import com.aoindustries.aoserv.client.distribution.management.DistroFile;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class DistroFile_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.DISTRO_FILES);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		if(provideProgress) throw new SQLException("Unable to provide progress when fetching rows for " + TableHandler.getTableName(conn, Table.TableID.DISTRO_FILES));
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107)<=0) {
			MasterServer.writeObjects(source, out, false, Collections.emptyList());
		} else {
			MasterServer.fetchObjects(
				conn,
				source,
				out,
				new DistroFile(),
				"select * from \"distribution.management\".\"DistroFile\""
			);
		}
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		// Restrict to the operating system versions accessible to this user
		IntList osVersions = TableHandler.getOperatingSystemVersions(conn, source);
		if(osVersions.size()==0) {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		} else {
			if(provideProgress) throw new SQLException("Unable to provide progress when fetching rows for " + TableHandler.getTableName(conn, Table.TableID.DISTRO_FILES));
			StringBuilder sql=new StringBuilder();
			sql.append("select * from \"distribution.management\".\"DistroFile\" where operating_system_version in (");
			for(int c=0;c<osVersions.size();c++) {
				if(c>0) sql.append(',');
				sql.append(osVersions.getInt(c));
			}
			sql.append(')');
			MasterServer.fetchObjects(
				conn,
				source,
				out,
				new DistroFile(),
				sql.toString()
			);
		}
	}

	@Override
	public void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}
}