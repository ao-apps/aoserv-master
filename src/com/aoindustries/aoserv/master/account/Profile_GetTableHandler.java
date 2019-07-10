/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.account;

import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.account.Profile;
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
public class Profile_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	private static final String COLUMNS =
		"  ap.*,\n"
		+ "  ARRAY(SELECT be.\"billingEmail\"" + (AOServObject.USE_ARRAY_OF_DOMAIN ? "" : "::text") + " FROM account.\"Profile.billingEmail{}\" be WHERE ap.id = be.id ORDER BY index) AS \"billingEmail{}\",\n"
		+ "  ARRAY(SELECT te.\"technicalEmail\"" + (AOServObject.USE_ARRAY_OF_DOMAIN ? "" : "::text") + " FROM account.\"Profile.technicalEmail{}\" te WHERE ap.id = te.id ORDER BY index) AS \"technicalEmail{}\"";

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.BUSINESS_PROFILES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Profile(),
			"SELECT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "  account.\"Profile\" ap"
		);
	}

	// TODO: Does the daemon need access to the profiles?  What for?  I find no reference in the aoserv-daemon project
	// TODO: This might be best changed once roles are defined, and not just a class of filtering by server.
	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Profile(),
			"SELECT DISTINCT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  account.\"Profile\" ap\n"
			+ "WHERE\n"
			+ "  ms.username = ?\n"
			+ "  AND ms.server = bs.server\n"
			+ "  AND bs.accounting = ap.accounting",
			source.getCurrentAdministrator()
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
			new Profile(),
			"SELECT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  account.\"Profile\" ap\n"
			+ "WHERE\n"
			+ "  un.username = ?\n"
			+ "  AND un.package = pk.\"name\"\n"
			+ "  AND (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  AND bu1.accounting = ap.accounting",
			source.getCurrentAdministrator()
		);
	}
}
