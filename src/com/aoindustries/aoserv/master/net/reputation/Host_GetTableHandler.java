/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net.reputation;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.reputation.Host;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Host_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.IP_REPUTATION_SET_HOSTS);
	}

	/**
	 * Admin may access all sets.
	 */
	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Host(),
			"select * from \"net.reputation\".\"Host\""
		);
	}

	/**
	 * Router may access all sets used by any limiters in the same server farm.
	 * Non-router daemon may not access any reputation sets.
	 *
	 * @see  User#isRouter()
	 */
	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		if(masterUser.isRouter()) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				new com.aoindustries.aoserv.client.net.Host(),
				"select distinct\n"
				+ "  irsh.*\n"
				+ "from\n"
				+ "  master.\"UserHost\" ms\n"
				+ "  inner join net.\"Host\"                      se   on ms.server    =   se.id\n"         // Find all servers can access
				+ "  inner join net.\"Host\"                      se2  on se.farm      =  se2.farm\n"       // Find all servers in the same farm
				+ "  inner join net.\"Device\"                    nd   on se2.id       =   nd.server\n"     // Find all net.Device in the same farm
				+ "  inner join \"net.reputation\".\"Limiter\"    irl  on nd.id        =  irl.net_device\n" // Find all limiters in the same farm
				+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id       = irls.limiter\n"    // Find all sets used by all limiters in the same farm
				+ "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\" =  irs.id\n"         // Find all sets used by any limiter in the same farm
				+ "  inner join \"net.reputation\".\"Host\"       irsh on irs.id       = irsh.\"set\"\n"    // Find all hosts belonging to these sets
				+ "where\n"
				+ "  ms.username=?",
				source.getUsername()
			);
		} else {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}
	}

	/**
	 * Regular user may only access the hosts for their own or subaccount sets.
	 */
	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Host(),
			"select\n"
			+ "  irsh.*\n"
			+ "from\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  \"net.reputation\".\"Set\" irs,\n"
			+ "  \"net.reputation\".\"Host\" irsh\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=irs.accounting\n"
			+ "  and irs.id=irsh.\"set\"",
			source.getUsername()
		);
	}
}
