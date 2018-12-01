/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net.reputation;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
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
public class LimiterSet_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.IP_REPUTATION_LIMITER_SETS);
	}

	/**
	 * Admin may access all limiters.
	 */
	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new com.aoindustries.aoserv.client.net.reputation.Set(),
			"select * from \"net.reputation\".\"LimiterSet\""
		);
	}

	/**
	 * Router may access all limiters in the same server farm.
	 * Non-router daemon may not access any reputation limiters.
	 *
	 * @see  User#isRouter()
	 */
	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		if(masterUser.isRouter()) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				new com.aoindustries.aoserv.client.net.reputation.Set(),
				"select distinct\n"
				+ "  irls.*\n"
				+ "from\n"
				+ "             master.\"UserHost\"               ms\n"
				+ "  inner join net.\"Host\"                      se   on  ms.server =   se.id\n"         // Find all servers can access
				+ "  inner join net.\"Host\"                      se2  on  se.farm   =  se2.farm\n"       // Find all servers in the same farm
				+ "  inner join net.\"Device\"                    nd   on se2.id     =   nd.server\n"     // Find all net.Device in the same farm
				+ "  inner join \"net.reputation\".\"Limiter\"    irl  on  nd.id     =  irl.net_device\n" // Find all limiters in the same farm
				+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id     = irls.limiter\n"    // Find all limiters sets in the same farm
				+ "where\n"
				+ "  ms.username=?",
				source.getUsername()
			);
		} else {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}
	}

	/**
	 * Regular user may access the limiters for servers they have direct access to.
	 */
	@Override
	public void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new com.aoindustries.aoserv.client.net.reputation.Set(),
			"select\n"
			+ "  irls.*\n"
			+ "from\n"
			+ "             account.\"Username\"              un\n"
			+ "  inner join billing.\"Package\"               pk   on  un.package    =   pk.name\n"
			+ "  inner join account.\"AccountHost\"           bs   on  pk.accounting =   bs.accounting\n"
			+ "  inner join net.\"Device\"                    nd   on  bs.server     =   nd.server\n"
			+ "  inner join \"net.reputation\".\"Limiter\"    irl  on  nd.id         =  irl.net_device\n"
			+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id         = irls.limiter\n"
			+ "where\n"
			+ "  un.username=?",
			source.getUsername()
		);
	}
}
