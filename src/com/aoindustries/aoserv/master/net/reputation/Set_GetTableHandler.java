/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net.reputation;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.UsernameHandler;
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
public class Set_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.IP_REPUTATION_SETS);
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
			CursorMode.AUTO,
			new com.aoindustries.aoserv.client.net.reputation.Set(),
			"select * from \"net.reputation\".\"Set\""
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
				CursorMode.AUTO,
				new com.aoindustries.aoserv.client.net.reputation.Set(),
				"select distinct\n"
				+ "  irs.*\n"
				+ "from\n"
				+ "  master.\"UserHost\"                          ms\n"
				+ "  inner join net.\"Host\"                      se   on   ms.server  =   se.id\n"         // Find all servers can access
				+ "  inner join net.\"Host\"                      se2  on   se.farm    =  se2.farm\n"       // Find all servers in the same farm
				+ "  inner join net.\"Device\"                    nd   on  se2.id      =   nd.server\n"     // Find all net.Device in the same farm
				+ "  inner join \"net.reputation\".\"Limiter\"    irl  on   nd.id      =  irl.net_device\n" // Find all limiters in the same farm
				+ "  inner join \"net.reputation\".\"LimiterSet\" irls on  irl.id      = irls.limiter\n"    // Find all sets used by all limiters in the same farm
				+ "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\" =  irs.id\n"         // Find all sets used by any limiter in the same farm
				+ "where\n"
				+ "  ms.username=?",
				source.getUsername()
			);
		} else {
			MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
		}
	}

	/**
	 * Regular user may access their own or subaccount sets, as well as any parent account
	 * set that allows subaccount use.
	 */
	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		UserId username = source.getUsername();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new com.aoindustries.aoserv.client.net.reputation.Set(),
			"select\n"
			+ "  irs.*\n"
			+ "from\n"
			+ "  \"net.reputation\".\"Set\" irs\n"
			+ "where\n"
			// Allow own and any subaccount
			+ "  irs.id in (\n"
			+ "    select\n"
			+ "      irs2.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un,\n"
			+ "      billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "      \"net.reputation\".\"Set\" irs2\n"
			+ "    where\n"
			+ "      un.username=?\n"
			+ "      and un.package=pk.name\n"
			+ "      and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu1.accounting=irs2.accounting\n"
			+ "  )\n"
			// Allow any parent business that allow_subaccount_user
			+ "  or irs.id in (\n"
			+ "    select\n"
			+ "      irs3.id\n"
			+ "    from\n"
			+ "      \"net.reputation\".\"Set\" irs3\n"
			+ "    where\n"
			+ "      irs3.allow_subaccount_use\n"
			+ "      and account.is_account_or_parent(irs3.accounting, ?)\n"
			+ "  )",
			username,
			UsernameHandler.getBusinessForUsername(conn, username)
		);
	}
}
