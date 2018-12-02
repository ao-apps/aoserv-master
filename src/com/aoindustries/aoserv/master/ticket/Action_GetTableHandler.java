/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.ticket;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.ticket.Action;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.TicketHandler;
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
public class Action_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.TICKET_ACTIONS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.FETCH,
			new Action(),
			"select\n"
			+ "  id,\n"
			+ "  ticket,\n"
			+ "  administrator,\n"
			+ "  time,\n"
			+ "  action_type,\n"
			+ "  old_accounting,\n"
			+ "  new_accounting,\n"
			+ "  old_priority,\n"
			+ "  new_priority,\n"
			+ "  old_type,\n"
			+ "  new_type,\n"
			+ "  old_status,\n"
			+ "  new_status,\n"
			+ "  old_assigned_to,\n"
			+ "  new_assigned_to,\n"
			+ "  old_category,\n"
			+ "  new_category,\n"
			+ "  from_address,\n"
			+ "  summary\n"
			+ "from\n"
			+ "  ticket.\"Action\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		if(TicketHandler.isTicketAdmin(conn, source)) {
			// If a ticket admin, can see all ticket.Action
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new Action(),
				"select distinct\n" // TODO: distinct required?
				+ "  ta.id,\n"
				+ "  ta.ticket,\n"
				+ "  ta.administrator,\n"
				+ "  ta.time,\n"
				+ "  ta.action_type,\n"
				+ "  ta.old_accounting,\n"
				+ "  ta.new_accounting,\n"
				+ "  ta.old_priority,\n"
				+ "  ta.new_priority,\n"
				+ "  ta.old_type,\n"
				+ "  ta.new_type,\n"
				+ "  ta.old_status,\n"
				+ "  ta.new_status,\n"
				+ "  ta.old_assigned_to,\n"
				+ "  ta.new_assigned_to,\n"
				+ "  ta.old_category,\n"
				+ "  ta.new_category,\n"
				+ "  ta.from_address,\n"
				+ "  ta.summary\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  ticket.\"Ticket\" ti,\n"
				+ "  ticket.\"Action\" ta\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and (\n"
				+ "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
				+ "    or bu1.accounting=ti.brand\n" // Has access to brand
				+ "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
				+ "  )\n"
				+ "  and ti.id=ta.ticket",
				source.getUsername()
			);
		} else {
			// Can only see non-admin types and statuses
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new Action(),
				"select\n"
				+ "  ta.id,\n"
				+ "  ta.ticket,\n"
				+ "  ta.administrator,\n"
				+ "  ta.time,\n"
				+ "  ta.action_type,\n"
				+ "  ta.old_accounting,\n"
				+ "  ta.new_accounting,\n"
				+ "  ta.old_priority,\n"
				+ "  ta.new_priority,\n"
				+ "  ta.old_type,\n"
				+ "  ta.new_type,\n"
				+ "  ta.old_status,\n"
				+ "  ta.new_status,\n"
				+ "  ta.old_assigned_to,\n"
				+ "  ta.new_assigned_to,\n"
				+ "  ta.old_category,\n"
				+ "  ta.new_category,\n"
				+ "  ta.from_address,\n"
				+ "  ta.summary\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  ticket.\"Ticket\" ti,\n"
				+ "  ticket.\"Action\" ta,\n"
				+ "  ticket.\"ActionType\" tat\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and bu1.accounting=ti.accounting\n"
				+ "  and ti.status not in (?,?)\n"
				+ "  and ti.id=ta.ticket\n"
				+ "  and ta.action_type=tat.type\n"
				+ "  and not tat.visible_admin_only",
				source.getUsername(),
				Status.JUNK,
				Status.DELETED
			);
		}
	}
}
