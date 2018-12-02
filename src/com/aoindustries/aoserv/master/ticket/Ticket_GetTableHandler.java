/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.ticket;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.Ticket;
import com.aoindustries.aoserv.client.ticket.TicketType;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.TicketHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Ticket_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.TICKETS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.FETCH,
			new Ticket(),
			"select\n"
			+ "  id,\n"
			+ "  brand,\n"
			+ "  reseller,\n"
			+ "  accounting,\n"
			+ "  language,\n"
			+ "  created_by,\n"
			+ "  category,\n"
			+ "  ticket_type,\n"
			+ "  from_address,\n"
			+ "  summary,\n"
			+ "  open_date,\n"
			+ "  client_priority,\n"
			+ "  admin_priority,\n"
			+ "  status,\n"
			+ "  status_timeout,\n"
			+ "  contact_emails,\n"
			+ "  contact_phone_numbers\n"
			+ "from\n"
			+ "  ticket.\"Ticket\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		// AOServDaemon only needs access to its own open logs ticket.Ticket
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.FETCH,
			new Ticket(),
			"select\n"
			+ "  ti.id,\n"
			+ "  ti.brand,\n"
			+ "  ti.reseller,\n"
			+ "  ti.accounting,\n"
			+ "  ti.language,\n"
			+ "  ti.created_by,\n"
			+ "  ti.category,\n"
			+ "  ti.ticket_type,\n"
			+ "  ti.from_address,\n"
			+ "  ti.summary,\n"
			+ "  ti.open_date,\n"
			+ "  ti.client_priority,\n"
			+ "  ti.admin_priority,\n"
			+ "  ti.status,\n"
			+ "  ti.status_timeout,\n"
			+ "  ti.contact_emails,\n"
			+ "  ti.contact_phone_numbers\n"
			+ "from\n"
			//+ "  account.\"Username\" un,\n"
			//+ "  billing.\"Package\" pk,\n"
			+ "  ticket.\"Ticket\" ti\n"
			+ "where\n"
			+ "  ti.created_by=?\n"
			//+ "  un.username=?\n"
			//+ "  and un.package=pk.name\n"
			//+ "  and pk.accounting=ti.brand\n"
			//+ "  and pk.accounting=ti.accounting\n"
			+ "  and ti.status in (?,?,?)\n"
			+ "  and ti.ticket_type=?",
			source.getUsername(),
			Status.OPEN,
			Status.HOLD,
			Status.BOUNCED,
			TicketType.LOGS
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		if(TicketHandler.isTicketAdmin(conn, source)) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new Ticket(),
				"select distinct\n" // TODO: distinct required?
				+ "  ti.id,\n"
				+ "  ti.brand,\n"
				+ "  ti.reseller,\n"
				+ "  ti.accounting,\n"
				+ "  ti.language,\n"
				+ "  ti.created_by,\n"
				+ "  ti.category,\n"
				+ "  ti.ticket_type,\n"
				+ "  ti.from_address,\n"
				+ "  ti.summary,\n"
				+ "  ti.open_date,\n"
				+ "  ti.client_priority,\n"
				+ "  ti.admin_priority,\n"
				+ "  ti.status,\n"
				+ "  ti.status_timeout,\n"
				+ "  ti.contact_emails,\n"
				+ "  ti.contact_phone_numbers\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  ticket.\"Ticket\" ti\n"
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
				+ "  )",
				source.getUsername()
			);
		} else {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new Ticket(),
				"select\n"
				+ "  ti.id,\n"
				+ "  ti.brand,\n"
				+ "  null::text,\n" // reseller
				+ "  ti.accounting,\n"
				+ "  ti.language,\n"
				+ "  ti.created_by,\n"
				+ "  ti.category,\n"
				+ "  ti.ticket_type,\n"
				+ "  ti.from_address,\n"
				+ "  ti.summary,\n"
				+ "  ti.open_date,\n"
				+ "  ti.client_priority,\n"
				+ "  null,\n" // admin_priority
				+ "  ti.status,\n"
				+ "  ti.status_timeout,\n"
				+ "  ti.contact_emails,\n"
				+ "  ti.contact_phone_numbers\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  ticket.\"Ticket\" ti\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and bu1.accounting=ti.accounting\n"
				+ "  and ti.status not in (?,?)",
				source.getUsername(),
				Status.JUNK,
				Status.DELETED
			);
		}
	}
}
