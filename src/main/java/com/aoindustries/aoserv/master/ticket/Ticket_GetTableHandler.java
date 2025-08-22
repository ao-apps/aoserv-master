/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021, 2022  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master.ticket;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.Ticket;
import com.aoindustries.aoserv.client.ticket.TicketType;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.TicketHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Ticket_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.TICKETS);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void getTableMaster(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId,
      User masterUser
  ) throws IOException, SQLException {
    AoservMaster.writeObjects(
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
  @SuppressWarnings("deprecation")
  protected void getTableDaemon(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId,
      User masterUser,
      UserHost[] masterServers
  ) throws IOException, SQLException {
    // AoservDaemon only needs access to its own open logs ticket.Ticket
    AoservMaster.writeObjects(
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
            // + "  account.\"User\" un,\n"
            // + "  billing.\"Package\" pk,\n"
            + "  ticket.\"Ticket\" ti\n"
            + "where\n"
            + "  ti.created_by=?\n"
            // + "  un.username=?\n"
            // + "  and un.package=pk.name\n"
            // + "  and pk.accounting=ti.brand\n"
            // + "  and pk.accounting=ti.accounting\n"
            + "  and ti.status in (?,?,?)\n"
            + "  and ti.ticket_type=?",
        source.getCurrentAdministrator(),
        Status.OPEN,
        Status.HOLD,
        Status.BOUNCED,
        TicketType.LOGS
    );
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void getTableAdministrator(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId
  ) throws IOException, SQLException {
    if (TicketHandler.isTicketAdmin(conn, source)) {
      AoservMaster.writeObjects(
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
              + "  account.\"User\" un,\n"
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
          source.getCurrentAdministrator()
      );
    } else {
      AoservMaster.writeObjects(
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
              + "  account.\"User\" un,\n"
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
          source.getCurrentAdministrator(),
          Status.JUNK,
          Status.DELETED
      );
    }
  }
}
