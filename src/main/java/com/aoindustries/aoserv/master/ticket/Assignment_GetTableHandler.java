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
import com.aoindustries.aoserv.client.ticket.Assignment;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.TicketHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Assignment_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.TICKET_ASSIGNMENTS);
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
        CursorMode.AUTO,
        new Assignment(),
        "select * from ticket.\"Assignment\""
    );
  }

  @Override
  protected void getTableDaemon(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId,
      User masterUser,
      UserHost[] masterServers
  ) throws IOException, SQLException {
    AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
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
      // Only ticket admin can see assignments
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new Assignment(),
          "select distinct\n" // TODO: distinct required?
              + "  ta.*\n"
              + "from\n"
              + "  account.\"User\" un,\n"
              + "  billing.\"Package\" pk1,\n"
              + TableHandler.BU1_PARENTS_JOIN
              + "  ticket.\"Ticket\" ti,\n"
              + "  ticket.\"Assignment\" ta\n"
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
          source.getCurrentAdministrator()
      );
    } else {
      // Non-admins don't get any assignment details
      AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
    }
  }
}
