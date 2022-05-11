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

package com.aoindustries.aoserv.master.aosh;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.aosh.Command;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Command_GetTableHandler extends TableHandler.GetTableHandlerPublic {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.AOSH_COMMANDS);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void getTablePublic(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId
  ) throws IOException, SQLException {
    AoservMaster.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.SELECT,
        new Command(),
        "select\n"
            + "  ac.command,\n"
            + "  ac.\"sinceVersion\",\n"
            + "  ac.\"lastVersion\",\n"
            + "  st.\"name\" as \"table\",\n"
            + "  ac.description,\n"
            + "  ac.syntax\n"
            + "from\n"
            + "  \"schema\".\"AoservProtocol\" client_ap,\n"
            + "                   aosh.\"Command\"              ac\n"
            + "  inner join \"schema\".\"AoservProtocol\" since_ap on ac.\"sinceVersion\" = since_ap.version\n"
            + "  left  join \"schema\".\"AoservProtocol\"  last_ap on ac.\"lastVersion\"  =  last_ap.version\n"
            + "  left  join \"schema\".\"Table\"                st on ac.\"table\"        =       st.id\n"
            + "where\n"
            + "  client_ap.version=?\n"
            + "  and client_ap.created >= since_ap.created\n"
            + "  and (\n"
            + "    last_ap.created is null\n"
            + "    or client_ap.created <= last_ap.created\n"
            + "  )",
        source.getProtocolVersion().getVersion()
    );
  }
}
