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

package com.aoindustries.aoserv.master.mysql;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.mysql.Server;
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
public class Server_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.MYSQL_SERVERS);
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
        new Server(),
        "SELECT\n"
            + "  ms.*,\n"
            // Protocol conversion
            + "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
            + "FROM\n"
            + "  mysql.\"Server\" ms"
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
    AoservMaster.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new Server(),
        "SELECT\n"
            + "  ms.*,\n"
            // Protocol conversion
            + "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
            + "from\n"
            + "             master.\"UserHost\" uh\n"
            + "  INNER JOIN mysql.\"Server\"    ms ON uh.server=ms.ao_server\n"
            + "where\n"
            + "  uh.username=?",
        source.getCurrentAdministrator()
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
    AoservMaster.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new Server(),
        "SELECT\n"
            + "  ms.*,\n"
            // Protocol conversion
            + "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
            + "FROM\n"
            + "             account.\"User\"        un\n"
            + "  INNER JOIN billing.\"Package\"     pk ON un.package    = pk.name\n"
            + "  INNER JOIN account.\"AccountHost\" bs ON pk.accounting = bs.accounting\n"
            + "  INNER JOIN mysql.\"Server\"        ms ON bs.server     = ms.ao_server\n"
            + "WHERE\n"
            + "  un.username=?",
        source.getCurrentAdministrator()
    );
  }
}
