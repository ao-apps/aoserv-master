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
import com.aoindustries.aoserv.client.mysql.UserServer;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
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
public class UserServer_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.MYSQL_SERVER_USERS);
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
        new UserServer(),
        "select * from mysql.\"UserServer\""
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
        new UserServer(),
        "select\n"
            + "  msu.*\n"
            + "from\n"
            + "  master.\"UserHost\" ms,\n"
            + "  mysql.\"Server\" mys,\n"
            + "  mysql.\"UserServer\" msu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mys.ao_server\n"
            + "  and mys.bind=msu.mysql_server",
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
        new UserServer(),
        "select\n"
            + "  msu.id,\n"
            + "  msu.username,\n"
            + "  msu.mysql_server,\n"
            + "  msu.host,\n"
            + "  msu.disable_log,\n"
            + "  case when msu.predisable_password is null then null else ? end,\n"
            + "  msu.max_questions,\n"
            + "  msu.max_updates\n,"
            + "  msu.max_connections,\n"
            + "  msu.max_user_connections\n"
            + "from\n"
            + "  account.\"User\" un1,\n"
            + "  billing.\"Package\" pk1,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "  billing.\"Package\" pk2,\n"
            + "  account.\"User\" un2,\n"
            + "  mysql.\"UserServer\" msu\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and un1.package=pk1.name\n"
            + "  and (\n"
            + TableHandler.PK1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pk2.accounting\n"
            + "  and pk2.name=un2.package\n"
            + "  and un2.username=msu.username",
        AoservProtocol.FILTERED,
        source.getCurrentAdministrator()
    );
  }
}
