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

package com.aoindustries.aoserv.master.infrastructure;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.infrastructure.ServerFarm;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
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
public class ServerFarm_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.SERVER_FARMS);
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
        CursorMode.SELECT,
        new ServerFarm(),
        "select * from infrastructure.\"ServerFarm\""
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
        CursorMode.SELECT,
        new ServerFarm(),
        "select distinct\n"
            + "  sf.*\n"
            + "from\n"
            + "  master.\"UserHost\" ms,\n"
            + "            net.\"Host\"          se\n"
            + "  left join backup.\"FileReplication\" ffr on  se.id             = ffr.server\n"
            + "  left join backup.\"BackupPartition\" bp  on ffr.backup_partition =  bp.id\n"
            + "  left join net.\"Host\"          fs  on  bp.ao_server        =  fs.id,\n"
            + "  infrastructure.\"ServerFarm\" sf\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=se.id\n"
            + "  and (\n"
            + "    se.farm=sf.name\n"
            + "    or fs.farm=sf.name\n"
            + "  )",
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
        CursorMode.SELECT,
        new ServerFarm(),
        "select distinct\n"
            + "  sf.*\n"
            + "from\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk,\n"
            + "  account.\"AccountHost\" bs,\n"
            + "  net.\"Host\" se,\n"
            + "  infrastructure.\"ServerFarm\" sf\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk.name\n"
            + "  and (\n"
            + "    (\n"
            + "      pk.accounting=bs.accounting\n"
            + "      and bs.server=se.id\n"
            + "      and se.farm=sf.name\n"
            + "    ) or pk.id=sf.owner\n"
            + "  )",
        source.getCurrentAdministrator()
    );
  }
}
