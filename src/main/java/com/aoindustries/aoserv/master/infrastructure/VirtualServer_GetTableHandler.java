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
import com.aoindustries.aoserv.client.infrastructure.VirtualServer;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
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
public class VirtualServer_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.VIRTUAL_SERVERS);
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
        new VirtualServer(),
        "select * from infrastructure.\"VirtualServer\""
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
        new VirtualServer(),
        "select distinct\n"
            + "  vs.*\n"
            + "from\n"
            + "  master.\"UserHost\" ms\n"
            + "  inner join infrastructure.\"VirtualServer\" vs on ms.server=vs.server\n"
            + "where\n"
            + "  ms.username=?",
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
        new VirtualServer(),
        "select distinct\n"
            + "  vs.server,\n"
            + "  vs.primary_ram,\n"
            + "  vs.primary_ram_target,\n"
            + "  vs.secondary_ram,\n"
            + "  vs.secondary_ram_target,\n"
            + "  vs.minimum_processor_type,\n"
            + "  vs.minimum_processor_architecture,\n"
            + "  vs.minimum_processor_speed,\n"
            + "  vs.minimum_processor_speed_target,\n"
            + "  vs.processor_cores,\n"
            + "  vs.processor_cores_target,\n"
            + "  vs.processor_weight,\n"
            + "  vs.processor_weight_target,\n"
            + "  vs.primary_physical_server_locked,\n"
            + "  vs.secondary_physical_server_locked,\n"
            + "  vs.requires_hvm,\n"
            + "  case\n"
            + "    when vs.vnc_password is null then null\n"
            // Only provide the password when the user can connect to VNC console
            + "    when (\n"
            + "      select bs2.id from account.\"AccountHost\" bs2 where bs2.accounting=pk.accounting and bs2.server=vs.server and bs2.can_vnc_console limit 1\n"
            + "    ) is not null then vs.vnc_password\n"
            + "    else ?\n"
            + "  end\n"
            + "from\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk,\n"
            + "  account.\"AccountHost\" bs,\n"
            // Allow servers it replicates to
            // + "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
            // + "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
            + "  infrastructure.\"VirtualServer\" vs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk.name\n"
            + "  and pk.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=vs.server\n"
            // Allow servers it replicates to
            // + "    or bp.ao_server=vs.server\n"
            + "  )",
        AoservProtocol.FILTERED,
        source.getCurrentAdministrator()
    );
  }
}
