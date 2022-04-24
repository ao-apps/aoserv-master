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

package com.aoindustries.aoserv.master.net;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.Host;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Host_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.SERVERS);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new Host(),
        "select * from net.\"Host\""
    );
  }

  @Override
  protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new Host(),
        "select distinct\n"
            + "  se.*\n"
            + "from\n"
            + "  master.\"UserHost\" ms\n"
            + "  left join linux.\"Server\" ao on ms.server=ao.server\n"
            // Allow its failover parent
            + "  left join linux.\"Server\" ff on ao.failover_server=ff.server\n"
            // Allow its failover children
            + "  left join linux.\"Server\" fs on ao.server=fs.failover_server\n"
            // Allow servers it replicates to
            + "  left join backup.\"FileReplication\" ffr on ms.server=ffr.server\n"
            + "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
            + "  net.\"Host\" se\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            // Allow direct access
            + "    ms.server=se.id\n"
            // Allow its failover parent
            + "    or ff.server=se.id\n"
            // Allow its failover children
            + "    or fs.server=se.id\n"
            // Allow servers it replicates to
            + "    or bp.ao_server=se.id\n"
            + "  )",
        source.getCurrentAdministrator()
    );
  }

  @Override
  protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new Host(),
        "select distinct\n"
            + "  se.*\n"
            + "from\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk,\n"
            + "  account.\"AccountHost\" bs,\n"
            // Allow servers it replicates to
            //+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
            //+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
            + "  net.\"Host\" se\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk.name\n"
            + "  and pk.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=se.id\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=se.id\n"
            + "  )",
        source.getCurrentAdministrator()
    );
  }
}
