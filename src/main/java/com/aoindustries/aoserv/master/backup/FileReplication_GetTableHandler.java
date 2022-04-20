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

package com.aoindustries.aoserv.master.backup;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.backup.FileReplication;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
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
public class FileReplication_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.FAILOVER_FILE_REPLICATIONS);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.AUTO,
      new FileReplication(),
      "select\n"
      + "  id,\n"
      + "  server,\n"
      + "  backup_partition,\n"
      + "  max_bit_rate,\n"
      + "  use_compression,\n"
      + "  retention,\n"
      + "  connect_address,\n"
      + "  host(connect_from) as connect_from,\n"
      + "  enabled,\n"
      + "  quota_gid\n"
      + "from\n"
      + "  backup.\"FileReplication\""
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
      new FileReplication(),
      "select\n"
      + "  ffr.id,\n"
      + "  ffr.server,\n"
      + "  ffr.backup_partition,\n"
      + "  ffr.max_bit_rate,\n"
      + "  ffr.use_compression,\n"
      + "  ffr.retention,\n"
      + "  ffr.connect_address,\n"
      + "  host(ffr.connect_from) as connect_from,\n"
      + "  ffr.enabled,\n"
      + "  ffr.quota_gid\n"
      + "from\n"
      + "  master.\"UserHost\" ms,\n"
      + "  backup.\"FileReplication\" ffr\n"
      + "where\n"
      + "  ms.username=?\n"
      + "  and ms.server=ffr.server",
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
      new FileReplication(),
      "select\n"
      + "  ffr.id,\n"
      + "  ffr.server,\n"
      + "  ffr.backup_partition,\n"
      + "  ffr.max_bit_rate,\n"
      + "  ffr.use_compression,\n"
      + "  ffr.retention,\n"
      + "  ffr.connect_address,\n"
      + "  host(ffr.connect_from) as connect_from,\n"
      + "  ffr.enabled,\n"
      + "  ffr.quota_gid\n"
      + "from\n"
      + "  account.\"User\" un,\n"
      + "  billing.\"Package\" pk,\n"
      + "  account.\"AccountHost\" bs,\n"
      + "  backup.\"FileReplication\" ffr\n"
      + "where\n"
      + "  un.username=?\n"
      + "  and un.package=pk.name\n"
      + "  and pk.accounting=bs.accounting\n"
      + "  and bs.server=ffr.server",
      source.getCurrentAdministrator()
    );
  }
}
