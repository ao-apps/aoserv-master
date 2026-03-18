/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021, 2022, 2026  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AccountUserHandler;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author  AO Industries, Inc.
 */
public class Process_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.MASTER_PROCESSES);
  }

  @Override
  protected void getTableMaster(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId,
      User masterUser
  ) throws IOException, SQLException {
    AoservMaster.writeObjectsSynced(
        source,
        out,
        provideProgress,
        Process_Manager.getSnapshot()
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
    // Filter all connections that match the source admin with the effective user
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    List<Process> filtered = Process_Manager.getSnapshot().stream()
        .filter(process -> currentAdministrator.equals(process.getEffectiveAdministrator_username()))
        .collect(Collectors.toList()); // Java 16: toList();
    AoservMaster.writeObjectsSynced(source, out, provideProgress, filtered);
  }

  @Override
  protected void getTableAdministrator(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId
  ) throws IOException, SQLException {
    List<Process> processesCopy = Process_Manager.getSnapshot();
    List<Process> filtered = new ArrayList<>(processesCopy.size());
    for (Process process : processesCopy) {
      // Filter all connections that can access the effective user
      com.aoindustries.aoserv.client.account.User.Name effectiveUser = process.getEffectiveAdministrator_username();
      if (
          effectiveUser != null
              && AccountUserHandler.canAccessUser(conn, source, effectiveUser)
      ) {
        filtered.add(process);
      }
    }
    AoservMaster.writeObjectsSynced(source, out, provideProgress, filtered);
  }
}
