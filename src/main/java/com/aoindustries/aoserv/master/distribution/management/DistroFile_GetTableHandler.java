/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.distribution.management;

import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.distribution.management.DistroFile;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class DistroFile_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.DISTRO_FILES);
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
    if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107) <= 0) {
      AoservMaster.writeObjects(source, out, false, Collections.emptyList());
    } else {
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.FETCH,
          new DistroFile(),
          "select * from \"distribution.management\".\"DistroFile\""
      );
    }
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
    // Restrict to the operating system versions accessible to this user
    IntList osVersions = TableHandler.getOperatingSystemVersions(conn, source);
    if (osVersions.isEmpty()) {
      AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
    } else {
      StringBuilder sql = new StringBuilder();
      sql.append("select * from \"distribution.management\".\"DistroFile\" where operating_system_version in (");
      for (int i = 0; i < osVersions.size(); i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append(osVersions.getInt(i));
      }
      sql.append(')');
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.FETCH,
          new DistroFile(),
          sql.toString()
      );
    }
  }

  @Override
  protected void getTableAdministrator(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId
  ) throws IOException, SQLException {
    AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
  }
}
