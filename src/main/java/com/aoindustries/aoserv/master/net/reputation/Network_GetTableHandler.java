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

package com.aoindustries.aoserv.master.net.reputation;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.reputation.Network;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
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
public class Network_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.IP_REPUTATION_SET_NETWORKS);
  }

  /**
   * Admin may access all sets.
   */
  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.FETCH,
      new Network(),
      "select * from \"net.reputation\".\"Network\""
    );
  }

  /**
   * Router may access all sets used by any limiters in the same server farm.
   * Non-router daemon may not access any reputation sets.
   *
   * @see  User#isRouter()
   */
  @Override
  protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
    if (masterUser.isRouter()) {
      MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.FETCH,
        new Network(),
        "select distinct\n"
        + "  irsn.*\n"
        + "from\n"
        + "  master.\"UserHost\"                          ms\n"
        + "  inner join net.\"Host\"                      se   on  ms.server   =   se.id\n"         // Find all servers can access
        + "  inner join net.\"Host\"                      se2  on  se.farm     =  se2.farm\n"       // Find all servers in the same farm
        + "  inner join net.\"Device\"                    nd   on se2.id       =   nd.server\n"     // Find all net.Device in the same farm
        + "  inner join \"net.reputation\".\"Limiter\"    irl  on  nd.id       =  irl.net_device\n" // Find all limiters in the same farm
        + "  inner join \"net.reputation\".\"LimiterSet\" irls on  irl.id      = irls.limiter\n"    // Find all sets used by all limiters in the same farm
        + "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\" =  irs.id\n"         // Find all sets used by any limiter in the same farm
        + "  inner join \"net.reputation\".\"Network\"    irsn on  irs.id      = irsn.\"set\"\n"    // Find all networks belonging to these sets
        + "where\n"
        + "  ms.username=?",
        source.getCurrentAdministrator()
      );
    } else {
      MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
    }
  }

  /**
   * Regular user may only access the networks for their own or subaccount sets.
   */
  @Override
  protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.FETCH,
      new Network(),
      "select\n"
      + "  irsn.*\n"
      + "from\n"
      + "  account.\"User\" un,\n"
      + "  billing.\"Package\" pk,\n"
      + TableHandler.BU1_PARENTS_JOIN
      + "  \"net.reputation\".\"Set\" irs,\n"
      + "  \"net.reputation\".\"Network\" irsn\n"
      + "where\n"
      + "  un.username=?\n"
      + "  and un.package=pk.name\n"
      + "  and (\n"
      + TableHandler.PK_BU1_PARENTS_WHERE
      + "  )\n"
      + "  and bu1.accounting=irs.accounting\n"
      + "  and irs.id=irsn.\"set\"",
      source.getCurrentAdministrator()
    );
  }
}
