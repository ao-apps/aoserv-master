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

package com.aoindustries.aoserv.master.email;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.email.InboxAddress;
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
public class InboxAddress_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.LINUX_ACC_ADDRESSES);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.AUTO,
      new InboxAddress(),
      "SELECT\n"
      + "  ia.*,\n"
      // Protocol conversion <= 1.30:
      + "  us.username\n"
      + "FROM\n"
      + "  email.\"InboxAddress\" ia\n"
      + "  INNER JOIN linux.\"UserServer\" us ON ia.linux_server_account = us.id"
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
      new InboxAddress(),
      "SELECT\n"
      + "  ia.*,\n"
      // Protocol conversion <= 1.30:
      + "  us.username\n"
      + "FROM\n"
      + "  master.\"UserHost\"    ms,\n"
      + "  email.\"Domain\"       ed,\n"
      + "  email.\"Address\"      ea,\n"
      + "  email.\"InboxAddress\" ia,\n"
      + "  linux.\"UserServer\"   us\n"
      + "WHERE\n"
      + "      ms.username             = ?\n"
      + "  AND ms.server               = ed.ao_server\n"
      + "  AND ed.id                   = ea.domain\n"
      + "  AND ea.id                   = ia.email_address\n"
      + "  AND ia.linux_server_account = us.id",
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
      new InboxAddress(),
      "SELECT\n"
      + "  ia.*,\n"
      // Protocol conversion <= 1.30:
      + "  us.username\n"
      + "FROM\n"
      + "  account.\"User\"       un,\n"
      + "  billing.\"Package\"    pk1,\n"
      + TableHandler.BU1_PARENTS_JOIN
      + "  billing.\"Package\"    pk2,\n"
      + "  email.\"Domain\"       ed,\n"
      + "  email.\"Address\"      ea,\n"
      + "  email.\"InboxAddress\" ia,\n"
      + "  linux.\"UserServer\"   us\n"
      + "WHERE\n"
      + "      un.username             = ?\n"
      + "  AND un.package              = pk1.name\n"
      + "  AND (\n"
      + TableHandler.PK1_BU1_PARENTS_WHERE
      + "  )\n"
      + "  AND bu1.accounting          = pk2.accounting\n"
      + "  AND pk2.name                = ed.package\n"
      + "  AND ed.id                   = ea.domain\n"
      + "  AND ea.id                   = ia.email_address\n"
      + "  AND ia.linux_server_account = us.id",
      source.getCurrentAdministrator()
    );
  }
}
