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

package com.aoindustries.aoserv.master.account;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.account.Account;
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
public class Account_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.BUSINESSES);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.AUTO,
      new Account(),
      "select * from account.\"Account\""
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
      new Account(),
      "select distinct\n"
      + "  bu.*\n"
      + "from\n"
      + "  master.\"UserHost\" ms,\n"
      + "  account.\"AccountHost\" bs,\n"
      + "  account.\"Account\" bu\n"
      + "where\n"
      + "  ms.username=?\n"
      + "  and ms.server=bs.server\n"
      + "  and bs.accounting=bu.accounting",
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
      new Account(),
      "WITH RECURSIVE accounts(accounting) AS (\n"
      + "  SELECT\n"
      + "    ac.*\n"
      + "  FROM\n"
      + "               account.\"User\"    un\n"
      + "    INNER JOIN billing.\"Package\" pk ON un.package    = pk.name\n"
      + "    INNER JOIN account.\"Account\" ac ON pk.accounting = ac.accounting\n"
      + "  WHERE\n"
      + "    un.username=?\n"
      + "UNION ALL\n"
      + "  SELECT a.* FROM\n"
      + "    accounts\n"
      + "    INNER JOIN account.\"Account\" a ON accounts.accounting = a.parent\n"
      + ")\n"
      + "SELECT * FROM accounts",
      source.getCurrentAdministrator()
    );
    /*
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      new Account(),
      "select\n"
      + "  bu1.*\n"
      + "from\n"
      + "  account.\"User\" un,\n"
      + "  billing.\"Package\" pk,\n"
      + TableHandler.BU1_PARENTS_JOIN_NO_COMMA
      + "where\n"
      + "  un.username=?\n"
      + "  and un.package=pk.name\n"
      + "  and (\n"
      + TableHandler.PK_BU1_PARENTS_WHERE
      + "  )",
      source.getUsername()
    );
     */
  }
}
