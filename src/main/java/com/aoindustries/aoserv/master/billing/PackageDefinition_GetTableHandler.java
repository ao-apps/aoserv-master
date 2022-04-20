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

package com.aoindustries.aoserv.master.billing;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.billing.PackageDefinition;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AccountHandler;
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
public class PackageDefinition_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.PACKAGE_DEFINITIONS);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
      conn,
      source,
      out,
      provideProgress,
      CursorMode.AUTO,
      new PackageDefinition(),
      "select * from billing.\"PackageDefinition\""
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
      new PackageDefinition(),
      "select distinct\n"
      + "  pd.*\n"
      + "from\n"
      + "  master.\"UserHost\" ms,\n"
      + "  account.\"AccountHost\" bs,\n"
      + "  billing.\"Package\" pk,\n"
      + "  billing.\"PackageDefinition\" pd\n"
      + "where\n"
      + "  ms.username=?\n"
      + "  and ms.server=bs.server\n"
      + "  and bs.accounting=pk.accounting\n"
      + "  and pk.package_definition=pd.id",
      source.getCurrentAdministrator()
    );
  }

  @Override
  protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
    if (AccountHandler.canSeePrices(conn, source)) {
      MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new PackageDefinition(),
        "select distinct\n"
        + "  pd.*\n"
        + "from\n"
        + "  account.\"User\" un,\n"
        + "  billing.\"Package\" pk1,\n"
        + TableHandler.BU1_PARENTS_JOIN
        + "  billing.\"Package\" pk2,\n"
        + "  billing.\"PackageDefinition\" pd\n"
        + "where\n"
        + "  un.username=?\n"
        + "  and un.package=pk1.name\n"
        + "  and (\n"
        + TableHandler.PK1_BU1_PARENTS_WHERE
        + "  )\n"
        + "  and bu1.accounting=pk2.accounting\n"
        + "  and (\n"
        + "    pk2.package_definition=pd.id\n"
        + "    or bu1.accounting=pd.accounting\n"
        + "  )",
        source.getCurrentAdministrator()
      );
    } else {
      MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new PackageDefinition(),
        "select distinct\n"
        + "  pd.id,\n"
        + "  pd.accounting,\n"
        + "  pd.category,\n"
        + "  pd.\"name\",\n"
        + "  pd.version,\n"
        + "  pd.display,\n"
        + "  pd.description,\n"
        + "  null as \"setupFee.currency\",\n"
        + "  null as \"setupFee.value\",\n"
        + "  null as setup_fee_transaction_type,\n"
        + "  null as \"monthlyRate.currency\",\n"
        + "  null as \"monthlyRate.value\",\n"
        + "  null as monthly_rate_transaction_type,\n"
        + "  pd.active,\n"
        + "  pd.approved\n"
        + "from\n"
        + "  account.\"User\" un,\n"
        + "  billing.\"Package\" pk1,\n"
        + TableHandler.BU1_PARENTS_JOIN
        + "  billing.\"Package\" pk2,\n"
        + "  billing.\"PackageDefinition\" pd\n"
        + "where\n"
        + "  un.username=?\n"
        + "  and un.package=pk1.name\n"
        + "  and (\n"
        + TableHandler.PK1_BU1_PARENTS_WHERE
        + "  )\n"
        + "  and bu1.accounting=pk2.accounting\n"
        + "  and (\n"
        + "    pk2.package_definition=pd.id\n"
        + "    or bu1.accounting=pd.accounting\n"
        + "  )",
        source.getCurrentAdministrator()
      );
    }
  }
}
