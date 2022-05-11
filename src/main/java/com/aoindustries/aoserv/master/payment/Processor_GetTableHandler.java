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

package com.aoindustries.aoserv.master.payment;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.payment.Processor;
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
public class Processor_GetTableHandler extends TableHandler.GetTableHandlerPermissionByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.CREDIT_CARD_PROCESSORS);
  }

  @Override
  protected Permission.Name getPermissionName() {
    return Permission.Name.get_credit_card_processors;
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void getTableMasterHasPermission(
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
        new Processor(),
        "select * from payment.\"Processor\""
    );
  }

  @Override
  protected void getTableDaemonHasPermission(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      Table.TableId tableId,
      User masterUser,
      UserHost[] masterServers
  ) throws IOException, SQLException {
    AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void getTableAdministratorHasPermission(
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
        new Processor(),
        "select\n"
            + "  ccp.*\n"
            + "from\n"
            + "  account.\"User\" un,\n"
            + "  billing.\"Package\" pk,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "  payment.\"Processor\" ccp\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk.name\n"
            + "  and (\n"
            + TableHandler.PK_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=ccp.accounting",
        source.getCurrentAdministrator()
    );
  }
}
