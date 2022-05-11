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
import com.aoindustries.aoserv.client.billing.Currency;
import com.aoindustries.aoserv.client.billing.NoticeLog;
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
public class NoticeLog_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableId> getTableIds() {
    return EnumSet.of(Table.TableId.NOTICE_LOG);
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
    if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new NoticeLog(),
          "SELECT\n"
              + "  nl.*,\n"
              // Protocol compatibility
              + "  COALESCE(\n"
              + "    (\n"
              + "      SELECT\n"
              + "        \"balance.value\"\n"
              + "      FROM\n"
              + "        billing.\"NoticeLog.balance\" nlb\n"
              + "      WHERE\n"
              + "        nl.id = nlb.\"noticeLog\"\n"
              + "        AND nlb.\"balance.currency\" = ?\n"
              + "    ),\n"
              + "    '0.00'::numeric(9,2)\n"
              + "  ) AS balance\n"
              + "FROM\n"
              + "  billing.\"NoticeLog\" nl",
          Currency.USD.getCurrencyCode()
      );
    } else {
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new NoticeLog(),
          "SELECT\n"
              + "  *,\n"
              // Protocol compatibility
              + "  '0.00'::numeric(9,2) AS balance\n"
              + "FROM\n"
              + "  billing.\"NoticeLog\""
      );
    }
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
    AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
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
    if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new NoticeLog(),
          "SELECT\n"
              + "  nl.*,\n"
              // Protocol compatibility
              + "  COALESCE(\n"
              + "    (\n"
              + "      SELECT\n"
              + "        \"balance.value\"\n"
              + "      FROM\n"
              + "        billing.\"NoticeLog.balance\" nlb\n"
              + "      WHERE\n"
              + "        nl.id = nlb.\"noticeLog\"\n"
              + "        AND nlb.\"balance.currency\" = ?\n"
              + "    ),\n"
              + "    '0.00'::numeric(9,2)\n"
              + "  ) AS balance\n"
              + "FROM\n"
              + "  account.\"User\" un,\n"
              + "  billing.\"Package\" pk,\n"
              + TableHandler.BU1_PARENTS_JOIN
              + "  billing.\"NoticeLog\" nl\n"
              + "WHERE\n"
              + "  un.username = ?\n"
              + "  AND un.package = pk.name\n"
              + "  AND (\n"
              + TableHandler.PK_BU1_PARENTS_WHERE
              + "  )\n"
              + "  AND bu1.accounting = nl.accounting",
          Currency.USD.getCurrencyCode(),
          source.getCurrentAdministrator()
      );
    } else {
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new NoticeLog(),
          "SELECT\n"
              + "  nl.*,\n"
              // Protocol compatibility
              + "  '0.00'::numeric(9,2) AS balance\n"
              + "FROM\n"
              + "  account.\"User\" un,\n"
              + "  billing.\"Package\" pk,\n"
              + TableHandler.BU1_PARENTS_JOIN
              + "  billing.\"NoticeLog\" nl\n"
              + "WHERE\n"
              + "  un.username = ?\n"
              + "  AND un.package = pk.name\n"
              + "  AND (\n"
              + TableHandler.PK_BU1_PARENTS_WHERE
              + "  )\n"
              + "  AND bu1.accounting = nl.accounting",
          source.getCurrentAdministrator()
      );
    }
  }
}
