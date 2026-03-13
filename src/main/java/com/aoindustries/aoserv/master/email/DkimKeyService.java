/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2026  AO Industries, Inc.
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
import com.aoindustries.aoserv.client.email.DkimKey;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.AoservMaster;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class DkimKeyService implements MasterService {

  // <editor-fold desc="GetTableHandler" defaultstate="collapsed">
  @Override
  public TableHandler.GetTableHandler startGetTableHandler() {
    return new TableHandler.GetTableHandlerByRole() {
      @Override
      public Set<Table.TableId> getTableIds() {
        return EnumSet.of(Table.TableId.email_DkimKey);
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
        AoservMaster.writeObjects(
            conn,
            source,
            out,
            provideProgress,
            CursorMode.AUTO,
            new DkimKey(),
            "SELECT * FROM email.\"DkimKey\""
        );
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
        AoservMaster.writeObjects(
            conn,
            source,
            out,
            provideProgress,
            CursorMode.AUTO,
            new DkimKey(),
            "SELECT\n"
                + "  dk.*\n"
                + "FROM\n"
                + "             master.\"UserHost\" ms\n"
                + "  INNER JOIN email.\"Domain\"    ed ON ms.server = ed.ao_server\n"
                + "  INNER JOIN email.\"DkimKey\"   dk ON ed.id     = dk.\"domain\"\n"
                + "WHERE\n"
                + "  ms.username = ?",
            source.getCurrentAdministrator()
        );
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
        AoservMaster.writeObjects(
            conn,
            source,
            out,
            provideProgress,
            CursorMode.AUTO,
            new DkimKey(),
            "SELECT\n"
                + "  dk.*\n"
                + "FROM\n"
                + "             account.\"User\"    un\n"
                + "  INNER JOIN billing.\"Package\" pk1 ON un.package     = pk1.name,\n"
                + TableHandler.BU1_PARENTS_JOIN_NO_COMMA
                + "  INNER JOIN billing.\"Package\" pk2 ON bu1.accounting = pk2.accounting\n"
                + "  INNER JOIN email.\"Domain\"    ed  ON pk2.name       = ed.package\n"
                + "  INNER JOIN email.\"DkimKey\"   dk  ON ed.id          = dk.\"domain\"\n"
                + "WHERE\n"
                + "  un.username = ?\n"
                + "  AND (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "  )",
            source.getCurrentAdministrator()
        );
      }
    };
  }
  // </editor-fold>
}
