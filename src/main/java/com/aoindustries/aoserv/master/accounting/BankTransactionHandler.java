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

package com.aoindustries.aoserv.master.accounting;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.accounting.BankTransaction;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.BankAccountHandler;
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
public final class BankTransactionHandler {

  /** Make no instances. */
  private BankTransactionHandler() {
    throw new AssertionError();
  }

  private static final String QUERY_ACCOUNTING =
      "select\n"
          + "  id,\n"
          + "  time,\n"
          + "  account,\n"
          + "  processor,\n"
          + "  administrator,\n"
          + "  type,\n"
          + "  \"expenseCategory\",\n"
          + "  description,\n"
          + "  \"checkNo\",\n"
          + "  amount,\n"
          + "  confirmed\n"
          + "from\n"
          + "  accounting.\"BankTransaction\"";

  public static class GetObject implements TableHandler.GetObjectHandler {

    @Override
    public Set<Table.TableID> getTableIds() {
      return EnumSet.of(Table.TableID.BANK_TRANSACTIONS);
    }

    @Override
    public void getObject(DatabaseConnection conn, RequestSource source, StreamableInput in, StreamableOutput out, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
      int bankTransaction = in.readCompressedInt();
      if (BankAccountHandler.isBankAccounting(conn, source)) {
        MasterServer.writeObject(
            conn,
            source,
            out,
            new BankTransaction(),
            QUERY_ACCOUNTING + "\n"
                + "where\n"
                + "  id=?",
            bankTransaction
        );
      } else {
        out.writeByte(AoservProtocol.DONE);
      }
    }
  }

  public static class GetTable implements GetTableHandlerAccountingOnly {

    @Override
    public Set<Table.TableID> getTableIds() {
      return EnumSet.of(Table.TableID.BANK_TRANSACTIONS);
    }

    @Override
    public void getTableAccounting(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
      MasterServer.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.FETCH,
          new BankTransaction(),
          QUERY_ACCOUNTING
      );
    }
  }
}
