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
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.email.SpamMessage;
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
public final class SpamMessageHandler {

  /** Make no instances. */
  private SpamMessageHandler() {
    throw new AssertionError();
  }

  private static final String QUERY_MASTER =
      "select * from email.\"SpamMessage\"";

  public static class GetObject implements TableHandler.GetObjectHandler {

    @Override
    public Set<Table.TableId> getTableIds() {
      return EnumSet.of(Table.TableId.SPAM_EMAIL_MESSAGES);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void getObject(
        DatabaseConnection conn,
        RequestSource source,
        StreamableInput in,
        StreamableOutput out,
        Table.TableId tableId,
        User masterUser,
        UserHost[] masterServers
    ) throws IOException, SQLException {
      int spamMessage = in.readCompressedInt();
      if (masterUser != null && masterServers != null && masterServers.length == 0) {
        AoservMaster.writeObject(
            conn,
            source,
            out,
            new SpamMessage(),
            QUERY_MASTER + " where id=?",
            spamMessage
        );
      } else {
        out.writeByte(AoservProtocol.DONE);
      }
    }
  }

  public static class GetTable extends TableHandler.GetTableHandlerByRole {

    @Override
    public Set<Table.TableId> getTableIds() {
      return EnumSet.of(Table.TableId.SPAM_EMAIL_MESSAGES);
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
          CursorMode.FETCH,
          new SpamMessage(),
          QUERY_MASTER
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
      AoservMaster.writeObjects(source, out, provideProgress, Collections.emptyList());
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
}
