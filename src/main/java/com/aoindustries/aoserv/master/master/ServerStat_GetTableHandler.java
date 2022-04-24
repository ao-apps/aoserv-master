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

package com.aoindustries.aoserv.master.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.FifoFile;
import com.aoapps.hodgepodge.io.FifoFileInputStream;
import com.aoapps.hodgepodge.io.FifoFileOutputStream;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.hodgepodge.util.ThreadUtility;
import com.aoapps.lang.Strings;
import com.aoapps.lang.util.BufferManager;
import com.aoapps.sql.pool.AOConnectionPool;
import com.aoindustries.aoserv.client.master.ServerStat;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.DaemonHandler;
import com.aoindustries.aoserv.master.MasterDatabase;
import static com.aoindustries.aoserv.master.MasterServer.getRequestConcurrency;
import static com.aoindustries.aoserv.master.MasterServer.getRequestConnections;
import static com.aoindustries.aoserv.master.MasterServer.getRequestMaxConcurrency;
import static com.aoindustries.aoserv.master.MasterServer.getRequestTotalTime;
import static com.aoindustries.aoserv.master.MasterServer.getRequestTransactions;
import static com.aoindustries.aoserv.master.MasterServer.getStartTime;
import static com.aoindustries.aoserv.master.MasterServer.writeObjects;
import com.aoindustries.aoserv.master.RandomHandler;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author  AO Industries, Inc.
 */
public class ServerStat_GetTableHandler extends TableHandler.GetTableHandlerPublic {

  private static final Logger logger = Logger.getLogger(ServerStat_GetTableHandler.class.getName());

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.MASTER_SERVER_STATS);
  }

  private static String trim(String inStr) {
    return (inStr == null) ? null : inStr.trim();
  }

  private static void addStat(
      List<ServerStat> objs,
      String name,
      String value,
      String description
  ) {
    objs.add(
        new ServerStat(
            trim(name),
            trim(value),
            trim(description)
        )
    );
  }

  @Override
  protected void getTablePublic(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
    List<ServerStat> objs;
    try {
      // Create the list of objects first
      objs = new ArrayList<>();
      addStat(objs, ServerStat.BYTE_ARRAY_CACHE_CREATES, Long.toString(BufferManager.getByteBufferCreates()), "Number of byte[] buffers created");
      addStat(objs, ServerStat.BYTE_ARRAY_CACHE_USES, Long.toString(BufferManager.getByteBufferUses()), "Total number of byte[] buffers allocated");
      if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
        addStat(objs, ServerStat.BYTE_ARRAY_CACHE_ZERO_FILLS, Long.toString(BufferManager.getByteBufferZeroFills()), "Total number of byte[] buffers zero-filled");
        addStat(objs, ServerStat.BYTE_ARRAY_CACHE_COLLECTED, Long.toString(BufferManager.getByteBuffersCollected()), "Total number of byte[] buffers detected as garbage collected");
      }

      addStat(objs, ServerStat.CHAR_ARRAY_CACHE_CREATES, Long.toString(BufferManager.getCharBufferCreates()), "Number of char[] buffers created");
      addStat(objs, ServerStat.CHAR_ARRAY_CACHE_USES, Long.toString(BufferManager.getCharBufferUses()), "Total number of char[] buffers allocated");
      if (source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
        addStat(objs, ServerStat.CHAR_ARRAY_CACHE_ZERO_FILLS, Long.toString(BufferManager.getCharBufferZeroFills()), "Total number of char[] buffers zero-filled");
        addStat(objs, ServerStat.CHAR_ARRAY_CACHE_COLLECTED, Long.toString(BufferManager.getCharBuffersCollected()), "Total number of char[] buffers detected as garbage collected");
      }

      addStat(objs, ServerStat.DAEMON_CONCURRENCY, Integer.toString(DaemonHandler.getDaemonConcurrency()), "Number of active daemon connections");
      addStat(objs, ServerStat.DAEMON_CONNECTIONS, Integer.toString(DaemonHandler.getDaemonConnections()), "Current number of daemon connections");
      addStat(objs, ServerStat.DAEMON_CONNECTS, Integer.toString(DaemonHandler.getDaemonConnects()), "Number of times connecting to daemons");
      addStat(objs, ServerStat.DAEMON_COUNT, Integer.toString(DaemonHandler.getDaemonCount()), "Number of daemons that have been accessed");
      addStat(objs, ServerStat.DAEMON_DOWN_COUNT, Integer.toString(DaemonHandler.getDownDaemonCount()), "Number of daemons that are currently unavailable");
      addStat(objs, ServerStat.DAEMON_MAX_CONCURRENCY, Integer.toString(DaemonHandler.getDaemonMaxConcurrency()), "Peak number of active daemon connections");
      addStat(objs, ServerStat.DAEMON_POOL_SIZE, Integer.toString(DaemonHandler.getDaemonPoolSize()), "Maximum number of daemon connections");
      addStat(objs, ServerStat.DAEMON_TOTAL_TIME, Strings.getDecimalTimeLengthString(DaemonHandler.getDaemonTotalTime()), "Total time spent accessing daemons");
      addStat(objs, ServerStat.DAEMON_TRANSACTIONS, Long.toString(DaemonHandler.getDaemonTransactions()), "Number of transactions processed by daemons");

      AOConnectionPool dbPool = MasterDatabase.getDatabase().getConnectionPool();
      addStat(objs, ServerStat.DB_CONCURRENCY, Integer.toString(dbPool.getConcurrency()), "Number of active database connections");
      addStat(objs, ServerStat.DB_CONNECTIONS, Integer.toString(dbPool.getConnectionCount()), "Current number of database connections");
      addStat(objs, ServerStat.DB_CONNECTS, Long.toString(dbPool.getConnects()), "Number of times connecting to the database");
      addStat(objs, ServerStat.DB_MAX_CONCURRENCY, Integer.toString(dbPool.getMaxConcurrency()), "Peak number of active database connections");
      addStat(objs, ServerStat.DB_POOL_SIZE, Integer.toString(dbPool.getPoolSize()), "Maximum number of database connections");
      addStat(objs, ServerStat.DB_TOTAL_TIME, Strings.getDecimalTimeLengthString(dbPool.getTotalTime()), "Total time spent accessing the database");
      addStat(objs, ServerStat.DB_TRANSACTIONS, Long.toString(dbPool.getTransactionCount()), "Number of transactions committed by the database");

      FifoFile entropyFile = RandomHandler.getFifoFile();
      addStat(objs, ServerStat.ENTROPY_AVAIL, Long.toString(entropyFile.getLength()), "Number of bytes of entropy currently available");
      addStat(objs, ServerStat.ENTROPY_POOLSIZE, Long.toString(entropyFile.getMaximumFifoLength()), "Maximum number of bytes of entropy");
      FifoFileInputStream entropyIn = entropyFile.getInputStream();
      addStat(objs, ServerStat.ENTROPY_READ_BYTES, Long.toString(entropyIn.getReadBytes()), "Number of bytes read from the entropy pool");
      addStat(objs, ServerStat.ENTROPY_READ_COUNT, Long.toString(entropyIn.getReadCount()), "Number of reads from the entropy pool");
      FifoFileOutputStream entropyOut = entropyFile.getOutputStream();
      addStat(objs, ServerStat.ENTROPY_WRITE_BYTES, Long.toString(entropyOut.getWriteBytes()), "Number of bytes written to the entropy pool");
      addStat(objs, ServerStat.ENTROPY_WRITE_COUNT, Long.toString(entropyOut.getWriteCount()), "Number of writes to the entropy pool");

      addStat(objs, ServerStat.MEMORY_FREE, Long.toString(Runtime.getRuntime().freeMemory()), "Free virtual machine memory in bytes");
      addStat(objs, ServerStat.MEMORY_TOTAL, Long.toString(Runtime.getRuntime().totalMemory()), "Total virtual machine memory in bytes");

      addStat(objs, ServerStat.PROTOCOL_VERSION, Strings.join(AoservProtocol.Version.values(), "\n"), "Supported AoservProtocol version numbers");

      addStat(objs, ServerStat.REQUEST_CONCURRENCY, Integer.toString(getRequestConcurrency()), "Current number of client requests being processed");
      addStat(objs, ServerStat.REQUEST_CONNECTIONS, Long.toString(getRequestConnections()), "Number of connections received from clients");
      addStat(objs, ServerStat.REQUEST_MAX_CONCURRENCY, Integer.toString(getRequestMaxConcurrency()), "Peak number of client requests being processed");
      addStat(objs, ServerStat.REQUEST_TOTAL_TIME, Strings.getDecimalTimeLengthString(getRequestTotalTime()), "Total time spent processing client requests");
      addStat(objs, ServerStat.REQUEST_TRANSACTIONS, Long.toString(getRequestTransactions()), "Number of client requests processed");

      addStat(objs, ServerStat.THREAD_COUNT, Integer.toString(ThreadUtility.getThreadCount()), "Current number of virtual machine threads");

      addStat(objs, ServerStat.UPTIME, Strings.getDecimalTimeLengthString(System.currentTimeMillis() - getStartTime()), "Amount of time the master server has been running");
    } catch (IOException err) {
      logger.log(Level.SEVERE, null, err);
      String message = err.getMessage();
      out.writeByte(AoservProtocol.IO_EXCEPTION);
      out.writeUTF(message == null ? "" : message);
      return;
    }
    writeObjects(source, out, provideProgress, objs);
  }
}
