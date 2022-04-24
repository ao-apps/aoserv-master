/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.net.InetAddress;
import com.aoapps.security.SmallIdentifier;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author  AO Industries, Inc.
 */
public final class Process_Manager {

  /** Make no instances. */
  private Process_Manager() {
    throw new AssertionError();
  }

  private static final Map<SmallIdentifier, Process> processes = new LinkedHashMap<>();

  public static Process createProcess(InetAddress host, String protocol, boolean is_secure) {
    Instant now = Instant.now();
    Timestamp ts = new Timestamp(now.getEpochSecond() * 1000);
    ts.setNanos(now.getNano());
    while (true) {
      SmallIdentifier id = new SmallIdentifier();
      synchronized (processes) {
        if (!processes.containsKey(id)) {
          Process process = new Process(
              id,
              host,
              protocol,
              is_secure,
              ts
          );
          processes.put(id, process);
          return process;
        }
      }
    }
  }

  public static void removeProcess(Process process) {
    synchronized (processes) {
      Process removed = processes.remove(process.getId());
      if (removed == null) {
        throw new IllegalStateException("Unable to find process " + process.getId() + " in the process list");
      }
    }
  }

  public static List<Process> getSnapshot() throws IOException, SQLException {
    synchronized (processes) {
      List<Process> processesCopy = new ArrayList<>(processes.size());
      processesCopy.addAll(processes.values());
      return processesCopy;
    }
  }
}
