/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.net.InetAddress;
import com.aoindustries.security.SmallIdentifier;
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
final public class Process_Manager {

	/**
	 * Make no instances.
	 */
	private Process_Manager() {
	}

	private static final Map<SmallIdentifier,Process> processes = new LinkedHashMap<>();

	public static Process createProcess(InetAddress host, String protocol, boolean is_secure) {
		Instant now = Instant.now();
		Timestamp ts = new Timestamp(now.getEpochSecond() * 1000);
		ts.setNanos(now.getNano());
		while(true) {
			SmallIdentifier id = new SmallIdentifier();
			synchronized(processes) {
				if(!processes.containsKey(id)) {
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
		synchronized(processes) {
			Process removed = processes.remove(process.getId());
			if(removed == null) throw new IllegalStateException("Unable to find process " + process.getId() + " in the process list");
		}
	}

	public static List<Process> getSnapshot() throws IOException, SQLException {
		synchronized(processes) {
			List<Process> processesCopy = new ArrayList<>(processes.size());
			processesCopy.addAll(processes.values());
			return processesCopy;
		}
	}
}
