/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.aoserv.client.master.Process;
import com.aoindustries.net.InetAddress;
import com.aoindustries.security.Identifier;
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

	// Once all clients are >= 1.83.0, change key to Identifier
	private static final Map<Long,Process> processes = new LinkedHashMap<>();

	public static Process createProcess(InetAddress host, String protocol, boolean is_secure) {
		Instant now = Instant.now();
		Timestamp ts = new Timestamp(now.getEpochSecond() * 1000);
		ts.setNanos(now.getNano());
		while(true) {
			Identifier id = new Identifier();
			Long idLo = id.getLo();
			synchronized(processes) {
				if(
					// For clients < 1.83.0, only the low-order bits are sent to be compatible with their 64-bit IDs.
					// Once protocols < 1.83.0 are no longer supported, can change this:
					// !processes.containsKey(id)
					!processes.containsKey(idLo)
				) {
					Process process = new Process(
						id,
						host,
						protocol,
						is_secure,
						ts
					);
					processes.put(idLo, process);
					return process;
				}
			}
		}
	}

	public static void removeProcess(Process process) {
		Identifier id = process.getId();
		Long idLo = id.getLo();
		synchronized(processes) {
			Process removed = processes.remove(
				// For clients < 1.83.0, only the low-order bits are sent to be compatible with their 64-bit IDs.
				// Once protocols < 1.83.0 are no longer supported, can change this:
				// id
				idLo
			);
			if(removed == null) throw new IllegalStateException("Unable to find process " + id + " in the process list");
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
