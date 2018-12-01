/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.aoserv.client.master.Process;
import com.aoindustries.net.InetAddress;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final public class Process_Manager {

	/**
	 * Make no instances.
	 */
	private Process_Manager() {
	}

	private static final List<Process> processes = new ArrayList<>();

	private static long nextPID = 1;

	public static Process createProcess(
		InetAddress host,
		String protocol,
		boolean is_secure
	) {
		synchronized(Process_Manager.class) {
			long time = System.currentTimeMillis();
			Process process = new Process(
				nextPID++,
				host,
				protocol,
				is_secure,
				time
			);
			processes.add(process);
			return process;
		}
	}

	public static void removeProcess(Process process) {
		synchronized(Process_Manager.class) {
			int size = processes.size();
			for(int c = 0; c < size; c++) {
				Process mp = processes.get(c);
				if(mp.getProcessID() == process.getProcessID()) {
					processes.remove(c);
					return;
				}
			}
			throw new IllegalStateException("Unable to find process #" + process.getProcessID() + " in the process list");
		}
	}

	public static List<Process> getSnapshot() throws IOException, SQLException {
		List<Process> processesCopy=new ArrayList<>(processes.size());
		synchronized(Process_Manager.class) {
			processesCopy.addAll(processes);
		}
		return processesCopy;
	}
}
