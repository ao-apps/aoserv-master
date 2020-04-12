/*
 * Copyright 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.net.InetAddress;
import com.aoindustries.security.Identifier;
import com.aoindustries.security.SmallIdentifier;
import com.aoindustries.sql.UnmodifiableTimestamp;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A mutable version of {@link com.aoindustries.aoserv.client.master.Process}
 * used to track processes on the master server.
 *
 * @author  AO Industries, Inc.
 */
public class Process extends com.aoindustries.aoserv.client.master.Process {

	private static boolean logCommands = false;

	/**
	 * Turns on/off command logging.
	 */
	public static void setLogCommands(boolean logCommands) {
		Process.logCommands = logCommands;
	}

	public static boolean getLogCommands() {
		return logCommands;
	}

	private Object[] command;

	public Process(
		SmallIdentifier id,
		InetAddress host,
		String protocol,
		boolean is_secure,
		Timestamp connect_time
	) {
		this.id = id;
		this.host = host;
		this.protocol = protocol;
		this.is_secure = is_secure;
		this.connect_time = UnmodifiableTimestamp.valueOf(connect_time);
		this.priority = Thread.NORM_PRIORITY;
		this.state = LOGIN;
		this.state_start_time = this.connect_time;
	}

	synchronized public void commandCompleted() {
		long time = System.currentTimeMillis();
		total_time += time - state_start_time.getTime();
		state = SLEEP;
		command = null;
		state_start_time = new UnmodifiableTimestamp(time);
	}

	synchronized public void commandRunning() {
		use_count++;
		state = RUN;
		state_start_time = new UnmodifiableTimestamp(System.currentTimeMillis());
	}

	synchronized public void commandSleeping() {
		if(!state.equals(SLEEP)) {
			long time = System.currentTimeMillis();
			state = SLEEP;
			total_time += time - state_start_time.getTime();
			state_start_time = new UnmodifiableTimestamp(time);
		}
	}

	public void setAOServProtocol(String aoserv_protocol) {
		this.aoserv_protocol = aoserv_protocol;
	}

	@Override
	public synchronized String[] getCommand() {
		if(command == null) return null;
		int len = command.length;
		List<String> params = new ArrayList<>(len);
		for(Object com : command) {
			// Expand any array parameter
			if(com instanceof Object[]) {
				for(Object com2 : (Object[])com) {
					params.add(Objects.toString(com2, null));
				}
			} else {
				params.add(Objects.toString(com, null));
			}
		}
		return params.toArray(new String[params.size()]);
	}

	synchronized public void setCommand(Object ... command) {
		this.command = command;
	}

	public void setAuthenticatedUser(User.Name username) {
		authenticated_user = username;
	}

	public void setConnectorId(Identifier connectorId) {
		this.connectorId = connectorId;
	}

	public void setDeamonServer(int server) {
		daemon_server = server;
	}

	public void setEffectiveUser(User.Name username) {
		effective_user = username;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}
}
