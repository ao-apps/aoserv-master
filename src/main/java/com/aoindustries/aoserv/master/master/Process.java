/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2019, 2021  AO Industries, Inc.
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
import com.aoapps.security.Identifier;
import com.aoapps.security.SmallIdentifier;
import com.aoapps.sql.UnmodifiableTimestamp;
import com.aoindustries.aoserv.client.account.User;
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

	@SuppressWarnings("deprecation")
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

	public synchronized void commandCompleted() {
		long time = System.currentTimeMillis();
		total_time += time - state_start_time.getTime();
		state = SLEEP;
		command = null;
		state_start_time = new UnmodifiableTimestamp(time);
	}

	public synchronized void commandRunning() {
		use_count++;
		state = RUN;
		state_start_time = new UnmodifiableTimestamp(System.currentTimeMillis());
	}

	public synchronized void commandSleeping() {
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

	public synchronized void setCommand(Object ... command) {
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
