/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master;

import java.util.Collections;

/**
 * A dynamically loaded component of the master server.
 *
 * @author  AO Industries, Inc.
 */
// TODO: Move this, along with related stuff, to a new ao-service-manager project
// TODO: Then monitor the service states in noc-monitor
// TODO: Finally, use the service manager for AOServDaemon, and monitor, too
public interface MasterService {

	/**
	 * Once all the services have been loaded and instantiated, they are all
	 * started in dependency (TODO: in dependency order?).
	 * <p>
	 * Once the master server has attempted to start each service at least once,
	 * it will then proceed to accept incoming connections.
	 * </p>
	 * <p>
	 * When a service returns from start, without throwing an exception, it is
	 * considerer alive and start will not be called again.  When a service
	 * fails to start, by throwing an exception, start will be re-attempted once
	 * per minute indefinitely.
	 * </p>
	 * <p>
	 * There is no timeout on calls to start.  If a service blocks the entire
	 * master server will not start.
	 * </p>
	 */
	default void start() throws Exception {
		// Do nothing
	}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a set of {@link TableHandler.GetObjectHandler}.
	 * These are registered only after the handler successfully
	 * {@link #start() starts}.
	 */
	default Iterable<TableHandler.GetObjectHandler> startGetObjectHandlers() {
		return Collections.emptyList();
	}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a {@link TableHandler.GetObjectHandler}.
	 * This is registered only after the handler successfully
	 * {@link #start() starts}.
	 * <p>
	 * When not null, this is combined into a single list, after the entries
	 * from {@link #startGetObjectHandlers()}.
	 * </p>
	 */
	default TableHandler.GetObjectHandler startGetObjectHandler() {
		return null;
	}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a set of {@link TableHandler.GetTableHandler}.
	 * These are registered only after the handler successfully
	 * {@link #start() starts}.
	 */
	default Iterable<TableHandler.GetTableHandler> startGetTableHandlers() {
		return Collections.emptyList();
	}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a {@link TableHandler.GetTableHandler}.
	 * This is registered only after the handler successfully
	 * {@link #start() starts}.
	 * <p>
	 * When not null, this is combined into a single list, after the entries
	 * from {@link #startGetTableHandlers()}.
	 * </p>
	 */
	default TableHandler.GetTableHandler startGetTableHandler() {
		return null;
	}

	// TODO: Command handlers
}
