/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
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
	default void start() throws Exception {}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a set of {@link TableHandler.GetObjectHandler}.
	 * These are registered only after the handler successfully
	 * {@link #start() starts}.
	 */
	default Iterable<TableHandler.GetObjectHandler> getGetObjectHandlers() {
		return Collections.emptyList();
	}

	/**
	 * In order the reduce the number of services listed in /META-INF/services,
	 * a handler may provide a set of {@link TableHandler.GetTableHandler}.
	 * These are registered only after the handler successfully
	 * {@link #start() starts}.
	 */
	default Iterable<TableHandler.GetTableHandler> getGetTableHandlers() {
		return Collections.emptyList();
	}

	// TODO: Command handlers
}
