/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.collections.IntList;
import com.aoapps.security.Identifier;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import java.io.IOException;

/**
 * Obtains information necessary for request processing.
 */
public interface RequestSource {

	void cachesInvalidated(IntList tableList) throws IOException;

	Identifier getConnectorId();

	InvalidateCacheEntry getNextInvalidatedTables();

	String getSecurityMessageHeader();

	User.Name getCurrentAdministrator();

	/**
	 * Determines if the communication with the client is currently secure.
	 */
	boolean isSecure() throws IOException;

	boolean isClosed();

	/**
	 * Gets the id of the server that this connection is created from.  This
	 * is only used by connections initiated by daemons.
	 *
	 * @return  the id of the server or <code>-1</code> for none
	 */
	int getDaemonServer();

	/**
	 * Gets the protocol version number supported by the client.
	 */
	AoservProtocol.Version getProtocolVersion();
}
