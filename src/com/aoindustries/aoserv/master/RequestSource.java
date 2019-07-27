/*
 * Copyright 2001-2013, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.security.Identifier;
import com.aoindustries.util.IntList;
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
