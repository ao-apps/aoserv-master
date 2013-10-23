/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.util.IntList;
import java.io.IOException;

/**
 * Obtains information necessary for request processing.
 */
public interface RequestSource {

    void cachesInvalidated(IntList tableList) throws IOException;

    long getConnectorID();

    InvalidateCacheEntry getNextInvalidatedTables();

    String getSecurityMessageHeader();

    String getUsername();

    /**
     * Determines if the communication with the client is currently secure.
     */
    boolean isSecure() throws IOException;
    
    boolean isClosed();
    
    /**
     * Gets the pkey of the server that this connection is created from.  This
     * is only used by connections initiated by daemons.
     *
     * @return  the pkey of the server or <code>-1</code> for none
     */
    int getDaemonServer();
    
    /**
     * Gets the protocol version number supported by the client.
     */
    AOServProtocol.Version getProtocolVersion();
}