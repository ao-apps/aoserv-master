package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.util.*;
import java.io.*;
import java.util.*;

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
    String getProtocolVersion();
}