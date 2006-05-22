package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.util.*;

/**
 * Invalidate requests are place into a queue and then processed by concurrent threads.
 * The requests are temporarily stored in <code>InvalidateCacheEntry</code> objects.
 *
 * @author  AO Industries, Inc.
 */
final public class InvalidateCacheEntry {

    private final IntList invalidateList;
    private final int server;
    private final Long cacheSyncID;
    
    public InvalidateCacheEntry(
        IntList invalidateList,
        int server,
        Long cacheSyncID
    ) {
        this.invalidateList=invalidateList;
        this.server=server;
        this.cacheSyncID=cacheSyncID;
    }
    
    public IntList getInvalidateList() {
        return invalidateList;
    }
    
    public int getServer() {
        return server;
    }
    
    public Long getCacheSyncID() {
        return cacheSyncID;
    }
}