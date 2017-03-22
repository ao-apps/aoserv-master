/*
 * Copyright 2001-2013, 2017 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.util.IntList;

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
