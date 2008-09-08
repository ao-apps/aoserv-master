package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */

/**
 * @author  AO Industries, Inc.
 */
public final class Disk {

    final String device;
    final DiskType diskType;
    final int extents;

    /**
     * The allocated extents during the recursive processing.
     */
    int allocatedExtents = 0;

    /**
     * The allocated weight during the recursive processing.
     */
    int allocatedWeight = 0;

    Disk(String device, DiskType diskType, int extents) {
        this.device = device;
        this.diskType = diskType;
        this.extents = extents;
    }
}
