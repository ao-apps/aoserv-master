package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */

/**
 * @author  AO Industries, Inc.
 */
public final class Disk {

    final String device;
    final DiskType diskType;
    final int extents;

    Disk(String device, DiskType diskType, int extents) {
        this.device = device;
        this.diskType = diskType;
        this.extents = extents;
    }
}
