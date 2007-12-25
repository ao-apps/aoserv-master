package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */

/**
 * @author  AO Industries, Inc.
 */
public final class VirtualDisk {

    final String device;
    final int extents;
    final DiskType primaryDiskType;
    final int primaryWeight;
    final DiskType secondaryDiskType;
    final int secondaryWeight;

    Disk selectedPrimaryDisk = null;
    Disk selectedSecondaryDisk = null;

    VirtualDisk(
        String device,
        int extents,
        DiskType primaryDiskType,
        int primaryWeight,
        DiskType secondaryDiskType,
        int secondaryWeight
    ) {
        this.device = device;
        this.extents = extents;
        this.primaryDiskType = primaryDiskType;
        this.primaryWeight = primaryWeight;
        this.secondaryDiskType = secondaryDiskType;
        this.secondaryWeight = secondaryWeight;
    }
}