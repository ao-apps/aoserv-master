package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */

/**
 * @author  AO Industries, Inc.
 */
public enum DiskType {
    RAID1_7200,
    RAID1_10000,
    RAID5_10000,
    RAID1_15000;

    static final DiskType[] diskTypes = DiskType.values();
}
