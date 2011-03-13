package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
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
