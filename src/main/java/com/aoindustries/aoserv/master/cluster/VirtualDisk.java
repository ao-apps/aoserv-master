/*
 * Copyright 2007-2013, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.cluster;

/**
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("overrides") // We will not implement hashCode, despite having equals
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

	@Override
	public boolean equals(Object O) {
		return O!=null && (O instanceof VirtualDisk) && equals((VirtualDisk)O);
	}

	public boolean equals(VirtualDisk other) {
		return
			extents==other.extents
			&& primaryDiskType==other.primaryDiskType
			&& primaryWeight==other.primaryWeight
			&& secondaryDiskType==other.secondaryDiskType
			&& secondaryWeight==other.secondaryWeight
			&& device.equals(other.device)
		;
	}
}
