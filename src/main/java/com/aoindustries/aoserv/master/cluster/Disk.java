/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2009, 2020  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master.cluster;

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
