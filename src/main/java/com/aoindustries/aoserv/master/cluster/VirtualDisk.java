/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2013, 2020, 2021, 2022  AO Industries, Inc.
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
@SuppressWarnings("overrides") // We will not implement hashCode, despite having equals
public final class VirtualDisk {

  final String device;
  final int extents;
  final DiskType primaryDiskType;
  final int primaryWeight;
  final DiskType secondaryDiskType;
  final int secondaryWeight;

  Disk selectedPrimaryDisk;
  Disk selectedSecondaryDisk;

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
  public boolean equals(Object obj) {
    return (obj instanceof VirtualDisk) && equals((VirtualDisk) obj);
  }

  public boolean equals(VirtualDisk other) {
    return
        extents == other.extents
            && primaryDiskType == other.primaryDiskType
            && primaryWeight == other.primaryWeight
            && secondaryDiskType == other.secondaryDiskType
            && secondaryWeight == other.secondaryWeight
            && device.equals(other.device)
    ;
  }
}
