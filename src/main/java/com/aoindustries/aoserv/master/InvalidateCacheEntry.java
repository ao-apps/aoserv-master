/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2017, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master;

import com.aoapps.collections.IntList;

/**
 * Invalidate requests are place into a queue and then processed by concurrent threads.
 * The requests are temporarily stored in <code>InvalidateCacheEntry</code> objects.
 *
 * @author  AO Industries, Inc.
 */
public final class InvalidateCacheEntry {

  private final IntList invalidateList;
  private final int host;
  private final Long cacheSyncId;

  public InvalidateCacheEntry(
      IntList invalidateList,
      int host,
      Long cacheSyncId
  ) {
    this.invalidateList = invalidateList;
    this.host = host;
    this.cacheSyncId = cacheSyncId;
  }

  public IntList getInvalidateList() {
    return invalidateList;
  }

  public int getHost() {
    return host;
  }

  public Long getCacheSyncId() {
    return cacheSyncId;
  }
}
