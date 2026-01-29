/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021, 2022, 2024  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;

/**
 * The cursor mode used by a {@link TableHandler}.
 *
 * @author  AO Industries, Inc.
 */
public enum CursorMode {

  /**
   * The query will be performed as-is, no CURSOR and FETCH.
   * This has the potential to use more memory during larger queries, but
   * minimizes the number of round-trips to the database.
   */
  SELECT,

  /**
   * The query will be performed via DECLARE CURSOR, FETCH, CLOSE.
   * This reduces the memory consumption of larger queries, at the cost of
   * more round-trips to the database.
   */
  FETCH,

  /**
   * Automatic mode currently simply uses {@link CursorMode#FETCH} when {@code !provideProgress}, otherwise
   * uses {@link CursorMode#SELECT}.  This matches the old behavior, but much more crafty ideas follow.
   *
   * <p>TODO: In automatic mode, the first query for a given table and user is performed
   * with {@link CursorMode#FETCH}, while subsequent queries will only use {@link CursorMode#FETCH}
   * if the previous query returned more than {@link CursorMode#AUTO_CURSOR_ABOVE} rows.</p>
   *
   * <p>TODO: The per-(table, user) cache is cleaned in the background when unused for
   * {@link TableHandler#MAX_ROW_COUNT_CACHE_AGE} milliseconds.</p>
   *
   * <p>TODO: Invalidating schema_tables clears these caches?</p>
   *
   * <p>TODO: Should we query PostgreSQL statistics instead, or in addition (pg_class.reltuples)?
   * At least as an upper bound for the first query, where if total rows is low enough,
   * skip cursor on the first query.  Hint: cache rultuples to bigint.</p>
   *
   * @see  AoservMaster#writeObjects(com.aoapps.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoapps.hodgepodge.io.stream.StreamableOutput, boolean, com.aoindustries.aoserv.master.CursorMode, com.aoindustries.aoserv.client.AoservObject, java.lang.String, java.lang.Object...)
   */
  AUTO;

  /**
   * The number of rows above which when cursors are enabled in auto mode.
   * By default, triple {@link DatabaseConnection#FETCH_SIZE}, which avoids
   * round-trips to the server for tables that would result in only a few
   * batches.
   *
   * @see  DatabaseConnection#FETCH_SIZE
   */
  public static final int AUTO_CURSOR_ABOVE = DatabaseConnection.FETCH_SIZE * 3;

}
