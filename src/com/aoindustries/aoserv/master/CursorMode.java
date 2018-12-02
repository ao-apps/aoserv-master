/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

/**
 * The <code>TableHandler</code> handles all the accesses to the AOServ tables.
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
	 * Automatic mode currently simply uses {@link #FETCH} when {@code !provideProgress}, otherwise
	 * uses {@link #SELECT}.  This matches the old behavior, but much more crafty ideas follow.
	 * <p>
	 * TODO: In automatic mode, the first query for a give table and user is performed
	 * with {@link #FETCH}, while subsequent queries will only use {@link #FETCH}
	 * if the previous query returned more than {@link #AUTO_CURSOR_ABOVE} rows.
	 * </p>
	 * <p>
	 * TODO: The per-(table, user) cache is cleaned in the background when unused for
	 * {@link TableHandler#MAX_ROW_COUNT_CACHE_AGE} milliseconds.
	 * </p>
	 * <p>
	 * TODO: Invalidating schema_tables clears these caches?
	 * </p>
	 * <p>
	 * TODO: Should we query PostgreSQL statistics instead, or in addition (pg_class.reltuples)?
	 * At least as an upper bound for the first query, where if total rows is low enough,
	 * skip cursor on the first query.  Hint: cache rultuples to bigint.
	 * </p>
	 *
	 * @see  MasterServer#writeObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.master.CursorMode, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...)
	 */
	AUTO;

	/**
	 * The number of rows above which when cursors are enabled in auto mode.
	 * By default, triple {@link TableHandler#RESULT_SET_BATCH_SIZE}, which avoids
	 * round-trips to the server for tables that would result in only a few
	 * batches.
	 *
	 * @see  TableHandler#RESULT_SET_BATCH_SIZE
	 */
	public static final int AUTO_CURSOR_ABOVE = TableHandler.RESULT_SET_BATCH_SIZE * 3;

}
