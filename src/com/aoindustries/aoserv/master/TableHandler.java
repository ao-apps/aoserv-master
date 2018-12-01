/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServWritable;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.AccountHost;
import com.aoindustries.aoserv.client.account.Administrator;
import com.aoindustries.aoserv.client.accounting.BankTransaction;
import com.aoindustries.aoserv.client.backup.BackupReport;
import com.aoindustries.aoserv.client.billing.Transaction;
import com.aoindustries.aoserv.client.email.SpamMessage;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.Host;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>TableHandler</code> handles all the accesses to the AOServ tables.
 *
 * @author  AO Industries, Inc.
 */
final public class TableHandler {

	private static final Logger logger = LogFactory.getLogger(TableHandler.class);

	private static boolean started = false;

    public static void start() {
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting " + TableHandler.class.getSimpleName());
				initGetTableHandlers(System.out);
				started = true;
                System.out.println(": Done");
            }
        }
    }

	private TableHandler() {
	}

	/**
	 * The number of rows that will be loaded into each ResultSet for large tables.
	 * This is done to get around the fact that the PostgreSQL JDBC loads all results
	 * into the ResultSet, causing OutOfMemoryErrors on tables with millions of rows.
	 */
	public static final int RESULT_SET_BATCH_SIZE=500;

	/**
	 * The number of rows statements that should typically be used per update/insert/delete batch.
	 */
	public static final int UPDATE_BATCH_SIZE=500;

	/**
	 * The number of updates that will typically be done before the changes are committed.
	 */
	public static final int BATCH_COMMIT_INTERVAL=500;

	/*
	 * TODO: Use WITH RECURSIVE and no longer limit business tree depth.
	 * 
	 * Example query to get an account and all its parents:
	 * 
	 * WITH RECURSIVE account_and_up(accounting) AS (
	 *   VALUES ('LOG_NEWRANKS_NET')
	 * UNION ALL
	 *   SELECT a.parent FROM
	 *     account_and_up
	 *     INNER JOIN account."Account" a ON account_and_up.accounting = a.accounting
	 *   WHERE
	 *     a.parent IS NOT NULL
	 * )
	 * SELECT * FROM account_and_up;
	 *
	 * Example query to get an account and all its subaccounts:
	 *
	 * WITH RECURSIVE account_and_down(accounting) AS (
	 *   VALUES ('WOOT_WHMCS')
	 * UNION ALL
	 *   SELECT a.accounting FROM
	 *     account_and_down
	 *     INNER JOIN account."Account" a ON account_and_down.accounting = a.parent
	 * )
	 * SELECT * FROM account_and_down;
	 */

	/**
	 * The joins used for the business tree.
	 */
	public static final String
		BU1_PARENTS_JOIN=
			  "  account.\"Account\" bu1\n"
			+ "  left join account.\"Account\" bu2 on bu1.parent=bu2.accounting\n"
			+ "  left join account.\"Account\" bu3 on bu2.parent=bu3.accounting\n"
			+ "  left join account.\"Account\" bu4 on bu3.parent=bu4.accounting\n"
			+ "  left join account.\"Account\" bu5 on bu4.parent=bu5.accounting\n"
			+ "  left join account.\"Account\" bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting,\n",
		BU1_PARENTS_JOIN_NO_COMMA=
			  "  account.\"Account\" bu1\n"
			+ "  left join account.\"Account\" bu2 on bu1.parent=bu2.accounting\n"
			+ "  left join account.\"Account\" bu3 on bu2.parent=bu3.accounting\n"
			+ "  left join account.\"Account\" bu4 on bu3.parent=bu4.accounting\n"
			+ "  left join account.\"Account\" bu5 on bu4.parent=bu5.accounting\n"
			+ "  left join account.\"Account\" bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting\n",
		BU2_PARENTS_JOIN=
			  "      account.\"Account\" bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+"\n"
			+ "      left join account.\"Account\" bu8 on bu7.parent=bu8.accounting\n"
			+ "      left join account.\"Account\" bu9 on bu8.parent=bu9.accounting\n"
			+ "      left join account.\"Account\" bu10 on bu9.parent=bu10.accounting\n"
			+ "      left join account.\"Account\" bu11 on bu10.parent=bu11.accounting\n"
			+ "      left join account.\"Account\" bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+" on bu11.parent=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".accounting,\n"
	;

	/**
	 * The where clauses that accompany the joins.
	 */
	public static final String
		PK_BU1_PARENTS_WHERE=
			  "    pk.accounting=bu1.accounting\n"
			+ "    or pk.accounting=bu1.parent\n"
			+ "    or pk.accounting=bu2.parent\n"
			+ "    or pk.accounting=bu3.parent\n"
			+ "    or pk.accounting=bu4.parent\n"
			+ "    or pk.accounting=bu5.parent\n"
			+ "    or pk.accounting=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
		PK1_BU1_PARENTS_OR_WHERE=
			  "    or pk1.accounting=bu1.accounting\n"
			+ "    or pk1.accounting=bu1.parent\n"
			+ "    or pk1.accounting=bu2.parent\n"
			+ "    or pk1.accounting=bu3.parent\n"
			+ "    or pk1.accounting=bu4.parent\n"
			+ "    or pk1.accounting=bu5.parent\n"
			+ "    or pk1.accounting=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
		PK1_BU1_PARENTS_WHERE=
			  "    pk1.accounting=bu1.accounting\n"
			+ "    or pk1.accounting=bu1.parent\n"
			+ "    or pk1.accounting=bu2.parent\n"
			+ "    or pk1.accounting=bu3.parent\n"
			+ "    or pk1.accounting=bu4.parent\n"
			+ "    or pk1.accounting=bu5.parent\n"
			+ "    or pk1.accounting=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
		PK3_BU2_PARENTS_OR_WHERE=
			  "        or pk3.accounting=bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
			+ "        or pk3.accounting=bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
			+ "        or pk3.accounting=bu8.parent\n"
			+ "        or pk3.accounting=bu9.parent\n"
			+ "        or pk3.accounting=bu10.parent\n"
			+ "        or pk3.accounting=bu11.parent\n"
			+ "        or pk3.accounting=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n",
		PK3_BU2_PARENTS_WHERE=
			  "        pk3.accounting=bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
			+ "        or pk3.accounting=bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
			+ "        or pk3.accounting=bu8.parent\n"
			+ "        or pk3.accounting=bu9.parent\n"
			+ "        or pk3.accounting=bu10.parent\n"
			+ "        or pk3.accounting=bu11.parent\n"
			+ "        or pk3.accounting=bu"+(Account.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n"
	;

	/**
	 * Gets one object from a table.
	 */
	public static void getObject(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataInputStream in,
		CompressedDataOutputStream out,
		Table.TableID tableID
	) throws IOException, SQLException {
		UserId username=source.getUsername();
		User masterUser=MasterServer.getUser(conn, username);
		UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, source.getUsername());
		switch(tableID) {
			case BACKUP_REPORTS :
			{
				int id = in.readCompressedInt();
				if(masterUser != null) {
					assert masterServers != null;
					if(masterServers.length == 0) MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						"select * from backup.\"BackupReport\" where id=?",
						id
					); else MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						"select\n"
						+ "  br.*\n"
						+ "from\n"
						+ "  master.\"UserHost\" ms,\n"
						+ "  backup.\"BackupReport\" br\n"
						+ "where\n"
						+ "  ms.username=?\n"
						+ "  and ms.server=br.server\n"
						+ "  and br.id=?",
						username,
						id
					);
				} else {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BackupReport(),
						"select\n"
						+ "  br.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  backup.\"BackupReport\" br\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.id=br.package\n"
						+ "  and br.id=?",
						username,
						id
					);
				}
				break;
			}
			case BANK_TRANSACTIONS :
				if(BankAccountHandler.isBankAccounting(conn, source)) {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new BankTransaction(),
						"select\n"
						+ "  id,\n"
						+ "  time,\n" // Was cast to date here but not in full table query - why?
						+ "  account,\n"
						+ "  processor,\n"
						+ "  administrator,\n"
						+ "  type,\n"
						+ "  \"expenseCategory\",\n"
						+ "  description,\n"
						+ "  \"checkNo\",\n"
						+ "  amount,\n"
						+ "  confirmed\n"
						+ "from\n"
						+ "  accounting.\"BankTransaction\"\n"
						+ "where\n"
						+ "  id=?",
						in.readCompressedInt()
					);
				} else out.writeByte(AoservProtocol.DONE);
				break;
			case SPAM_EMAIL_MESSAGES :
				{
					int id=in.readCompressedInt();
					if(masterUser!=null && masterServers!=null && masterServers.length==0) {
						MasterServer.writeObject(
							conn,
							source,
							out,
							new SpamMessage(),
							"select * from email.\"SpamMessage\" where id=?",
							id
						);
					} else {
						throw new SQLException("Only master users may access email.SpamMessage.");
					}
				}
				break;
			case TRANSACTIONS :
				int transid=in.readCompressedInt();
				if(TransactionHandler.canAccessTransaction(conn, source, transid)) {
					MasterServer.writeObject(
						conn,
						source,
						out,
						new Transaction(),
						"select * from billing.\"Transaction\" where transid=?",
						transid
					);
				} else {
					out.writeShort(AoservProtocol.DONE);
				}
				break;
			default :
				throw new IOException("Unknown table ID: "+tableID);
		}
	}

	/**
	 * Caches row counts for each table on a per-username basis.
	 */
	private static final Map<UserId,int[]> rowCountsPerUsername=new HashMap<>();
	private static final Map<UserId,long[]> expireTimesPerUsername=new HashMap<>();

	private static final int MAX_ROW_COUNT_CACHE_AGE=60*60*1000;

	/** Copy used to avoid multiple array copies on each access. */
	private static final Table.TableID[] _tableIDs = Table.TableID.values();
	private static final int _numTables = _tableIDs.length;

	public static int getCachedRowCount(
		DatabaseConnection conn,
		RequestSource source,
		Table.TableID tableID
	) throws IOException, SQLException {
		UserId username=source.getUsername();

		// Synchronize to get the correct objects
		int[] rowCounts;
		long[] expireTimes;
		synchronized(rowCountsPerUsername) {
			rowCounts=rowCountsPerUsername.get(username);
			if(rowCounts==null) {
				rowCountsPerUsername.put(username, rowCounts=new int[_numTables]);
				expireTimesPerUsername.put(username, expireTimes=new long[_numTables]);
			} else expireTimes=expireTimesPerUsername.get(username);
		}

		// Synchronize on the array to provide a per-user lock
		synchronized(rowCounts) {
			long expireTime=expireTimes[tableID.ordinal()];
			long startTime=System.currentTimeMillis();
			if(
				expireTime==0
				|| expireTime<=startTime
				|| expireTime>(startTime+MAX_ROW_COUNT_CACHE_AGE)
			) {
				rowCounts[tableID.ordinal()]=getRowCount(
					conn,
					source,
					tableID
				);
				expireTimes[tableID.ordinal()]=System.currentTimeMillis()+MAX_ROW_COUNT_CACHE_AGE;
			}

			return rowCounts[tableID.ordinal()];
		}
	}

	/**
	 * Gets the number of accessible rows in a table.
	 */
	public static int getRowCount(
		DatabaseConnection conn,
		RequestSource source,
		Table.TableID tableID
	) throws IOException, SQLException {
		UserId username=source.getUsername();
		User masterUser=MasterServer.getUser(conn, username);
		UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, source.getUsername());
		switch(tableID) {
			case DISTRO_FILES :
				if(masterUser != null) {
					assert masterServers != null;
					if(masterServers.length == 0) {
						if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107)<=0) {
							return 0;
						} else {
							return conn.executeIntQuery(
								"select count(*) from \"distribution.management\".\"DistroFile\""
							);
						}
					} else {
						// Restrict to the operating system versions accessible to this user
						IntList osVersions=getOperatingSystemVersions(conn, source);
						if(osVersions.size()==0) return 0;
						StringBuilder sql=new StringBuilder();
						sql.append("select count(*) from \"distribution.management\".\"DistroFile\" where operating_system_version in (");
						for(int c=0;c<osVersions.size();c++) {
							if(c>0) sql.append(',');
							sql.append(osVersions.getInt(c));
						}
						sql.append(')');
						return conn.executeIntQuery(sql.toString());
					}
				} else return 0;
			case TICKETS :
				if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) return 0; // For backwards-compatibility only
				throw new IOException("Unknown table ID: "+tableID); // No longer used as of version 1.44
			default :
				throw new IOException("Unknown table ID: "+tableID);
		}
	}

	public interface GetTableHandler {
		/**
		 * Gets the set of tables handled.
		 */
		java.util.Set<Table.TableID> getTableIds();

		/**
		 * Handles a client request for the given table.
		 */
		void getTable(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException;
	}

	private static final ConcurrentMap<Table.TableID,GetTableHandler> getTableHandlers = new ConcurrentHashMap<>();

	/**
	 * This is available, but recommend registering via {@link ServiceLoader}.
	 */
	public static int addGetTableHandler(GetTableHandler handler) {
		int numTables = 0;
		{
			boolean successful = false;
			java.util.Set<Table.TableID> added = EnumSet.noneOf(Table.TableID.class);
			try {
				for(Table.TableID tableID : handler.getTableIds()) {
					GetTableHandler existing = getTableHandlers.putIfAbsent(tableID, handler);
					if(existing != null) throw new IllegalStateException("Handler already registered for table " + tableID + ": " + existing);
					added.add(tableID);
					numTables++;
				}
				successful = true;
			} finally {
				if(!successful) {
					// Rollback partial
					for(Table.TableID id : added) getTableHandlers.remove(id);
				}
			}
		}
		if(numTables == 0 && logger.isLoggable(Level.WARNING)) {
			logger.log(Level.WARNING, "Handler did not specify any tables: " + handler);
		}
		return numTables;
	}

	private static void initGetTableHandlers(PrintStream out) {
		int tableCount = 0;
		int handlerCount = 0;
		ServiceLoader<GetTableHandler> loader = ServiceLoader.load(GetTableHandler.class);
		Iterator<GetTableHandler> iter = loader.iterator();
		while(iter.hasNext()) {
			tableCount += addGetTableHandler(iter.next());
			handlerCount ++;
		}
		out.print(": " + GetTableHandler.class.getSimpleName() + " (" + handlerCount + " handlers for " + tableCount + " tables)");
	}

	static abstract public class GetTableHandlerByRole implements GetTableHandler {

		/**
		 * Calls role-specific implementations.
		 */
		@Override
		public void getTable(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException {
			if(masterUser != null) {
				assert masterServers != null;
				if(masterServers.length == 0) {
					getTableMaster(conn, source, out, provideProgress, tableID, masterUser);
				} else {
					getTableDaemon(conn, source, out, provideProgress, tableID, masterUser, masterServers);
				}
			} else {
				getTableAdministrator(conn, source, out, provideProgress, tableID);
			}
		}

		/**
		 * Handles a {@link User master user} request for the given table, with
		 * access to all {@link Account accounts} and {@link Host hosts}.
		 */
		abstract protected void getTableMaster(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser
		) throws IOException, SQLException;

		/**
		 * Handles a {@link User master user} request for the given table, with
		 * access limited to a set of {@link Host hosts}.  This is the filtering
		 * generally used by <a href="https://aoindustries.com/aoserv/daemon/">AOServ Daemon</a>.
		 */
		abstract protected void getTableDaemon(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException;

		/**
		 * Handles an {@link Administrator} request for the given table, with
		 * access limited by their set of {@link Account accounts} and the
		 * {@link Host hosts} those accounts can access (see {@link AccountHost}.
		 */
		abstract protected void getTableAdministrator(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID
		) throws IOException, SQLException;
	}

	static abstract public class GetTableHandlerPublic implements GetTableHandler {

		/**
		 * Handles requests for public tables, where nothing is filtered.
		 */
		@Override
		public void getTable(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException {
			getTablePublic(conn, source, out, provideProgress, tableID);
		}

		/**
		 * Handles the request for a public table.
		 */
		abstract protected void getTablePublic(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID
		) throws IOException, SQLException;
	}

	static abstract public class GetTableHandlerPermission implements GetTableHandler {

		/**
		 * Gets the permission that is required to query this table.
		 */
		abstract protected Permission.Name getPermissionName();

		/**
		 * Checks if has permission, writing an empty table when does not have the permission.
		 *
		 * @see  BusinessHandler#hasPermission(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
		 */
		@Override
		final public void getTable(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException {
			if(BusinessHandler.hasPermission(conn, source, getPermissionName())) {
				getTableHasPermission(conn, source, out, provideProgress, tableID, masterUser, masterServers);
			} else {
				// No permission, write empty table
				MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
			}
		}

		/**
		 * Handles a request for the given table, once permission has been verified.
		 *
		 * @see  BusinessHandler#hasPermission(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
		 */
		abstract protected void getTableHasPermission(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException;
	}

	static abstract public class GetTableHandlerPermissionByRole extends GetTableHandlerPermission {

		/**
		 * Calls role-specific implementations.
		 */
		@Override
		protected void getTableHasPermission(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException {
			if(masterUser != null) {
				assert masterServers != null;
				if(masterServers.length == 0) {
					getTableMasterHasPermission(conn, source, out, provideProgress, tableID, masterUser);
				} else {
					getTableDaemonHasPermission(conn, source, out, provideProgress, tableID, masterUser, masterServers);
				}
			} else {
				getTableAdministratorHasPermission(conn, source, out, provideProgress, tableID);
			}
		}

		/**
		 * Handles a {@link User master user} request for the given table, once
		 * permission has been verified, with access to all {@link Account accounts}
		 * and {@link Host hosts}.
		 *
		 * @see  BusinessHandler#hasPermission(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
		 */
		abstract protected void getTableMasterHasPermission(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser
		) throws IOException, SQLException;

		/**
		 * Handles a {@link User master user} request for the given table, once
		 * permission has been verified, with access limited to a set of
		 * {@link Host hosts}.  This is the filtering generally used by
		 * <a href="https://aoindustries.com/aoserv/daemon/">AOServ Daemon</a>.
		 *
		 * @see  BusinessHandler#hasPermission(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
		 */
		abstract protected void getTableDaemonHasPermission(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID,
			User masterUser,
			UserHost[] masterServers
		) throws IOException, SQLException;

		/**
		 * Handles an {@link Administrator} request for the given table, once
		 * permission has been verified, with access limited by their set of
		 * {@link Account accounts} and the {@link Host hosts} those accounts
		 * can access (see {@link AccountHost}.
		 *
		 * @see  BusinessHandler#hasPermission(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.aoserv.client.master.Permission.Name)
		 */
		abstract protected void getTableAdministratorHasPermission(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID
		) throws IOException, SQLException;
	}

	/**
	 * Gets an entire table.
	 */
	public static void getTable(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		final Table.TableID tableID
	) throws IOException, SQLException {
		UserId username=source.getUsername();
		User masterUser=MasterServer.getUser(conn, username);
		UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, username);

		GetTableHandler handler = getTableHandlers.get(tableID);
		if(handler != null) {
			handler.getTable(conn, source, out, provideProgress, tableID, masterUser, masterServers);
		} else {
			throw new IOException("No handler registered for table ID: " + tableID);
		}
	}

	/**
	 * Gets an old table given its table name.
	 * This is used for backwards compatibility to provide data for tables that no
	 * longer exist.
	 */
	public static void getOldTable(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream clientOut,
		boolean provideProgress,
		String tableName
	) throws IOException, SQLException {
		switch(tableName) {
			case "mysql.mysql_reserved_words" :
				if(
					source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
					&& source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
				) {
					com.aoindustries.aoserv.client.mysql.Server.ReservedWord[] reservedWords = com.aoindustries.aoserv.client.mysql.Server.ReservedWord.values();
					MasterServer.writeObjects(
						source,
						clientOut,
						provideProgress,
						new AbstractList<AOServWritable>() {
							@Override
							public AOServWritable get(int index) {
								return (out, clientVersion) -> {
									out.writeUTF(reservedWords[index].name().toLowerCase(Locale.ROOT));
								};
							}
							@Override
							public int size() {
								return reservedWords.length;
							}
						}
					);
					return;
				}
				// fall-through to empty response
				break;
			case "net.net_protocols" :
				if(
					source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
					&& source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
				) {
					// Send in lowercase
					com.aoindustries.net.Protocol[] netProtocols = com.aoindustries.net.Protocol.values();
					MasterServer.writeObjects(
						source,
						clientOut,
						provideProgress,
						new AbstractList<AOServWritable>() {
							@Override
							public AOServWritable get(int index) {
								return (out, clientVersion) -> {
									out.writeUTF(netProtocols[index].name().toLowerCase(Locale.ROOT));
								};
							}
							@Override
							public int size() {
								return netProtocols.length;
							}
						}
					);
					return;
				}
				// fall-through to empty response
				break;
			case "postgresql.postgres_reserved_words" :
				if(
					source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_100) >= 0
					&& source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80) <= 0
				) {
					com.aoindustries.aoserv.client.postgresql.Server.ReservedWord[] reservedWords = com.aoindustries.aoserv.client.postgresql.Server.ReservedWord.values();
					MasterServer.writeObjects(
						source,
						clientOut,
						provideProgress,
						new AbstractList<AOServWritable>() {
							@Override
							public AOServWritable get(int index) {
								return (out, clientVersion) -> {
									out.writeUTF(reservedWords[index].name().toLowerCase(Locale.ROOT));
								};
							}
							@Override
							public int size() {
								return reservedWords.length;
							}
						}
					);
					return;
				}
				// fall-through to empty response
				break;
		}
		// Not recognized table name and version range: write empty response
		MasterServer.writeObjects(source, clientOut, provideProgress, Collections.emptyList());
	}

	public static void invalidate(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Table.TableID tableID,
		int server
	) throws SQLException, IOException {
		checkInvalidator(conn, source, "invalidate");
		invalidateList.addTable(
			conn,
			tableID,
			InvalidateList.allBusinesses,
			server==-1 ? InvalidateList.allServers : InvalidateList.getServerCollection(server),
			true
		);
	}

	public static void checkInvalidator(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
		if(!isInvalidator(conn, source)) throw new SQLException("Table invalidation not allowed, '"+action+"'");
	}

	public static boolean isInvalidator(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		User mu=MasterServer.getUser(conn, source.getUsername());
		return mu!=null && mu.canInvalidateTables();
	}

	private static final Object tableNamesLock = new Object();
	private static Map<Integer,String> tableNames;

	/**
	 * Gets the table name, with schema prefixed.
	 *
	 * @see  #getTableName(com.aoindustries.dbc.DatabaseAccess, com.aoindustries.aoserv.client.Table.TableID)
	 */
	public static String getTableNameForDBTableID(DatabaseAccess conn, Integer dbTableId) throws SQLException {
		synchronized(tableNamesLock) {
			if(tableNames == null) {
				tableNames = conn.executeQuery(
					(ResultSet results) -> {
						Map<Integer,String> newMap = new HashMap<>();
						while(results.next()) {
							Integer id = results.getInt("id");
							String schema = results.getString("schema");
							String name = results.getString("name");
							if(newMap.put(id, schema + "." + name) != null) throw new SQLException("Duplicate id: " + id);
						}
						return newMap;
					},
					"select\n"
					+ "  s.\"name\" as \"schema\",\n"
					+ "  t.id,\n"
					+ "  t.\"name\"\n"
					+ "from\n"
					+ "  \"schema\".\"Table\" t\n"
					+ "  inner join \"schema\".\"Schema\" s on t.\"schema\" = s.id"
				);
			}
			return tableNames.get(dbTableId);
		}
	}

	/**
	 * Gets the table name, with schema prefixed.
	 *
	 * @see  #getTableNameForDBTableID(com.aoindustries.dbc.DatabaseAccess, java.lang.Integer)
	 */
	public static String getTableName(DatabaseAccess conn, Table.TableID tableID) throws IOException, SQLException {
		return getTableNameForDBTableID(
			conn,
			convertClientTableIDToDBTableID(
				conn,
				AoservProtocol.Version.CURRENT_VERSION,
				tableID.ordinal()
			)
		);
	}

	final private static EnumMap<AoservProtocol.Version,Map<Integer,Integer>> fromClientTableIDs=new EnumMap<>(AoservProtocol.Version.class);

	/**
	 * Converts a specific AoservProtocol version table ID to the number used in the database storage.
	 *
	 * @return  the {@code id} used in the database or {@code -1} if unknown
	 */
	public static int convertClientTableIDToDBTableID(
		DatabaseAccess conn,
		AoservProtocol.Version version,
		int clientTableID
	) throws IOException, SQLException {
		synchronized(fromClientTableIDs) {
			Map<Integer,Integer> tableIDs = fromClientTableIDs.get(version);
			if(tableIDs == null) {
				IntList clientTables = conn.executeIntListQuery(
					"select\n"
					+ "  st.id\n"
					+ "from\n"
					+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
					+ "  \"schema\".\"Table\"          st\n"
					+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
					+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  =  \"lastVersion\".version\n"
					+ "where\n"
					+ "  client_ap.version=?\n"
					+ "  and client_ap.created >= \"sinceVersion\".created\n"
					+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
					+ "order by\n"
					+ "  st.id",
					version.getVersion()
				);
				int numTables = clientTables.size();
				tableIDs = new HashMap<>(numTables*4/3+1);
				for(int c=0;c<numTables;c++) {
					tableIDs.put(c, clientTables.getInt(c));
				}
				fromClientTableIDs.put(version, tableIDs);
			}
			Integer I = tableIDs.get(clientTableID);
			return (I == null) ? -1 : I;
		}
	}

	final private static EnumMap<AoservProtocol.Version,Map<Integer,Integer>> toClientTableIDs=new EnumMap<>(AoservProtocol.Version.class);

	public static int convertDBTableIDToClientTableID(
		DatabaseConnection conn,
		AoservProtocol.Version version,
		int tableID
	) throws IOException, SQLException {
		synchronized(toClientTableIDs) {
			Map<Integer,Integer> clientTableIDs = toClientTableIDs.get(version);
			if(clientTableIDs == null) {
				IntList clientTables = conn.executeIntListQuery(
					"select\n"
					+ "  st.id\n"
					+ "from\n"
					+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
					+ "             \"schema\".\"Table\"                      st\n"
					+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
					+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  = \"lastVersion\".version\n"
					+ "where\n"
					+ "  client_ap.version=?\n"
					+ "  and client_ap.created >= \"sinceVersion\".created\n"
					+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
					+ "order by\n"
					+ "  st.id",
					version.getVersion()
				);
				int numTables = clientTables.size();
				clientTableIDs = new HashMap<>(numTables*4/3+1);
				for(int c = 0; c < numTables; c++) {
					clientTableIDs.put(clientTables.getInt(c), c);
				}
				toClientTableIDs.put(version, clientTableIDs);
			}
			Integer I = clientTableIDs.get(tableID);
			int clientTableID = (I == null) ? -1 : I;
			return clientTableID;
		}
	}

	/**
	 * Converts the client's AoservProtocol-version-specific table ID to the version used by the master's AoservProtocol version.
	 *
	 * @return  The <code>Table.TableID</code> or <code>null</code> if no match.
	 */
	public static Table.TableID convertFromClientTableID(
		DatabaseConnection conn,
		RequestSource source,
		int clientTableID
	) throws IOException, SQLException {
		int dbTableID=convertClientTableIDToDBTableID(conn, source.getProtocolVersion(), clientTableID);
		if(dbTableID==-1) return null;
		int tableID = convertDBTableIDToClientTableID(conn, AoservProtocol.Version.CURRENT_VERSION, dbTableID);
		if(tableID==-1) return null;
		return _tableIDs[tableID];
	}

	/**
	 * Converts a local (Master AoservProtocol) table ID to a client-version matched table ID.
	 */
	public static int convertToClientTableID(
		DatabaseConnection conn,
		RequestSource source,
		Table.TableID tableID
	) throws IOException, SQLException {
		int dbTableID=convertClientTableIDToDBTableID(conn, AoservProtocol.Version.CURRENT_VERSION, tableID.ordinal());
		if(dbTableID==-1) return -1;
		return convertDBTableIDToClientTableID(conn, source.getProtocolVersion(), dbTableID);
	}

	final private static EnumMap<AoservProtocol.Version,Map<Table.TableID,Map<String,Integer>>> clientColumnIndexes=new EnumMap<>(AoservProtocol.Version.class);

	/*
	 * 2018-11-18: This method appears unused.
	 * If need to bring it back, see the "TODO" note below about a likely bug.
	 * Also note that index is now a smallint/short.
	public static int getClientColumnIndex(
		DatabaseConnection conn,
		RequestSource source,
		Table.TableID tableID,
		String columnName
	) throws IOException, SQLException {
		// Get the list of resolved tables for the requested version
		AoservProtocol.Version version = source.getProtocolVersion();
		synchronized(clientColumnIndexes) {
			Map<Table.TableID,Map<String,Integer>> tables = clientColumnIndexes.get(version);
			if(tables==null) clientColumnIndexes.put(version, tables = new EnumMap<>(Table.TableID.class));

			// Find the list of columns for this table
			Map<String,Integer> columns = tables.get(tableID);
			if(columns == null) {
				// TODO: Why is tableID not used in this query???
				List<String> clientColumns = conn.executeStringListQuery(
					"select\n"
					+ "  sc.\"name\"\n"
					+ "from\n"
					+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
					+ "             \"schema\".\"Column\"                     sc\n"
					+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on sc.\"sinceVersion\" = \"sinceVersion\".version\n"
					+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on sc.\"lastVersion\"  =  \"lastVersion\".version\n"
					+ "where\n"
					+ "  client_ap.version=?\n"
					+ "  and client_ap.created >= \"sinceVersion\".created\n"
					+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
					+ "order by\n"
					+ "  sc.index",
					version.getVersion()
				);
				int numColumns = clientColumns.size();
				columns = new HashMap<>(numColumns*4/3+1);
				for(int c = 0; c < numColumns; c++) {
					columns.put(clientColumns.get(c), c);
				}
				tables.put(tableID, columns);
			}

			// Return the column or -1 if not found
			Integer columnIndex = columns.get(columnName);
			return (columnIndex == null) ? -1 : columnIndex;
		}
	}
	 */

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID == Table.TableID.SCHEMA_TABLES) {
			synchronized(tableNamesLock) {
				tableNames = null;
			}
		}
		if(tableID==Table.TableID.AOSERV_PROTOCOLS || tableID==Table.TableID.SCHEMA_TABLES) {
			synchronized(fromClientTableIDs) {
				fromClientTableIDs.clear();
			}
			synchronized(toClientTableIDs) {
				toClientTableIDs.clear();
			}
		}
		if(tableID==Table.TableID.AOSERV_PROTOCOLS || tableID==Table.TableID.SCHEMA_COLUMNS) {
			synchronized(clientColumnIndexes) {
				clientColumnIndexes.clear();
			}
		}
	}

	// TODO: Move to proper handler class
	public static IntList getOperatingSystemVersions(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select distinct\n"
			+ "  se.operating_system_version\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  linux.\"Server\" ao,\n"
			+ "  net.\"Host\" se,\n"
			+ "  distribution.\"OperatingSystemVersion\" osv\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=ao.server\n"
			+ "  and ao.server=se.id\n"
			+ "  and se.operating_system_version=osv.id\n"
			+ "  and osv.is_aoserv_daemon_supported",
			source.getUsername()
		);
	}
}