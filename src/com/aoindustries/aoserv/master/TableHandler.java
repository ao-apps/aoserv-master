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
import com.aoindustries.aoserv.client.ftp.GuestUser;
import com.aoindustries.aoserv.client.ftp.PrivateServer;
import com.aoindustries.aoserv.client.infrastructure.PhysicalServer;
import com.aoindustries.aoserv.client.infrastructure.ProcessorType;
import com.aoindustries.aoserv.client.infrastructure.Rack;
import com.aoindustries.aoserv.client.infrastructure.ServerFarm;
import com.aoindustries.aoserv.client.infrastructure.VirtualDisk;
import com.aoindustries.aoserv.client.infrastructure.VirtualServer;
import com.aoindustries.aoserv.client.linux.DaemonAcl;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.GroupServer;
import com.aoindustries.aoserv.client.linux.GroupType;
import com.aoindustries.aoserv.client.linux.GroupUser;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.linux.Shell;
import com.aoindustries.aoserv.client.linux.TimeZone;
import com.aoindustries.aoserv.client.linux.UserServer;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.master.AdministratorPermission;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserAcl;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.mysql.Database;
import com.aoindustries.aoserv.client.mysql.DatabaseUser;
import com.aoindustries.aoserv.client.net.AppProtocol;
import com.aoindustries.aoserv.client.net.Bind;
import com.aoindustries.aoserv.client.net.BindFirewallZone;
import com.aoindustries.aoserv.client.net.Device;
import com.aoindustries.aoserv.client.net.DeviceId;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.net.Host;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.net.TcpRedirect;
import com.aoindustries.aoserv.client.net.monitoring.IpAddressMonitoring;
import com.aoindustries.aoserv.client.net.reputation.Limiter;
import com.aoindustries.aoserv.client.net.reputation.LimiterClass;
import com.aoindustries.aoserv.client.net.reputation.Network;
import com.aoindustries.aoserv.client.net.reputation.Set;
import com.aoindustries.aoserv.client.payment.CountryCode;
import com.aoindustries.aoserv.client.payment.CreditCard;
import com.aoindustries.aoserv.client.payment.Payment;
import com.aoindustries.aoserv.client.payment.PaymentType;
import com.aoindustries.aoserv.client.payment.Processor;
import com.aoindustries.aoserv.client.pki.Certificate;
import com.aoindustries.aoserv.client.pki.CertificateName;
import com.aoindustries.aoserv.client.pki.CertificateOtherUse;
import com.aoindustries.aoserv.client.pki.EncryptionKey;
import com.aoindustries.aoserv.client.postgresql.Encoding;
import com.aoindustries.aoserv.client.postgresql.Version;
import com.aoindustries.aoserv.client.reseller.Brand;
import com.aoindustries.aoserv.client.reseller.BrandCategory;
import com.aoindustries.aoserv.client.reseller.Category;
import com.aoindustries.aoserv.client.reseller.Reseller;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Column;
import com.aoindustries.aoserv.client.schema.ForeignKey;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.schema.Type;
import com.aoindustries.aoserv.client.scm.CvsRepository;
import com.aoindustries.aoserv.client.signup.Option;
import com.aoindustries.aoserv.client.signup.Request;
import com.aoindustries.aoserv.client.ticket.Action;
import com.aoindustries.aoserv.client.ticket.ActionType;
import com.aoindustries.aoserv.client.ticket.Assignment;
import com.aoindustries.aoserv.client.ticket.Language;
import com.aoindustries.aoserv.client.ticket.Priority;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.Ticket;
import com.aoindustries.aoserv.client.ticket.TicketType;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.web.Header;
import com.aoindustries.aoserv.client.web.HttpdBind;
import com.aoindustries.aoserv.client.web.HttpdServer;
import com.aoindustries.aoserv.client.web.Location;
import com.aoindustries.aoserv.client.web.Redirect;
import com.aoindustries.aoserv.client.web.Site;
import com.aoindustries.aoserv.client.web.StaticSite;
import com.aoindustries.aoserv.client.web.VirtualHost;
import com.aoindustries.aoserv.client.web.VirtualHostName;
import com.aoindustries.aoserv.client.web.tomcat.Context;
import com.aoindustries.aoserv.client.web.tomcat.ContextDataSource;
import com.aoindustries.aoserv.client.web.tomcat.ContextParameter;
import com.aoindustries.aoserv.client.web.tomcat.JkMount;
import com.aoindustries.aoserv.client.web.tomcat.PrivateTomcatSite;
import com.aoindustries.aoserv.client.web.tomcat.SharedTomcat;
import com.aoindustries.aoserv.client.web.tomcat.SharedTomcatSite;
import com.aoindustries.aoserv.client.web.tomcat.Worker;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
                System.out.print("Starting " + TableHandler.class.getName());
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

	public interface GetTableHandlerByRole extends GetTableHandler {

		/**
		 * Calls role-specific implementations.
		 */
		@Override
		default void getTable(
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
		void getTableMaster(
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
		void getTableDaemon(
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
		void getTableAdministrator(
			DatabaseConnection conn,
			RequestSource source,
			CompressedDataOutputStream out,
			boolean provideProgress,
			Table.TableID tableID
		) throws IOException, SQLException;
	}

	public interface GetTableHandlerPublic extends GetTableHandler {

		/**
		 * Handles requests for public tables, where nothing is filtered.
		 */
		@Override
		default void getTable(
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
		void getTablePublic(
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
			switch(tableID) {
				case AO_SERVER_DAEMON_HOSTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new DaemonAcl(),
							"select * from linux.\"DaemonAcl\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new DaemonAcl(),
							"select\n"
							+ "  sdh.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  linux.\"DaemonAcl\" sdh\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=sdh.ao_server",
							username
						);
					} else {
						MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
					}
					break;
				case AO_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Server(),
							"select\n"
							+ "  server,\n"
							+ "  hostname,\n"
							+ "  daemon_bind,\n"
							+ "  daemon_key,\n"
							+ "  pool_size,\n"
							+ "  distro_hour,\n"
							+ "  last_distro_time,\n"
							+ "  failover_server,\n"
							+ "  \"daemonDeviceId\",\n"
							+ "  daemon_connect_bind,\n"
							+ "  time_zone,\n"
							+ "  jilter_bind,\n"
							+ "  restrict_outbound_email,\n"
							+ "  daemon_connect_address,\n"
							+ "  failover_batch_size,\n"
							+ "  monitoring_load_low,\n"
							+ "  monitoring_load_medium,\n"
							+ "  monitoring_load_high,\n"
							+ "  monitoring_load_critical,\n"
							+ "  \"uidMin\",\n"
							+ "  \"gidMin\",\n"
							+ "  \"uidMax\",\n"
							+ "  \"gidMax\",\n"
							+ "  \"lastUid\",\n"
							+ "  \"lastGid\",\n"
							+ "  sftp_umask\n"
							+ "from\n"
							+ "  linux.\"Server\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Server(),
							"select distinct\n"
							+ "  ao2.server,\n"
							+ "  ao2.hostname,\n"
							+ "  ao2.daemon_bind,\n"
							+ "  ao2.daemon_key,\n"
							+ "  ao2.pool_size,\n"
							+ "  ao2.distro_hour,\n"
							+ "  ao2.last_distro_time,\n"
							+ "  ao2.failover_server,\n"
							+ "  ao2.\"daemonDeviceId\",\n"
							+ "  ao2.daemon_connect_bind,\n"
							+ "  ao2.time_zone,\n"
							+ "  ao2.jilter_bind,\n"
							+ "  ao2.restrict_outbound_email,\n"
							+ "  ao2.daemon_connect_address,\n"
							+ "  ao2.failover_batch_size,\n"
							+ "  ao2.monitoring_load_low,\n"
							+ "  ao2.monitoring_load_medium,\n"
							+ "  ao2.monitoring_load_high,\n"
							+ "  ao2.monitoring_load_critical,\n"
							+ "  ao2.\"uidMin\",\n"
							+ "  ao2.\"gidMin\",\n"
							+ "  ao2.\"uidMax\",\n"
							+ "  ao2.\"gidMax\",\n"
							+ "  ao2.\"lastUid\",\n"
							+ "  ao2.\"lastGid\",\n"
							+ "  ao2.sftp_umask\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join linux.\"Server\" ao on ms.server=ao.server\n"
							// Allow its failover parent
							+ "  left join linux.\"Server\" ff on ao.failover_server=ff.server\n"
							// Allow its failover children
							+ "  left join linux.\"Server\" fs on ao.server=fs.failover_server\n"
							// Allow servers it replicates to
							+ "  left join backup.\"FileReplication\" ffr on ms.server=ffr.server\n"
							+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
							+ "  linux.\"Server\" ao2\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and (\n"
							// Allow direct access
							+ "    ms.server=ao2.server\n"
							// Allow its failover parent
							+ "    or ff.server=ao2.server\n"
							// Allow its failover children
							+ "    or fs.server=ao2.server\n"
							// Allow servers it replicates to
							+ "    or bp.ao_server=ao2.server\n"
							+ "  )",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Server(),
						"select distinct\n"
						+ "  ao.server,\n"
						+ "  ao.hostname,\n"
						+ "  ao.daemon_bind,\n"
						+ "  '"+AoservProtocol.FILTERED+"'::text,\n"
						+ "  ao.pool_size,\n"
						+ "  ao.distro_hour,\n"
						+ "  ao.last_distro_time,\n"
						+ "  ao.failover_server,\n"
						+ "  ao.\"daemonDeviceId\",\n"
						+ "  ao.daemon_connect_bind,\n"
						+ "  ao.time_zone,\n"
						+ "  ao.jilter_bind,\n"
						+ "  ao.restrict_outbound_email,\n"
						+ "  ao.daemon_connect_address,\n"
						+ "  ao.failover_batch_size,\n"
						+ "  ao.monitoring_load_low,\n"
						+ "  ao.monitoring_load_medium,\n"
						+ "  ao.monitoring_load_high,\n"
						+ "  ao.monitoring_load_critical,\n"
						+ "  ao.\"uidMin\",\n"
						+ "  ao.\"gidMin\",\n"
						+ "  ao.\"uidMax\",\n"
						+ "  ao.\"gidMax\",\n"
						+ "  ao.\"lastUid\",\n"
						+ "  ao.\"lastGid\",\n"
						+ "  ao.sftp_umask\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  linux.\"Server\" ao\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=ao.server\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=ao.server\n"
						+ "  )",
						username
					);
					break;
				case AOSERV_PERMISSIONS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Permission(),
						"select * from master.\"Permission\""
					);
					break;
				case AOSERV_PROTOCOLS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new AoservProtocol(),
						"select * from \"schema\".\"AoservProtocol\""
					);
					break;
				case BRANDS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Brand(),
								"select * from reseller.\"Brand\""
							);
						} else {
							// Limited access to its own brand only
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Brand(),
								"select\n"
								+ "  br.accounting,\n"
								+ "  br.nameserver1,\n"
								+ "  br.nameserver2,\n"
								+ "  br.nameserver3,\n"
								+ "  br.nameserver4,\n"
								+ "  br.smtp_linux_server_account,\n"
								+ "  null::text,\n" // smtp_host
								+ "  '"+AoservProtocol.FILTERED+"'::text,\n" // smtp_password
								+ "  br.imap_linux_server_account,\n"
								+ "  null::text,\n" // imap_host
								+ "  '"+AoservProtocol.FILTERED+"'::text,\n" // imap_password
								+ "  br.support_email_address,\n"
								+ "  br.support_email_display,\n"
								+ "  br.signup_email_address,\n"
								+ "  br.signup_email_display,\n"
								+ "  br.ticket_encryption_from,\n"
								+ "  br.ticket_encryption_recipient,\n"
								+ "  br.signup_encryption_from,\n"
								+ "  br.signup_encryption_recipient,\n"
								+ "  br.support_toll_free,\n"
								+ "  br.support_day_phone,\n"
								+ "  br.support_emergency_phone1,\n"
								+ "  br.support_emergency_phone2,\n"
								+ "  br.support_fax,\n"
								+ "  br.support_mailing_address1,\n"
								+ "  br.support_mailing_address2,\n"
								+ "  br.support_mailing_address3,\n"
								+ "  br.support_mailing_address4,\n"
								+ "  br.english_enabled,\n"
								+ "  br.japanese_enabled,\n"
								+ "  br.aoweb_struts_http_url_base,\n"
								+ "  br.aoweb_struts_https_url_base,\n"
								+ "  br.aoweb_struts_google_verify_content,\n"
								+ "  br.aoweb_struts_noindex,\n"
								+ "  br.aoweb_struts_google_analytics_new_tracking_code,\n"
								+ "  br.aoweb_struts_signup_admin_address,\n"
								+ "  br.aoweb_struts_vnc_bind,\n"
								+ "  '"+AoservProtocol.FILTERED+"'::text,\n" // aoweb_struts_keystore_type
								+ "  '"+AoservProtocol.FILTERED+"'::text\n" // aoweb_struts_keystore_password
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk,\n"
								+ "  reseller.\"Brand\" br\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk.name\n"
								+ "  and pk.accounting=br.accounting",
								username
							);
						}
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Brand(),
							"select\n"
							+ "  br.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  reseller.\"Brand\" br\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  ) and bu1.accounting=br.accounting",
							username
						);
					}
					break;
				case BUSINESS_ADMINISTRATOR_PERMISSIONS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new AdministratorPermission(),
							"select * from master.\"AdministratorPermission\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new AdministratorPermission(),
							"select distinct\n"
							+ "  bp.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  master.\"AdministratorPermission\" bp\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=bp.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new AdministratorPermission(),
						"select\n"
						+ "  bp.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  master.\"AdministratorPermission\" bp\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username=un1.username\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=bp.username",
						username
					);
					break;
				case CREDIT_CARD_PROCESSORS :
					if(BusinessHandler.hasPermission(conn, source, Permission.Name.get_credit_card_processors)) {
						if(masterUser != null) {
							assert masterServers != null;
							if(masterServers.length == 0) MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Processor(),
								"select * from payment.\"Processor\""
							); else {
								MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
							}
						} else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Processor(),
							"select\n"
							+ "  ccp.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  payment.\"Processor\" ccp\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=ccp.accounting",
							username
						);
					} else {
						// No permission, return empty list
						MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
					}
					break;
				case CREDIT_CARDS :
					if(BusinessHandler.hasPermission(conn, source, Permission.Name.get_credit_cards)) {
						if(masterUser != null) {
							assert masterServers != null;
							if(masterServers.length == 0) MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new CreditCard(),
								"select * from payment.\"CreditCard\""
							); else {
								MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
							}
						} else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CreditCard(),
							"select\n"
							+ "  cc.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  payment.\"CreditCard\" cc\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=cc.accounting",
							username
						);
					} else {
						// No permission, return empty list
						MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
					}
					break;
				case CREDIT_CARD_TRANSACTIONS :
					if(BusinessHandler.hasPermission(conn, source, Permission.Name.get_credit_card_transactions)) {
						if(masterUser != null) {
							assert masterServers != null;
							if(masterServers.length == 0) MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Payment(),
								"select * from payment.\"Payment\""
							); else {
								MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
							}
						} else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Payment(),
							"select\n"
							+ "  cct.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  payment.\"Payment\" cct\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=cct.accounting",
							username
						);
					} else {
						// No permission, return empty list
						MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
					}
					break;
				case COUNTRY_CODES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new CountryCode(),
						"select * from payment.\"CountryCode\""
					);
					break;
				case CVS_REPOSITORIES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CvsRepository(),
							"select * from scm.\"CvsRepository\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CvsRepository(),
							"select\n"
							+ "  cr.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  linux.\"UserServer\" lsa,\n"
							+ "  scm.\"CvsRepository\" cr\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=lsa.ao_server\n"
							+ "  and lsa.id=cr.linux_server_account",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new CvsRepository(),
						"select\n"
						+ "  cr.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  linux.\"UserServer\" lsa,\n"
						+ "  scm.\"CvsRepository\" cr\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=lsa.username\n"
						+ "  and lsa.id=cr.linux_server_account",
						username
					);
					break;
				case ENCRYPTION_KEYS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new EncryptionKey(),
								"select * from pki.\"EncryptionKey\""
							);
						} else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new EncryptionKey(),
							"select\n"
							+ "  ek.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  pki.\"EncryptionKey\" ek\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk1.name\n"
							+ "  and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=ek.accounting",
							username
						);
					}
					break;
				case FIREWALLD_ZONES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new FirewallZone(),
							"select * from net.\"FirewallZone\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new FirewallZone(),
							"select\n"
							+ "  fz.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"FirewallZone\" fz\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=fz.server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new FirewallZone(),
						"select\n"
						+ "  fz.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						+ "  net.\"FirewallZone\" fz\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and bs.server=fz.server",
						username
					);
					break;
				case FTP_GUEST_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GuestUser(),
							"select * from ftp.\"GuestUser\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GuestUser(),
							"select distinct\n"
							+ "  fgu.username\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  ftp.\"GuestUser\" fgu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=fgu.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new GuestUser(),
						"select\n"
						+ "  fgu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  ftp.\"GuestUser\" fgu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=fgu.username",
						username
					);
					break;
				case HTTPD_BINDS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new HttpdBind(),
							"select * from web.\"HttpdBind\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new HttpdBind(),
							"select\n"
							+ "  hb.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  web.\"HttpdBind\" hb\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=nb.server\n"
							+ "  and nb.id=hb.net_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new HttpdBind(),
						"select\n"
						+ "  hb.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"VirtualHost\" hsb,\n"
						+ "  web.\"HttpdBind\" hb,\n"
						+ "  net.\"Bind\" nb\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsb.httpd_site\n"
						+ "  and hsb.httpd_bind=hb.net_bind\n"
						+ "  and hb.net_bind=nb.id\n"
						+ "group by\n"
						+ "  hb.net_bind,\n"
						+ "  hb.httpd_server,\n"
						+ "  nb.server,\n"
						+ "  nb.\"ipAddress\",\n"
						+ "  nb.port,\n"
						+ "  nb.net_protocol",
						username
					);
					break;
				case HTTPD_JBOSS_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new com.aoindustries.aoserv.client.web.jboss.Site(),
								"select * from \"web.jboss\".\"Site\""
							);
						} else {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new com.aoindustries.aoserv.client.web.jboss.Site(),
								"select\n"
								+ "  hjs.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms,\n"
								+ "  web.\"Site\" hs,\n"
								+ "  \"web.jboss\".\"Site\" hjs\n"
								+ "where\n"
								+ "  ms.username=?\n"
								+ "  and ms.server=hs.ao_server\n"
								+ "  and hs.id=hjs.tomcat_site",
								username
							);
						}
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.jboss.Site(),
						"select\n"
						+ "  hjs.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.jboss\".\"Site\" hjs\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hjs.tomcat_site",
						username
					);
					break;
				case HTTPD_JBOSS_VERSIONS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.jboss.Version(),
						"select * from \"web.jboss\".\"Version\""
					);
					break;
				case HTTPD_JK_CODES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.tomcat.WorkerName(),
						"select * from \"web.tomcat\".\"WorkerName\""
					);
					break;
				case HTTPD_JK_PROTOCOLS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.tomcat.JkProtocol(),
						"select * from \"web.tomcat\".\"JkProtocol\""
					);
					break;
				case HTTPD_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new HttpdServer(),
							"select * from web.\"HttpdServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new HttpdServer(),
							"select\n"
							+ "  hs.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"HttpdServer\" hs\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server",
							username
						);
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new HttpdServer(),
							"select\n"
							+ "  hs.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  web.\"HttpdServer\" hs\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and pk.accounting=bs.accounting\n"
							+ "  and bs.server=hs.ao_server",
							username
						);
					}
					break;
				case HTTPD_SHARED_TOMCATS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new SharedTomcat(),
							"select * from \"web.tomcat\".\"SharedTomcat\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new SharedTomcat(),
							"select\n"
							+ "  hst.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  \"web.tomcat\".\"SharedTomcat\" hst\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hst.ao_server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new SharedTomcat(),
						"select\n"
						+ "  hst.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  linux.\"Group\" lg,\n"
						+ "  linux.\"GroupServer\" lsg,\n"
						+ "  \"web.tomcat\".\"SharedTomcat\" hst\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=lg.package\n"
						+ "  and lg.name=lsg.name\n"
						+ "  and lsg.id=hst.linux_server_group",
						username
					);
					break;
				case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Location(),
							"select * from web.\"Location\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Location(),
							"select\n"
							+ "  hsal.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"Location\" hsal\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hsal.httpd_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Location(),
						"select\n"
						+ "  hsal.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"Location\" hsal\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsal.httpd_site",
						username
					);
					break;
				case HTTPD_SITE_BIND_HEADERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Header(),
							"select * from web.\"Header\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Header(),
							"select\n"
							+ "  hsbh.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"VirtualHost\" hsb,\n"
							+ "  web.\"Header\" hsbh\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hsb.httpd_site\n"
							+ "  and hsb.id=hsbh.httpd_site_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Header(),
						"select\n"
						+ "  hsbh.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"VirtualHost\" hsb,\n"
						+ "  web.\"Header\" hsbh\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsb.httpd_site\n"
						+ "  and hsb.id=hsbh.httpd_site_bind",
						username
					);
					break;
				case HTTPD_SITE_BIND_REDIRECTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Redirect(),
							"select * from web.\"Redirect\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Redirect(),
							"select\n"
							+ "  hsbr.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"VirtualHost\" hsb,\n"
							+ "  web.\"Redirect\" hsbr\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hsb.httpd_site\n"
							+ "  and hsb.id=hsbr.httpd_site_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Redirect(),
						"select\n"
						+ "  hsbr.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"VirtualHost\" hsb,\n"
						+ "  web.\"Redirect\" hsbr\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsb.httpd_site\n"
						+ "  and hsb.id=hsbr.httpd_site_bind",
						username
					);
					break;
				case HTTPD_SITE_BINDS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualHost(),
							"select\n"
							+ "  hsb.*,\n"
							// Protocol conversion
							+ "  sc.cert_file  as ssl_cert_file,\n"
							+ "  sc.key_file   as ssl_cert_key_file,\n"
							+ "  sc.chain_file as ssl_cert_chain_file\n"
							+ "from\n"
							+ "  web.\"VirtualHost\" hsb\n"
							// Protocol conversion
							+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id"
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualHost(),
							"select\n"
							+ "  hsb.*,\n"
							// Protocol conversion
							+ "  sc.cert_file  as ssl_cert_file,\n"
							+ "  sc.key_file   as ssl_cert_key_file,\n"
							+ "  sc.chain_file as ssl_cert_chain_file\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"VirtualHost\" hsb\n"
							// Protocol conversion
							+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hsb.httpd_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new VirtualHost(),
						"select\n"
						+ "  hsb.*,\n"
						// Protocol conversion
						+ "  sc.cert_file  as ssl_cert_file,\n"
						+ "  sc.key_file   as ssl_cert_key_file,\n"
						+ "  sc.chain_file as ssl_cert_chain_file\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"VirtualHost\" hsb\n"
						// Protocol conversion
						+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsb.httpd_site",
						username
					);
					break;
				case HTTPD_SITE_URLS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualHostName(),
							"select * from web.\"VirtualHostName\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualHostName(),
							"select\n"
							+ "  hsu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"VirtualHost\" hsb,\n"
							+ "  web.\"VirtualHostName\" hsu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hsb.httpd_site\n"
							+ "  and hsb.id=hsu.httpd_site_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new VirtualHostName(),
						"select\n"
						+ "  hsu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"VirtualHost\" hsb,\n"
						+ "  web.\"VirtualHostName\" hsu\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hsb.httpd_site\n"
						+ "  and hsb.id=hsu.httpd_site_bind",
						username
					);
					break;
				case HTTPD_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Site(),
							"select * from web.\"Site\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Site(),
							"select\n"
							+ "  hs.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Site(),
						"select\n"
						+ "  hs.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package",
						username
					);
					break;
				case HTTPD_STATIC_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new StaticSite(),
							"select * from web.\"StaticSite\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new StaticSite(),
							"select\n"
							+ "  hss.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  web.\"StaticSite\" hss\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hss.httpd_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new StaticSite(),
						"select\n"
						+ "  hss.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  web.\"StaticSite\" hss\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hss.httpd_site",
						username
					);
					break;
				case HTTPD_TOMCAT_CONTEXTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Context(),
							"select * from \"web.tomcat\".\"Context\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Context(),
							"select\n"
							+ "  htc.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"Context\" htc\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htc.tomcat_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Context(),
						"select\n"
						+ "  htc.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"Context\" htc\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htc.tomcat_site",
						username
					);
					break;
				case HTTPD_TOMCAT_DATA_SOURCES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ContextDataSource(),
							"select * from \"web.tomcat\".\"ContextDataSource\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ContextDataSource(),
							"select\n"
							+ "  htds.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"Context\" htc,\n"
							+ "  \"web.tomcat\".\"ContextDataSource\" htds\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htc.tomcat_site\n"
							+ "  and htc.id=htds.tomcat_context",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ContextDataSource(),
						"select\n"
						+ "  htds.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"Context\" htc,\n"
						+ "  \"web.tomcat\".\"ContextDataSource\" htds\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htc.tomcat_site\n"
						+ "  and htc.id=htds.tomcat_context",
						username
					);
					break;
				case HTTPD_TOMCAT_PARAMETERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ContextParameter(),
							"select * from \"web.tomcat\".\"ContextParameter\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ContextParameter(),
							"select\n"
							+ "  htp.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"Context\" htc,\n"
							+ "  \"web.tomcat\".\"ContextParameter\" htp\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htc.tomcat_site\n"
							+ "  and htc.id=htp.tomcat_context",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ContextParameter(),
						"select\n"
						+ "  htp.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"Context\" htc,\n"
						+ "  \"web.tomcat\".\"ContextParameter\" htp\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htc.tomcat_site\n"
						+ "  and htc.id=htp.tomcat_context",
						username
					);
					break;
				case HTTPD_TOMCAT_SITE_JK_MOUNTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new JkMount(),
							"select * from \"web.tomcat\".\"JkMount\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new JkMount(),
							"select\n"
							+ "  htsjm.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"JkMount\" htsjm\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htsjm.httpd_tomcat_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new JkMount(),
						"select\n"
						+ "  htsjm.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"JkMount\" htsjm\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htsjm.httpd_tomcat_site",
						username
					);
					break;
				case HTTPD_TOMCAT_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.web.tomcat.Site(),
							"select\n"
							+ "  hts.*,\n"
							// Protocol conversion
							+ "  (\n"
							+ "    select htsjm.id from \"web.tomcat\".\"JkMount\" htsjm\n"
							+ "    where (htsjm.httpd_tomcat_site, htsjm.path)=(hts.httpd_site, '/*')\n"
							+ "  ) is null as use_apache\n"
							+ "from\n"
							+ "  \"web.tomcat\".\"Site\" hts"
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.web.tomcat.Site(),
							"select\n"
							+ "  hts.*,\n"
							// Protocol conversion
							+ "  (\n"
							+ "    select htsjm.id from \"web.tomcat\".\"JkMount\" htsjm\n"
							+ "    where (htsjm.httpd_tomcat_site, htsjm.path)=(hts.httpd_site, '/*')\n"
							+ "  ) is null as use_apache\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"Site\" hts\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=hts.httpd_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.tomcat.Site(),
						"select\n"
						+ "  hts.*,\n"
						// Protocol conversion
						+ "  (\n"
						+ "    select htsjm.id from \"web.tomcat\".\"JkMount\" htsjm\n"
						+ "    where (htsjm.httpd_tomcat_site, htsjm.path)=(hts.httpd_site, '/*')\n"
						+ "  ) is null as use_apache\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"Site\" hts\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=hts.httpd_site",
						username
					);
					break;
				case HTTPD_TOMCAT_SHARED_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new SharedTomcatSite(),
							"select * from \"web.tomcat\".\"SharedTomcatSite\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new SharedTomcatSite(),
							"select\n"
							+ "  htss.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"SharedTomcatSite\" htss\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htss.tomcat_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new SharedTomcatSite(),
						"select\n"
						+ "  htss.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"SharedTomcatSite\" htss\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htss.tomcat_site",
						username
					);
					break;
				case HTTPD_TOMCAT_STD_SITES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PrivateTomcatSite(),
							"select * from \"web.tomcat\".\"PrivateTomcatSite\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PrivateTomcatSite(),
							"select\n"
							+ "  htss.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  web.\"Site\" hs,\n"
							+ "  \"web.tomcat\".\"PrivateTomcatSite\" htss\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=hs.ao_server\n"
							+ "  and hs.id=htss.tomcat_site",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new PrivateTomcatSite(),
						"select\n"
						+ "  htss.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  web.\"Site\" hs,\n"
						+ "  \"web.tomcat\".\"PrivateTomcatSite\" htss\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=hs.package\n"
						+ "  and hs.id=htss.tomcat_site",
						username
					);
					break;
				case HTTPD_TOMCAT_VERSIONS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.web.tomcat.Version(),
						"select * from \"web.tomcat\".\"Version\""
					);
					break;
				// <editor-fold defaultstate="collapsed" desc="Httpd Workers">
				case HTTPD_WORKERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Worker(),
							"select\n"
							+ "  *\n"
							+ "from\n"
							+ "  \"web.tomcat\".\"Worker\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Worker(),
							"select\n"
							+ "  hw.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  \"web.tomcat\".\"Worker\" hw\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=nb.server\n"
							+ "  and nb.id=hw.bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Worker(),
						"select\n"
						+ "  hw.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  net.\"Bind\" nb,\n"
						+ "  \"web.tomcat\".\"Worker\" hw\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=nb.package\n"
						+ "  and nb.id=hw.bind",
						username
					);
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Addresses">
				case IP_ADDRESSES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddress(),
							"select\n"
							+ "  ia.id,\n"
							+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
							+ "  ia.device,\n"
							+ "  ia.\"isAlias\",\n"
							+ "  ia.hostname,\n"
							+ "  ia.package,\n"
							+ "  ia.created,\n"
							+ "  ia.\"isAvailable\",\n"
							+ "  ia.\"isOverflow\",\n"
							+ "  ia.\"isDhcp\",\n"
							+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
							+ "  ia.netmask,\n"
							// Protocol conversion
							+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
							+ "  iam.\"pingMonitorEnabled\",\n"
							+ "  iam.\"checkBlacklistsOverSmtp\",\n"
							+ "  iam.enabled as \"monitoringEnabled\"\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id"
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddress(),
							"select\n"
							+ "  ia.id,\n"
							+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
							+ "  ia.device,\n"
							+ "  ia.\"isAlias\",\n"
							+ "  ia.hostname,\n"
							+ "  ia.package,\n"
							+ "  ia.created,\n"
							+ "  ia.\"isAvailable\",\n"
							+ "  ia.\"isOverflow\",\n"
							+ "  ia.\"isDhcp\",\n"
							+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
							+ "  ia.netmask,\n"
							// Protocol conversion
							+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
							+ "  iam.\"pingMonitorEnabled\",\n"
							+ "  iam.\"checkBlacklistsOverSmtp\",\n"
							+ "  iam.enabled as \"monitoringEnabled\"\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
							+ "where\n"
							+ "  ia.id in (\n"
							+ "    select\n"
							+ "      ia2.id\n"
							+ "    from\n"
							+ "      master.\"UserHost\" ms\n"
							+ "      left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
							+ "      net.\"Device\" nd\n"
							+ "      right outer join net.\"IpAddress\" ia2 on nd.id=ia2.device\n"
							+ "    where\n"
							+ "      ia2.\"inetAddress\"='"+IpAddress.WILDCARD_IP+"' or (\n"
							+ "        ms.username=?\n"
							+ "        and (\n"
							+ "          ms.server=nd.server\n"
							+ "          or ff.server=nd.server\n"
							+ "          or (\n"
							+ "            select\n"
							+ "              ffr.id\n"
							+ "            from\n"
							+ "              backup.\"FileReplication\" ffr\n"
							+ "              inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
							+ "              inner join linux.\"Server\" bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
							+ "            where\n"
							+ "              ms.server=ffr.server\n"
							+ "              and bp.ao_server=nd.server\n"
							+ "              and bpao.\"daemonDeviceId\"=nd.\"deviceId\"\n" // Only allow access to the device device ID for failovers
							+ "            limit 1\n"
							+ "          ) is not null\n"
							+ "        )\n"
							+ "      )\n"
							+ "  )",
							username
						);
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddress(),
							"select\n"
							+ "  ia.id,\n"
							+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
							+ "  ia.device,\n"
							+ "  ia.\"isAlias\",\n"
							+ "  ia.hostname,\n"
							+ "  ia.package,\n"
							+ "  ia.created,\n"
							+ "  ia.\"isAvailable\",\n"
							+ "  ia.\"isOverflow\",\n"
							+ "  ia.\"isDhcp\",\n"
							+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
							+ "  ia.netmask,\n"
							// Protocol conversion
							+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
							+ "  iam.\"pingMonitorEnabled\",\n"
							+ "  iam.\"checkBlacklistsOverSmtp\",\n"
							+ "  iam.enabled as \"monitoringEnabled\"\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
							+ "where\n"
							+ "  ia.\"inetAddress\"='"+IpAddress.WILDCARD_IP+"'\n"
							+ "  or ia.id in (\n"
							+ "    select\n"
							+ "      ia2.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un1,\n"
							+ "      billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "      billing.\"Package\" pk2,\n"
							+ "      net.\"IpAddress\" ia2\n"
							+ "    where\n"
							+ "      un1.username=?\n"
							+ "      and un1.package=pk1.name\n"
							+ "      and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "      )\n"
							+ "      and bu1.accounting=pk2.accounting\n"
							+ "      and pk2.id=ia2.package\n"
							+ "  )\n"
							+ "  or ia.id in (\n"
							+ "    select\n"
							+ "      nb.\"ipAddress\"\n"
							+ "    from\n"
							+ "      account.\"Username\" un3,\n"
							+ "      billing.\"Package\" pk3,\n"
							+ BU2_PARENTS_JOIN
							+ "      billing.\"Package\" pk4,\n"
							+ "      web.\"Site\" hs,\n"
							+ "      web.\"VirtualHost\" hsb,\n"
							+ "      net.\"Bind\" nb\n"
							+ "    where\n"
							+ "      un3.username=?\n"
							+ "      and un3.package=pk3.name\n"
							+ "      and (\n"
							+ PK3_BU2_PARENTS_WHERE
							+ "      )\n"
							+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
							+ "      and pk4.name=hs.package\n"
							+ "      and hs.id=hsb.httpd_site\n"
							+ "      and hsb.httpd_bind=nb.id\n"
							+ "  ) or ia.id in (\n"
							+ "    select\n"
							+ "      ia5.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un5,\n"
							+ "      billing.\"Package\" pk5,\n"
							+ "      account.\"AccountHost\" bs5,\n"
							+ "      net.\"Device\" nd5,\n"
							+ "      net.\"IpAddress\" ia5\n"
							+ "    where\n"
							+ "      un5.username=?\n"
							+ "      and un5.package=pk5.name\n"
							+ "      and pk5.accounting=bs5.accounting\n"
							+ "      and bs5.server=nd5.server\n"
							+ "      and nd5.id=ia5.device\n"
							+ "      and (ia5.\"inetAddress\"='"+IpAddress.LOOPBACK_IP+"' or ia5.\"isOverflow\")\n"
							/*+ "  ) or ia.id in (\n"
							+ "    select \n"
							+ "      ia6.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un6,\n"
							+ "      billing.\"Package\" pk6,\n"
							+ "      account.\"AccountHost\" bs6,\n"
							+ "      backup.\"FileReplication\" ffr6,\n"
							+ "      backup.\"BackupPartition\" bp6,\n"
							+ "      linux.\"Server\" ao6,\n"
							+ "      net.\"Device\" nd6,\n"
							+ "      net.\"IpAddress\" ia6\n"
							+ "    where\n"
							+ "      un6.username=?\n"
							+ "      and un6.package=pk6.name\n"
							+ "      and pk6.accounting=bs6.accounting\n"
							+ "      and bs6.server=ffr6.server\n"
							+ "      and ffr6.backup_partition=bp6.id\n"
							+ "      and bp6.ao_server=ao6.server\n"
							+ "      and ao6.server=nd6.ao_server and ao6.\"daemonDeviceId\"=nd6.\"deviceId\"\n"
							+ "      and nd6.id=ia6.device and not ia6.\"isAlias\"\n"*/
							+ "  )",
							username,
							username,
							username//,
							//username
						);
					}
					break;
				case IpAddressMonitoring :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddressMonitoring(),
							"select\n"
							+ "  iam.*\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id"
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddressMonitoring(),
							"select\n"
							+ "  iam.*\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
							+ "where\n"
							+ "  ia.id in (\n"
							+ "    select\n"
							+ "      ia2.id\n"
							+ "    from\n"
							+ "      master.\"UserHost\" ms\n"
							+ "      left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
							+ "      net.\"Device\" nd\n"
							+ "      right outer join net.\"IpAddress\" ia2 on nd.id=ia2.device\n"
							+ "    where\n"
							+ "      ia2.\"inetAddress\"='"+IpAddress.WILDCARD_IP+"' or (\n"
							+ "        ms.username=?\n"
							+ "        and (\n"
							+ "          ms.server=nd.server\n"
							+ "          or ff.server=nd.server\n"
							+ "          or (\n"
							+ "            select\n"
							+ "              ffr.id\n"
							+ "            from\n"
							+ "              backup.\"FileReplication\" ffr\n"
							+ "              inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
							+ "              inner join linux.\"Server\" bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
							+ "            where\n"
							+ "              ms.server=ffr.server\n"
							+ "              and bp.ao_server=nd.server\n"
							+ "              and bpao.\"daemonDeviceId\"=nd.\"deviceId\"\n" // Only allow access to the device device ID for failovers
							+ "            limit 1\n"
							+ "          ) is not null\n"
							+ "        )\n"
							+ "      )\n"
							+ "  )",
							username
						);
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new IpAddressMonitoring(),
							"select\n"
							+ "  iam.*\n"
							+ "from\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
							+ "where\n"
							+ "  ia.\"inetAddress\"='"+IpAddress.WILDCARD_IP+"'\n"
							+ "  or ia.id in (\n"
							+ "    select\n"
							+ "      ia2.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un1,\n"
							+ "      billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "      billing.\"Package\" pk2,\n"
							+ "      net.\"IpAddress\" ia2\n"
							+ "    where\n"
							+ "      un1.username=?\n"
							+ "      and un1.package=pk1.name\n"
							+ "      and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "      )\n"
							+ "      and bu1.accounting=pk2.accounting\n"
							+ "      and pk2.id=ia2.package\n"
							+ "  )\n"
							+ "  or ia.id in (\n"
							+ "    select\n"
							+ "      nb.\"ipAddress\"\n"
							+ "    from\n"
							+ "      account.\"Username\" un3,\n"
							+ "      billing.\"Package\" pk3,\n"
							+ BU2_PARENTS_JOIN
							+ "      billing.\"Package\" pk4,\n"
							+ "      web.\"Site\" hs,\n"
							+ "      web.\"VirtualHost\" hsb,\n"
							+ "      net.\"Bind\" nb\n"
							+ "    where\n"
							+ "      un3.username=?\n"
							+ "      and un3.package=pk3.name\n"
							+ "      and (\n"
							+ PK3_BU2_PARENTS_WHERE
							+ "      )\n"
							+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
							+ "      and pk4.name=hs.package\n"
							+ "      and hs.id=hsb.httpd_site\n"
							+ "      and hsb.httpd_bind=nb.id\n"
							+ "  ) or ia.id in (\n"
							+ "    select\n"
							+ "      ia5.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un5,\n"
							+ "      billing.\"Package\" pk5,\n"
							+ "      account.\"AccountHost\" bs5,\n"
							+ "      net.\"Device\" nd5,\n"
							+ "      net.\"IpAddress\" ia5\n"
							+ "    where\n"
							+ "      un5.username=?\n"
							+ "      and un5.package=pk5.name\n"
							+ "      and pk5.accounting=bs5.accounting\n"
							+ "      and bs5.server=nd5.server\n"
							+ "      and nd5.id=ia5.device\n"
							+ "      and (ia5.\"inetAddress\"='"+IpAddress.LOOPBACK_IP+"' or ia5.\"isOverflow\")\n"
							/*+ "  ) or ia.id in (\n"
							+ "    select \n"
							+ "      ia6.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un6,\n"
							+ "      billing.\"Package\" pk6,\n"
							+ "      account.\"AccountHost\" bs6,\n"
							+ "      backup.\"FileReplication\" ffr6,\n"
							+ "      backup.\"BackupPartition\" bp6,\n"
							+ "      linux.\"Server\" ao6,\n"
							+ "      net.\"Device\" nd6,\n"
							+ "      net.\"IpAddress\" ia6\n"
							+ "    where\n"
							+ "      un6.username=?\n"
							+ "      and un6.package=pk6.name\n"
							+ "      and pk6.accounting=bs6.accounting\n"
							+ "      and bs6.server=ffr6.server\n"
							+ "      and ffr6.backup_partition=bp6.id\n"
							+ "      and bp6.ao_server=ao6.server\n"
							+ "      and ao6.server=nd6.ao_server and ao6.\"daemonDeviceId\"=nd6.\"deviceId\"\n"
							+ "      and nd6.id=ia6.device and not ia6.\"isAlias\"\n"*/
							+ "  )",
							username,
							username,
							username//,
							//username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Limiter Limits">
				case IP_REPUTATION_LIMITER_LIMITS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all limiters
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new LimiterClass(),
								"select * from \"net.reputation\".\"LimiterClass\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new LimiterClass(),
								"select distinct\n"
								+ "  irll.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\" se on ms.server=se.id\n"                         // Find all servers can access
								+ "  inner join net.\"Host\" se2 on se.farm=se2.farm\n"                         // Find all servers in the same farm
								+ "  inner join net.\"Device\" nd on se2.id=nd.server\n"                           // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\" irl on nd.id=irl.net_device\n"      // Find all limiters in the same farm
								+ "  inner join \"net.reputation\".\"LimiterClass\" irll on irl.id=irll.limiter\n" // Find all limiters limits in the same farm
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation limiters
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may access the limiters for servers they have direct access to
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new LimiterClass(),
							"select\n"
							+ "  irll.*\n"
							+ "from\n"
							+ "             account.\"Username\"                un\n"
							+ "  inner join billing.\"Package\"                 pk   on  un.package    =   pk.name\n"
							+ "  inner join account.\"AccountHost\"             bs   on  pk.accounting =   bs.accounting\n"
							+ "  inner join net.\"Device\"                      nd   on  bs.server     =   nd.server\n"
							+ "  inner join \"net.reputation\".\"Limiter\"      irl  on  nd.id       =  irl.net_device\n"
							+ "  inner join \"net.reputation\".\"LimiterClass\" irll on irl.id       = irll.limiter\n"
							+ "where\n"
							+ "  un.username=?",
							username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Limiter Sets">
				case IP_REPUTATION_LIMITER_SETS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all limiters
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Set(),
								"select * from \"net.reputation\".\"LimiterSet\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Set(),
								"select distinct\n"
								+ "  irls.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\" se on ms.server=se.id\n"                       // Find all servers can access
								+ "  inner join net.\"Host\" se2 on se.farm=se2.farm\n"                       // Find all servers in the same farm
								+ "  inner join net.\"Device\" nd on se2.id=nd.server\n"                         // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\" irl on nd.id=irl.net_device\n"    // Find all limiters in the same farm
								+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id=irls.limiter\n" // Find all limiters sets in the same farm
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation limiters
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may access the limiters for servers they have direct access to
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Set(),
							"select\n"
							+ "  irls.*\n"
							+ "from\n"
							+ "             account.\"Username\"              un\n"
							+ "  inner join billing.\"Package\"               pk   on  un.package    =   pk.name\n"
							+ "  inner join account.\"AccountHost\"           bs   on  pk.accounting =   bs.accounting\n"
							+ "  inner join net.\"Device\"                    nd   on  bs.server     =   nd.server\n"
							+ "  inner join \"net.reputation\".\"Limiter\"    irl  on  nd.id       =  irl.net_device\n"
							+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id       = irls.limiter\n"
							+ "where\n"
							+ "  un.username=?",
							username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Limiters">
				case IP_REPUTATION_LIMITERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all limiters
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Limiter(),
								"select * from \"net.reputation\".\"Limiter\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Limiter(),
								"select distinct\n"
								+ "  irl.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\" se on ms.server=se.id\n"                    // Find all servers can access
								+ "  inner join net.\"Host\" se2 on se.farm=se2.farm\n"                    // Find all servers in the same farm
								+ "  inner join net.\"Device\" nd on se2.id=nd.server\n"                      // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\" irl on nd.id=irl.net_device\n" // Find all limiters in the same farm
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation limiters
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may access the limiters for servers they have direct access to
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Limiter(),
							"select\n"
							+ "  irl.*\n"
							+ "from\n"
							+ "             account.\"Username\"           un\n"
							+ "  inner join billing.\"Package\"            pk  on un.package    =  pk.name\n"
							+ "  inner join account.\"AccountHost\"        bs  on pk.accounting =  bs.accounting\n"
							+ "  inner join net.\"Device\"                 nd  on bs.server     =  nd.server\n"
							+ "  inner join \"net.reputation\".\"Limiter\" irl on nd.id       = irl.net_device\n"
							+ "where\n"
							+ "  un.username=?",
							username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Set Hosts">
				case IP_REPUTATION_SET_HOSTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all sets
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Host(),
								"select * from \"net.reputation\".\"Host\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all sets used by any limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Host(),
								"select distinct\n"
								+ "  irsh.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\"                 se   on ms.server     = se.id\n"        // Find all servers can access
								+ "  inner join net.\"Host\"                 se2  on se.farm       = se2.farm\n"       // Find all servers in the same farm
								+ "  inner join net.\"Device\"                    nd   on se2.id      = nd.server\n"      // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\"    irl  on nd.id       = irl.net_device\n" // Find all limiters in the same farm
								+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id      = irls.limiter\n"   // Find all sets used by all limiters in the same farm
								+ "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\"  = irs.id\n"       // Find all sets used by any limiter in the same farm
								+ "  inner join \"net.reputation\".\"Host\"       irsh on irs.id      = irsh.\"set\"\n"   // Find all hosts belonging to these sets
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation sets
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may only access the hosts for their own or subaccount sets
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Host(),
							"select\n"
							+ "  irsh.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  \"net.reputation\".\"Set\" irs,\n"
							+ "  \"net.reputation\".\"Host\" irsh\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=irs.accounting\n"
							+ "  and irs.id=irsh.\"set\"",
							username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Set Networks">
				case IP_REPUTATION_SET_NETWORKS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all sets
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Network(),
								"select * from \"net.reputation\".\"Network\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all sets used by any limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Network(),
								"select distinct\n"
								+ "  irsn.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\"                 se   on ms.server     = se.id\n"        // Find all servers can access
								+ "  inner join net.\"Host\"                 se2  on se.farm       = se2.farm\n"       // Find all servers in the same farm
								+ "  inner join net.\"Device\"                    nd   on se2.id      = nd.server\n"      // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\"    irl  on nd.id       = irl.net_device\n" // Find all limiters in the same farm
								+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id      = irls.limiter\n"   // Find all sets used by all limiters in the same farm
								+ "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\"  = irs.id\n"       // Find all sets used by any limiter in the same farm
								+ "  inner join \"net.reputation\".\"Network\"    irsn on irs.id      = irsn.\"set\"\n"   // Find all networks belonging to these sets
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation sets
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may only access the networks for their own or subaccount sets
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Network(),
							"select\n"
							+ "  irsn.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  \"net.reputation\".\"Set\" irs,\n"
							+ "  \"net.reputation\".\"Network\" irsn\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk.name\n"
							+ "  and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=irs.accounting\n"
							+ "  and irs.id=irsn.\"set\"",
							username
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="IP Reputation Sets">
				case IP_REPUTATION_SETS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							// Admin may access all sets
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Set(),
								"select * from \"net.reputation\".\"Set\""
							);
						} else if(masterUser.isRouter()) {
							// Router may access all sets used by any limiters in the same server farm
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Set(),
								"select distinct\n"
								+ "  irs.*\n"
								+ "from\n"
								+ "  master.\"UserHost\" ms\n"
								+ "  inner join net.\"Host\"                 se   on ms.server     = se.id\n"        // Find all servers can access
								+ "  inner join net.\"Host\"                 se2  on se.farm       = se2.farm\n"       // Find all servers in the same farm
								+ "  inner join net.\"Device\"                    nd   on se2.id      = nd.server\n"      // Find all net.Device in the same farm
								+ "  inner join \"net.reputation\".\"Limiter\"    irl  on nd.id       = irl.net_device\n" // Find all limiters in the same farm
								+ "  inner join \"net.reputation\".\"LimiterSet\" irls on irl.id      = irls.limiter\n"   // Find all sets used by all limiters in the same farm
								+ "  inner join \"net.reputation\".\"Set\"        irs  on irls.\"set\"  = irs.id\n"       // Find all sets used by any limiter in the same farm
								+ "where\n"
								+ "  ms.username=?",
								username
							);
						} else {
							// Non-router daemon may not access any reputation sets
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						// Regular user may access their own or subaccount sets, as well as any parent account
						// set that allows subaccount use.
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Set(),
							"select\n"
							+ "  irs.*\n"
							+ "from\n"
							+ "  \"net.reputation\".\"Set\" irs\n"
							+ "where\n"
							// Allow own and any subaccount
							+ "  irs.id in (\n"
							+ "    select\n"
							+ "      irs2.id\n"
							+ "    from\n"
							+ "      account.\"Username\" un,\n"
							+ "      billing.\"Package\" pk,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "      \"net.reputation\".\"Set\" irs2\n"
							+ "    where\n"
							+ "      un.username=?\n"
							+ "      and un.package=pk.name\n"
							+ "      and (\n"
							+ TableHandler.PK_BU1_PARENTS_WHERE
							+ "      )\n"
							+ "      and bu1.accounting=irs2.accounting\n"
							+ "  )\n"
							// Allow any parent business that allow_subaccount_user
							+ "  or irs.id in (\n"
							+ "    select\n"
							+ "      irs3.id\n"
							+ "    from\n"
							+ "      \"net.reputation\".\"Set\" irs3\n"
							+ "    where\n"
							+ "      irs3.allow_subaccount_use\n"
							+ "      and account.is_account_or_parent(irs3.accounting, ?)\n"
							+ "  )",
							username,
							UsernameHandler.getBusinessForUsername(conn, username)
						);
					}
					break;
				// </editor-fold>
				// <editor-fold defaultstate="collapsed" desc="Languages">
				case LANGUAGES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Language(),
						"select * from ticket.\"Language\""
					);
					break;
				// </editor-fold>
				case LINUX_ACCOUNTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.linux.User(),
							"select * from linux.\"User\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.linux.User(),
							"select distinct\n"
							+ "  la.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  linux.\"Server\" ao\n"
							+ "  left join linux.\"Server\" ff on ao.server=ff.failover_server,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  linux.\"User\" la\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=ao.server\n"
							+ "  and (\n"
							+ "    ao.server=bs.server\n"
							+ "    or ff.server=bs.server\n"
							+ "  ) and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=la.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.linux.User(),
						"select\n"
						+ "  la.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  linux.\"User\" la\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username='"+com.aoindustries.aoserv.client.linux.User.MAIL+"'\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=la.username",
						username
					);
					break;
				case LINUX_ACCOUNT_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new UserType(),
						"select * from linux.\"UserType\""
					);
					break;
				case LINUX_GROUP_ACCOUNTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GroupUser(),
							"select * from linux.\"GroupUser\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GroupUser(),
							"select\n"
							+ "  *\n"
							+ "from\n"
							+ "  linux.\"GroupUser\"\n"
							+ "where\n"
							+ "  \"group\" in (\n"
							+ "    select\n"
							+ "      lsg.name\n"
							+ "      from\n"
							+ "        master.\"UserHost\" ms1,\n"
							+ "        linux.\"GroupServer\" lsg\n"
							+ "      where\n"
							+ "        ms1.username=?\n"
							+ "        and ms1.server=lsg.ao_server\n"
							+ "  )\n"
							+ "  and \"user\" in (\n"
							+ "    select\n"
							+ "      lsa.username\n"
							+ "      from\n"
							+ "        master.\"UserHost\" ms2,\n"
							+ "        linux.\"UserServer\" lsa\n"
							+ "      where\n"
							+ "        ms2.username=?\n"
							+ "        and ms2.server=lsa.ao_server\n"
							+ "  )",
							username,
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new GroupUser(),
						"select\n"
						+ " *\n"
						+ "from\n"
						+ "  linux.\"GroupUser\"\n"
						+ "where\n"
						+ "  \"group\" in (\n"
						+ "    select\n"
						+ "      lg.name\n"
						+ "    from\n"
						+ "      account.\"Username\" un1,\n"
						+ "      billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "      billing.\"Package\" pk2,\n"
						+ "      linux.\"Group\" lg\n"
						+ "    where\n"
						+ "      un1.username=?\n"
						+ "      and un1.package=pk1.name\n"
						+ "      and (\n"
						+ "        lg.name='"+Group.FTPONLY+"'\n"
						+ "        or lg.name='"+Group.MAIL+"'\n"
						+ "        or lg.name='"+Group.MAILONLY+"'\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "      )\n"
						+ "      and bu1.accounting=pk2.accounting\n"
						+ "      and pk2.name=lg.package\n"
						+ "  )\n"
						+ "  and \"user\" in (\n"
						+ "    select\n"
						+ "      la.username\n"
						+ "    from\n"
						+ "      account.\"Username\" un2,\n"
						+ "      billing.\"Package\" pk3,\n"
						+ BU2_PARENTS_JOIN
						+ "      billing.\"Package\" pk4,\n"
						+ "      account.\"Username\" un3,\n"
						+ "      linux.\"User\" la\n"
						+ "    where\n"
						+ "      un2.username=?\n"
						+ "      and un2.package=pk3.name\n"
						+ "      and (\n"
						+ "        un3.username='"+com.aoindustries.aoserv.client.linux.User.MAIL+"'\n"
						+ PK3_BU2_PARENTS_OR_WHERE
						+ "      )\n"
						+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
						+ "      and pk4.name=un3.package\n"
						+ "      and un3.username=la.username\n"
						+ "  )",
						username,
						username
					);
					break;
				case LINUX_GROUPS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Group(),
							"select * from linux.\"Group\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Group(),
							"select distinct\n"
							+ "  lg.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  linux.\"Server\" ao,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  linux.\"Group\" lg\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=ao.server\n"
							+ "  and ao.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=lg.package",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Group(),
						"select\n"
						+ "  lg.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  linux.\"Group\" lg\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ "    lg.name='"+Group.FTPONLY+"'\n"
						+ "    or lg.name='"+Group.MAIL+"'\n"
						+ "    or lg.name='"+Group.MAILONLY+"'\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=lg.package",
						username
					);
					break;
				case LINUX_GROUP_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new GroupType(),
						"select * from linux.\"GroupType\""
					);
					break;
				case LINUX_SERVER_ACCOUNTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserServer(),
							"select * from linux.\"UserServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserServer(),
							"select distinct\n"
							+ "  lsa.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
							+ "  linux.\"UserServer\" lsa\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and (\n"
							+ "    ms.server=lsa.ao_server\n"
							+ "    or ff.server=lsa.ao_server\n"
							+ "  )",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new UserServer(),
						"select\n"
						+ "  lsa.id,\n"
						+ "  lsa.username,\n"
						+ "  lsa.ao_server,\n"
						+ "  lsa.uid,\n"
						+ "  lsa.home,\n"
						+ "  lsa.autoresponder_from,\n"
						+ "  lsa.autoresponder_subject,\n"
						+ "  lsa.autoresponder_path,\n"
						+ "  lsa.is_autoresponder_enabled,\n"
						+ "  lsa.disable_log,\n"
						+ "  case when lsa.predisable_password is null then null else '"+AoservProtocol.FILTERED+"' end,\n"
						+ "  lsa.created,\n"
						+ "  lsa.use_inbox,\n"
						+ "  lsa.trash_email_retention,\n"
						+ "  lsa.junk_email_retention,\n"
						+ "  lsa.sa_integration_mode,\n"
						+ "  lsa.sa_required_score,\n"
						+ "  lsa.sa_discard_score,\n"
						+ "  lsa.sudo\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  account.\"AccountHost\" bs,\n"
						+ "  linux.\"UserServer\" lsa\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username='"+com.aoindustries.aoserv.client.linux.User.MAIL+"'\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and pk1.accounting=bs.accounting\n"
						+ "  and un2.username=lsa.username\n"
						+ "  and bs.server=lsa.ao_server",
						username
					);
					break;
				case LINUX_SERVER_GROUPS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GroupServer(),
							"select * from linux.\"GroupServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new GroupServer(),
							"select\n"
							+ "  lsg.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  linux.\"GroupServer\" lsg\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=lsg.ao_server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new GroupServer(),
						"select\n"
						+ "  lsg.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  linux.\"Group\" lg,\n"
						+ "  account.\"AccountHost\" bs,\n"
						+ "  linux.\"GroupServer\" lsg\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ "    lg.name='"+Group.FTPONLY+"'\n"
						+ "    or lg.name='"+Group.MAIL+"'\n"
						+ "    or lg.name='"+Group.MAILONLY+"'\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=lg.package\n"
						+ "  and pk1.accounting=bs.accounting\n"
						+ "  and lg.name=lsg.name\n"
						+ "  and bs.server=lsg.ao_server",
						username
					);
					break;
				case MASTER_HOSTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserAcl(),
							"select * from master.\"UserAcl\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserAcl(),
							"select distinct\n"
							+ "  mh.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  master.\"UserAcl\" mh\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=mh.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new UserAcl(),
						"select\n"
						+ "  mh.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  master.\"UserAcl\" mh\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username=un1.username\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=mh.username",
						username
					);
					break;
				case MASTER_PROCESSES :
					MasterProcessManager.writeProcesses(
						conn,
						out,
						provideProgress,
						source,
						masterUser,
						masterServers
					);
					break;
				case MASTER_SERVER_STATS :
					MasterServer.writeStats(
						source,
						out,
						provideProgress
					);
					break;
				case MASTER_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserHost(),
							"select * from master.\"UserHost\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new UserHost(),
							"select\n"
							+ "  ms2.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms1,\n"
							+ "  master.\"UserHost\" ms2\n"
							+ "where\n"
							+ "  ms1.username=?\n"
							+ "  and ms1.server=ms2.server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new UserHost(),
						"select\n"
						+ "  ms.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  master.\"UserHost\" ms\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username=un1.username\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=ms.username",
						username
					);
					break;
				case MASTER_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new User(),
							"select * from master.\"User\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new User(),
							"select distinct\n"
							+ "  mu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  master.\"User\" mu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=mu.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new User(),
						"select\n"
						+ "  mu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  master.\"User\" mu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ "    un2.username=un1.username\n"
						+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=mu.username",
						username
					);
					break;
				case MYSQL_DATABASES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Database(),
							"select * from mysql.\"Database\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Database(),
							"select\n"
							+ "  md.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  mysql.\"Server\" mys,\n"
							+ "  mysql.\"Database\" md\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=mys.ao_server\n"
							+ "  and mys.bind=md.mysql_server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Database(),
						"select\n"
						+ "  md.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  mysql.\"Database\" md\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=md.package",
						username
					);
					break;
				case MYSQL_DB_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new DatabaseUser(),
							"select * from mysql.\"DatabaseUser\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new DatabaseUser(),
							"select\n"
							+ "  mdu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  mysql.\"Server\" mys,\n"
							+ "  mysql.\"Database\" md,\n"
							+ "  mysql.\"DatabaseUser\" mdu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=mys.ao_server\n"
							+ "  and mys.bind=md.mysql_server\n"
							+ "  and md.id=mdu.mysql_database",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new DatabaseUser(),
						"select\n"
						+ "  mdu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  mysql.\"Database\" md,\n"
						+ "  mysql.\"DatabaseUser\" mdu\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=md.package\n"
						+ "  and md.id=mdu.mysql_database",
						username
					);
					break;
				case MYSQL_SERVER_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.UserServer(),
							"select * from mysql.\"UserServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.UserServer(),
							"select\n"
							+ "  msu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  mysql.\"Server\" mys,\n"
							+ "  mysql.\"UserServer\" msu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=mys.ao_server\n"
							+ "  and mys.bind=msu.mysql_server",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.mysql.UserServer(),
						 "select\n"
						+ "  msu.id,\n"
						+ "  msu.username,\n"
						+ "  msu.mysql_server,\n"
						+ "  msu.host,\n"
						+ "  msu.disable_log,\n"
						+ "  case when msu.predisable_password is null then null else '"+AoservProtocol.FILTERED+"' end,\n"
						+ "  msu.max_questions,\n"
						+ "  msu.max_updates\n,"
						+ "  msu.max_connections,\n"
						+ "  msu.max_user_connections\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  mysql.\"UserServer\" msu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=msu.username",
						username
					);
					break;
				case MYSQL_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.Server(),
							"SELECT\n"
							+ "  ms.*,\n"
							// Protocol conversion
							+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
							+ "FROM\n"
							+ "  mysql.\"Server\" ms"
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.Server(),
							"SELECT\n"
							+ "  ms.*,\n"
							// Protocol conversion
							+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
							+ "from\n"
							+ "             master.\"UserHost\" uh\n"
							+ "  INNER JOIN mysql.\"Server\"    ms ON uh.server=ms.ao_server\n"
							+ "where\n"
							+ "  uh.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.mysql.Server(),
						"SELECT\n"
						+ "  ms.*,\n"
						// Protocol conversion
						+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
						+ "FROM\n"
						+ "             account.\"Username\"    un\n"
						+ "  INNER JOIN billing.\"Package\"     pk ON un.package    = pk.name\n"
						+ "  INNER JOIN account.\"AccountHost\" bs ON pk.accounting = bs.accounting\n"
						+ "  INNER JOIN mysql.\"Server\"        ms ON bs.server     = ms.ao_server\n"
						+ "WHERE\n"
						+ "  un.username=?",
						username
					);
					break;
				case MYSQL_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.User(),
							"select * from mysql.\"User\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.mysql.User(),
							"select distinct\n"
							+ "  mu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  mysql.\"User\" mu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=mu.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.mysql.User(),
						"select\n"
						+ "  mu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  mysql.\"User\" mu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=mu.username",
						username
					);
					break;
				case NET_BIND_FIREWALLD_ZONES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new BindFirewallZone(),
							"select * from net.\"BindFirewallZone\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new BindFirewallZone(),
							"select\n"
							+ "  nbfz.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  net.\"BindFirewallZone\" nbfz\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=nb.server\n"
							+ "  and nb.id=nbfz.net_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new BindFirewallZone(),
						"select\n"
						+ "  nbfz.*\n"
						+ "from\n"
						+ "  net.\"Bind\" nb\n"
						+ "  inner join net.\"BindFirewallZone\" nbfz on nb.id=nbfz.net_bind\n"
						+ "where\n"
						+ "  nb.id in (\n"
						+ "    select\n"
						+ "      nb2.id\n"
						+ "    from\n"
						+ "      account.\"Username\" un1,\n"
						+ "      billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "      billing.\"Package\" pk2,\n"
						+ "      net.\"Bind\" nb2\n"
						+ "    where\n"
						+ "      un1.username=?\n"
						+ "      and un1.package=pk1.name\n"
						+ "      and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "      )\n"
						+ "      and bu1.accounting=pk2.accounting\n"
						+ "      and pk2.name=nb2.package\n"
						+ "  )\n"
						+ "  or nb.id in (\n"
						+ "    select\n"
						+ "      nb3.id\n"
						+ "    from\n"
						+ "      account.\"Username\" un3,\n"
						+ "      billing.\"Package\" pk3,\n"
						+ BU2_PARENTS_JOIN
						+ "      billing.\"Package\" pk4,\n"
						+ "      web.\"Site\" hs,\n"
						+ "      web.\"VirtualHost\" hsb,\n"
						+ "      net.\"Bind\" nb3\n"
						+ "    where\n"
						+ "      un3.username=?\n"
						+ "      and un3.package=pk3.name\n"
						+ "      and (\n"
						+ PK3_BU2_PARENTS_WHERE
						+ "      )\n"
						+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
						+ "      and pk4.name=hs.package\n"
						+ "      and hs.id=hsb.httpd_site\n"
						+ "      and hsb.httpd_bind=nb3.id\n"
						+ "  ) or nb.id in (\n"
						+ "    select\n"
						+ "      ms4.bind\n"
						+ "    from\n"
						+ "      account.\"Username\" un4,\n"
						+ "      billing.\"Package\" pk4,\n"
						+ "      account.\"AccountHost\" bs4,\n"
						+ "      mysql.\"Server\" ms4\n"
						+ "    where\n"
						+ "      un4.username=?\n"
						+ "      and un4.package=pk4.name\n"
						+ "      and pk4.accounting=bs4.accounting\n"
						+ "      and bs4.server=ms4.ao_server\n"
						+ "  ) or nb.id in (\n"
						+ "    select\n"
						+ "      ps5.bind\n"
						+ "    from\n"
						+ "      account.\"Username\" un5,\n"
						+ "      billing.\"Package\" pk5,\n"
						+ "      account.\"AccountHost\" bs5,\n"
						+ "      postgresql.\"Server\" ps5\n"
						+ "    where\n"
						+ "      un5.username=?\n"
						+ "      and un5.package=pk5.name\n"
						+ "      and pk5.accounting=bs5.accounting\n"
						+ "      and bs5.server=ps5.ao_server\n"
						+ "  )",
						username,
						username,
						username,
						username
					);
					break;
				case NET_BINDS :
					// TODO: Only do inner joins for open_firewall for clients 1.80.2 and older?
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Bind(),
							"select\n"
							+ "  nb.*,\n"
							// Protocol conversion
							+ "  (\n"
							+ "    select\n"
							+ "      nbfz.id\n"
							+ "    from\n"
							+ "      net.\"BindFirewallZone\" nbfz\n"
							+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
							+ "    where\n"
							+ "      nb.id=nbfz.net_bind\n"
							+ "      and fz.\"name\"=?\n"
							+ "  ) is not null as open_firewall\n"
							+ "from\n"
							+ "  net.\"Bind\" nb",
							FirewallZone.PUBLIC
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Bind(),
							"select\n"
							+ "  nb.id,\n"
							+ "  nb.package,\n"
							+ "  nb.server,\n"
							+ "  nb.\"ipAddress\",\n"
							+ "  nb.port,\n"
							+ "  nb.net_protocol,\n"
							+ "  nb.app_protocol,\n"
							+ "  nb.monitoring_enabled,\n"
							+ "  case when nb.monitoring_parameters is null then null::text else '"+AoservProtocol.FILTERED+"'::text end as monitoring_parameters,\n"
							// Protocol conversion
							+ "  (\n"
							+ "    select\n"
							+ "      nbfz.id\n"
							+ "    from\n"
							+ "      net.\"BindFirewallZone\" nbfz\n"
							+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
							+ "    where\n"
							+ "      nb.id=nbfz.net_bind\n"
							+ "      and fz.\"name\"=?\n"
							+ "  ) is not null as open_firewall\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and (\n"
							+ "    ms.server=nb.server\n"
							+ "    or (\n"
							+ "      select\n"
							+ "        ffr.id\n"
							+ "      from\n"
							+ "        backup.\"FileReplication\" ffr\n"
							+ "        inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
							+ "      where\n"
							+ "        ms.server=ffr.server\n"
							+ "        and bp.ao_server=nb.server\n"
							+ "        and (\n"
							+ "          nb.app_protocol='"+AppProtocol.AOSERV_DAEMON+"'\n"
							+ "          or nb.app_protocol='"+AppProtocol.AOSERV_DAEMON_SSL+"'\n"
							+ "        )\n"
							+ "      limit 1\n"
							+ "    ) is not null\n"
							+ "  )",
							FirewallZone.PUBLIC,
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Bind(),
						"select\n"
						+ "  nb.id,\n"
						+ "  nb.package,\n"
						+ "  nb.server,\n"
						+ "  nb.\"ipAddress\",\n"
						+ "  nb.port,\n"
						+ "  nb.net_protocol,\n"
						+ "  nb.app_protocol,\n"
						+ "  nb.monitoring_enabled,\n"
						+ "  case when nb.monitoring_parameters is null then null::text else '"+AoservProtocol.FILTERED+"'::text end as monitoring_parameters,\n"
						// Protocol conversion
						+ "  (\n"
						+ "    select\n"
						+ "      nbfz.id\n"
						+ "    from\n"
						+ "      net.\"BindFirewallZone\" nbfz\n"
						+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
						+ "    where\n"
						+ "      nb.id=nbfz.net_bind\n"
						+ "      and fz.\"name\"=?\n"
						+ "  ) is not null as open_firewall\n"
						+ "from\n"
						+ "  net.\"Bind\" nb\n"
						+ "where\n"
						+ "  nb.id in (\n"
						+ "    select\n"
						+ "      nb2.id\n"
						+ "    from\n"
						+ "      account.\"Username\" un1,\n"
						+ "      billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "      billing.\"Package\" pk2,\n"
						+ "      net.\"Bind\" nb2\n"
						+ "    where\n"
						+ "      un1.username=?\n"
						+ "      and un1.package=pk1.name\n"
						+ "      and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "      )\n"
						+ "      and bu1.accounting=pk2.accounting\n"
						+ "      and pk2.name=nb2.package\n"
						+ "  )\n"
						+ "  or nb.id in (\n"
						+ "    select\n"
						+ "      nb3.id\n"
						+ "    from\n"
						+ "      account.\"Username\" un3,\n"
						+ "      billing.\"Package\" pk3,\n"
						+ BU2_PARENTS_JOIN
						+ "      billing.\"Package\" pk4,\n"
						+ "      web.\"Site\" hs,\n"
						+ "      web.\"VirtualHost\" hsb,\n"
						+ "      net.\"Bind\" nb3\n"
						+ "    where\n"
						+ "      un3.username=?\n"
						+ "      and un3.package=pk3.name\n"
						+ "      and (\n"
						+ PK3_BU2_PARENTS_WHERE
						+ "      )\n"
						+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
						+ "      and pk4.name=hs.package\n"
						+ "      and hs.id=hsb.httpd_site\n"
						+ "      and hsb.httpd_bind=nb3.id\n"
						+ "  ) or nb.id in (\n"
						+ "    select\n"
						+ "      ms4.bind\n"
						+ "    from\n"
						+ "      account.\"Username\" un4,\n"
						+ "      billing.\"Package\" pk4,\n"
						+ "      account.\"AccountHost\" bs4,\n"
						+ "      mysql.\"Server\" ms4\n"
						+ "    where\n"
						+ "      un4.username=?\n"
						+ "      and un4.package=pk4.name\n"
						+ "      and pk4.accounting=bs4.accounting\n"
						+ "      and bs4.server=ms4.ao_server\n"
						+ "  ) or nb.id in (\n"
						+ "    select\n"
						+ "      ps5.bind\n"
						+ "    from\n"
						+ "      account.\"Username\" un5,\n"
						+ "      billing.\"Package\" pk5,\n"
						+ "      account.\"AccountHost\" bs5,\n"
						+ "      postgresql.\"Server\" ps5\n"
						+ "    where\n"
						+ "      un5.username=?\n"
						+ "      and un5.package=pk5.name\n"
						+ "      and pk5.accounting=bs5.accounting\n"
						+ "      and bs5.server=ps5.ao_server\n"
						/*+ "  ) or nb.id in (\n"
						// Allow net.Bind of receiving backup.FileReplication (exact package match - no tree inheritence)
						+ "    select\n"
						+ "      nb6.id\n"
						+ "    from\n"
						+ "      account.\"Username\" un6,\n"
						+ "      billing.\"Package\" pk6,\n"
						+ "      net.\"Host\" se6,\n"
						+ "      backup.\"FileReplication\" ffr6,\n"
						+ "      backup.\"BackupPartition\" bp6,\n"
						+ "      net.\"Bind\" nb6\n"
						+ "    where\n"
						+ "      un6.username=?\n"
						+ "      and un6.package=pk6.name\n"
						+ "      and pk6.id=se6.package\n"
						+ "      and se6.id=ffr6.server\n"
						+ "      and ffr6.backup_partition=bp6.id\n"
						+ "      and bp6.ao_server=nb6.ao_server\n"
						+ "      and (\n"
						+ "        nb6.app_protocol='"+Protocol.AOSERV_DAEMON+"'\n"
						+ "        or nb6.app_protocol='"+Protocol.AOSERV_DAEMON_SSL+"'\n"
						+ "      )\n"*/
						+ "  )",
						FirewallZone.PUBLIC,
						username,
						username,
						username,
						username//,
						//username
					);
					break;
				case NET_DEVICE_IDS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new DeviceId(),
						"select * from net.\"DeviceId\""
					);
					break;
				case NET_DEVICES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Device(),
							"select"
							+ "  id,\n"
							+ "  server,\n"
							+ "  \"deviceId\",\n"
							+ "  description,\n"
							+ "  delete_route,\n"
							+ "  host(gateway) as gateway,\n"
							+ "  host(network) as network,\n"
							+ "  host(broadcast) as broadcast,\n"
							+ "  mac_address::text,\n"
							+ "  max_bit_rate,\n"
							+ "  monitoring_bit_rate_low,\n"
							+ "  monitoring_bit_rate_medium,\n"
							+ "  monitoring_bit_rate_high,\n"
							+ "  monitoring_bit_rate_critical,\n"
							+ "  monitoring_enabled\n"
							+ "from\n"
							+ "  net.\"Device\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Device(),
							"select distinct\n"
							+ "  nd.id,\n"
							+ "  nd.server,\n"
							+ "  nd.\"deviceId\",\n"
							+ "  nd.description,\n"
							+ "  nd.delete_route,\n"
							+ "  host(nd.gateway) as gateway,\n"
							+ "  host(nd.network) as network,\n"
							+ "  host(nd.broadcast) as broadcast,\n"
							+ "  nd.mac_address::text,\n"
							+ "  nd.max_bit_rate,\n"
							+ "  nd.monitoring_bit_rate_low,\n"
							+ "  nd.monitoring_bit_rate_medium,\n"
							+ "  nd.monitoring_bit_rate_high,\n"
							+ "  nd.monitoring_bit_rate_critical,\n"
							+ "  nd.monitoring_enabled\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
							+ "  net.\"Device\" nd\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and (\n"
							+ "    ms.server=nd.server\n"
							+ "    or ff.server=nd.server\n"
							+ "    or (\n"
							+ "      select\n"
							+ "        ffr.id\n"
							+ "      from\n"
							+ "        backup.\"FileReplication\" ffr\n"
							+ "        inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
							+ "        inner join linux.\"Server\" bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
							+ "      where\n"
							+ "        ms.server=ffr.server\n"
							+ "        and bp.ao_server=nd.server\n"
							+ "        and bpao.\"daemonDeviceId\"=nd.\"deviceId\"\n" // Only allow access to the device device ID for failovers
							+ "      limit 1\n"
							+ "    ) is not null\n"
							+ "  )",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Device(),
						"select\n" // distinct
						+ "  nd.id,\n"
						+ "  nd.server,\n"
						+ "  nd.\"deviceId\",\n"
						+ "  nd.description,\n"
						+ "  nd.delete_route,\n"
						+ "  host(nd.gateway) as gateway,\n"
						+ "  host(nd.network) as network,\n"
						+ "  host(nd.broadcast) as broadcast,\n"
						+ "  nd.mac_address::text,\n"
						+ "  nd.max_bit_rate,\n"
						+ "  nd.monitoring_bit_rate_low,\n"
						+ "  nd.monitoring_bit_rate_medium,\n"
						+ "  nd.monitoring_bit_rate_high,\n"
						+ "  nd.monitoring_bit_rate_critical,\n"
						+ "  nd.monitoring_enabled\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow failover destinations
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
						//+ "  left join linux.\"Server\" bpao on bp.ao_server=bpao.server,\n"
						+ "  net.\"Device\" nd\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=nd.server\n"
						// Need distinct above when using this or
						//+ "    or (bp.ao_server=nd.ao_server and nd.\"deviceId\"=bpao.\"daemonDeviceId\")\n"
						+ "  )",
						username
					);
					break;
				case NET_TCP_REDIRECTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new TcpRedirect(),
							"select * from net.\"TcpRedirect\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new TcpRedirect(),
							"select\n"
							+ "  ntr.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  net.\"TcpRedirect\" ntr\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=nb.server\n"
							+ "  and nb.id=ntr.net_bind",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new TcpRedirect(),
						"select\n"
						+ "  ntr.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  net.\"Bind\" nb,\n"
						+ "  net.\"TcpRedirect\" ntr\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=nb.package\n"
						+ "  and nb.id=ntr.net_bind",
						username
					);
					break;
				case PAYMENT_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new PaymentType(),
						"select * from payment.\"PaymentType\""
					);
					break;
				case PHYSICAL_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PhysicalServer(),
							"select * from infrastructure.\"PhysicalServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PhysicalServer(),
							"select\n"
							+ "  ps.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join infrastructure.\"PhysicalServer\" ps on ms.server=ps.server\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new PhysicalServer(),
						"select distinct\n"
						+ "  ps.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  infrastructure.\"PhysicalServer\" ps\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=ps.server\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=ps.server\n"
						+ "  )",
						username
					);
					break;
				case POSTGRES_DATABASES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.Database(),
							"select * from postgresql.\"Database\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.Database(),
							"SELECT\n"
							+ "  pd.*\n"
							+ "FROM\n"
							+ "             master.\"UserHost\"     ms\n"
							+ "  INNER JOIN postgresql.\"Server\"   ps ON ms.server = ps.ao_server\n"
							+ "  INNER JOIN postgresql.\"Database\" pd ON ps.bind   = pd.postgres_server\n"
							+ "WHERE\n"
							+ "  ms.username = ?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.postgresql.Database(),
						"select\n"
						+ "  pd.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  postgresql.\"UserServer\" psu,\n"
						+ "  postgresql.\"Database\" pd\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=psu.username\n"
						+ "  and psu.id=pd.datdba",
						username
					);
					break;
				case POSTGRES_ENCODINGS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Encoding(),
						"select * from postgresql.\"Encoding\""
					);
					break;
				case POSTGRES_SERVER_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.UserServer(),
							"select * from postgresql.\"UserServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.UserServer(),
							"SELECT\n"
							+ "  psu.*\n"
							+ "FROM\n"
							+ "             master.\"UserHost\"       ms\n"
							+ "  INNER JOIN postgresql.\"Server\"     ps  ON ms.server =  ps.ao_server\n"
							+ "  INNER JOIN postgresql.\"UserServer\" psu ON ps.bind   = psu.postgres_server\n"
							+ "where\n"
							+ "  ms.username = ?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.postgresql.UserServer(),
						"select\n"
						+ "  psu.id,\n"
						+ "  psu.username,\n"
						+ "  psu.postgres_server,\n"
						+ "  psu.disable_log,\n"
						+ "  case when psu.predisable_password is null then null else '"+AoservProtocol.FILTERED+"' end\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  postgresql.\"UserServer\" psu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=psu.username",
						username
					);
					break;
				case POSTGRES_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.Server(),
							"SELECT\n"
							+ "  *\n"
							+ "FROM\n"
							+ "  postgresql.\"Server\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.Server(),
							"SELECT\n"
							+ "  ps.*\n"
							+ "FROM\n"
							+ "             master.\"UserHost\"   ms\n"
							+ "  INNER JOIN postgresql.\"Server\" ps ON ms.server = ps.ao_server\n"
							+ "WHERE\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.postgresql.Server(),
						"SELECT\n"
						+ "  ps.*\n"
						+ "FROM\n"
						+ "             account.\"Username\"    un\n"
						+ "  INNER JOIN billing.\"Package\"     pk ON un.package    = pk.name\n"
						+ "  INNER JOIN account.\"AccountHost\" bs ON pk.accounting = bs.accounting\n"
						+ "  INNER JOIN postgresql.\"Server\"   ps ON bs.server     = ps.ao_server\n"
						+ "WHERE\n"
						+ "  un.username = ?",
						username
					);
					break;
				case POSTGRES_USERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.User(),
							"select * from postgresql.\"User\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new com.aoindustries.aoserv.client.postgresql.User(),
							"select distinct\n"
							+ "  pu.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  account.\"AccountHost\" bs,\n"
							+ "  billing.\"Package\" pk,\n"
							+ "  account.\"Username\" un,\n"
							+ "  postgresql.\"User\" pu\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=bs.server\n"
							+ "  and bs.accounting=pk.accounting\n"
							+ "  and pk.name=un.package\n"
							+ "  and un.username=pu.username",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new com.aoindustries.aoserv.client.postgresql.User(),
						"select\n"
						+ "  pu.*\n"
						+ "from\n"
						+ "  account.\"Username\" un1,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  account.\"Username\" un2,\n"
						+ "  postgresql.\"User\" pu\n"
						+ "where\n"
						+ "  un1.username=?\n"
						+ "  and un1.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.name=un2.package\n"
						+ "  and un2.username=pu.username",
						username
					);
					break;
				case POSTGRES_VERSIONS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Version(),
						"select * from postgresql.\"Version\""
					);
					break;
				case PRIVATE_FTP_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PrivateServer(),
							"select * from ftp.\"PrivateServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PrivateServer(),
							"select\n"
							+ "  pfs.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  ftp.\"PrivateServer\" pfs\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=nb.server\n"
							+ "  and nb.id=pfs.net_bind",
							username
						);
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new PrivateServer(),
							"select\n"
							+ "  pfs.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  billing.\"Package\" pk2,\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  ftp.\"PrivateServer\" pfs\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk1.name\n"
							+ "  and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=pk2.accounting\n"
							+ "  and pk2.name=nb.package\n"
							+ "  and nb.id=pfs.net_bind",
							username
						);
					}
					break;
				case PROCESSOR_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ProcessorType(),
						"select * from infrastructure.\"ProcessorType\""
					);
					break;
				case PROTOCOLS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new AppProtocol(),
						"select * from net.\"AppProtocol\""
					);
					break;
				case RACKS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Rack(),
							"select * from infrastructure.\"Rack\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Rack(),
							"select distinct\n"
							+ "  ra.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join infrastructure.\"PhysicalServer\" ps on ms.server=ps.server\n"
							+ "  inner join infrastructure.\"Rack\" ra on ps.rack=ra.id\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Rack(),
						"select distinct\n"
						+ "  ra.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  infrastructure.\"PhysicalServer\" ps,\n"
						+ "  infrastructure.\"Rack\" ra\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=ps.server\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=ps.server\n"
						+ "  ) and ps.rack=ra.id",
						username
					);
					break;
				case RESELLERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Reseller(),
							"select * from reseller.\"Reseller\""
						); else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Reseller(),
						"select\n"
						+ "  re.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  reseller.\"Reseller\" re\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and (\n"
						+ TableHandler.PK_BU1_PARENTS_WHERE
						+ "  ) and bu1.accounting=re.accounting",
						username
					);
					break;
				case SCHEMA_COLUMNS :
					{
						List<Column> clientColumns=new ArrayList<>();
						PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
							"select\n"
							+ "  sc.id,\n"
							+ "  st.\"name\" as \"table\",\n"
							+ "  sc.\"name\",\n"
							+ "  sc.\"sinceVersion\",\n"
							+ "  sc.\"lastVersion\",\n"
							+ "  sc.index,\n"
							+ "  ty.\"name\" as \"type\",\n"
							+ "  sc.\"isNullable\",\n"
							+ "  sc.\"isUnique\",\n"
							+ "  sc.\"isPublic\",\n"
							+ "  coalesce(sc.description, d.description, '') as description\n"
							+ "from\n"
							+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
							+ "             \"schema\".\"Column\"              sc\n"
							+ "  inner join \"schema\".\"Table\"               st on sc.\"table\"        =      st.id\n"
							+ "  inner join \"schema\".\"Schema\"               s on st.\"schema\"       =       s.id\n"
							+ "  inner join \"schema\".\"Type\"                ty on sc.\"type\"         =      ty.id\n"
							+ "  inner join \"schema\".\"AoservProtocol\"   sc_ap on sc.\"sinceVersion\" =   sc_ap.version\n"
							+ "  left  join \"schema\".\"AoservProtocol\" last_ap on sc.\"lastVersion\"  = last_ap.version\n"
							+ "  left  join (\n"
							+ "    select\n"
							+ "      pn.nspname, pc.relname, pa.attname, pd.description\n"
							+ "    from\n"
							+ "                 pg_catalog.pg_namespace   pn\n"
							+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
							+ "      inner join pg_catalog.pg_attribute   pa on pc.oid = pa.attrelid\n"
							+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid = pa.attnum\n"
							+ "  ) d on (s.\"name\", st.\"name\", sc.\"name\") = (d.nspname, d.relname, d.attname)\n"
							+ "where\n"
							+ "  client_ap.version=?\n"
							+ "  and client_ap.created >= sc_ap.created\n"
							+ "  and (last_ap.created is null or client_ap.created <= last_ap.created)\n"
							+ "order by\n"
							+ "  st.id,\n"
							+ "  sc.index"
						);
						try {
							pstmt.setString(1, source.getProtocolVersion().getVersion());

							ResultSet results=pstmt.executeQuery();
							try {
								short clientColumnIndex = 0;
								String lastTableName=null;
								Column tempSC=new Column();
								while(results.next()) {
									tempSC.init(results);
									// Change the table ID if on next table
									String tableName = tempSC.getTable_name();
									if(lastTableName==null || !lastTableName.equals(tableName)) {
										clientColumnIndex = 0;
										lastTableName=tableName;
									}
									clientColumns.add(
										new Column(
											tempSC.getPkey(),
											tableName,
											tempSC.getName(),
											tempSC.getSinceVersion_version(),
											tempSC.getLastVersion_version(),
											clientColumnIndex++,
											tempSC.getType_name(),
											tempSC.isNullable(),
											tempSC.isUnique(),
											tempSC.isPublic(),
											tempSC.getDescription()
										)
									);
								}
							} finally {
								results.close();
							}
						} catch(SQLException err) {
							System.err.println("Error from query: "+pstmt.toString());
							throw err;
						} finally {
							pstmt.close();
						}
						MasterServer.writeObjects(
							source,
							out,
							provideProgress,
							clientColumns
						);
					}
					break;
				case SCHEMA_FOREIGN_KEYS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ForeignKey(),
						"select\n"
						+ "  sfk.*\n"
						+ "from\n"
						+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
						+ "  \"schema\".\"ForeignKey\" sfk\n"
						+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on sfk.\"sinceVersion\"=\"sinceVersion\".version\n"
						+ "  left join \"schema\".\"AoservProtocol\" \"lastVersion\" on sfk.\"lastVersion\"=\"lastVersion\".version\n"
						+ "where\n"
						+ "  client_ap.version=?\n"
						+ "  and client_ap.created >= \"sinceVersion\".created\n"
						+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)",
						source.getProtocolVersion().getVersion()
					);
					break;
				case SCHEMA_TABLES :
					{
						List<Table> clientTables=new ArrayList<>();
						PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
							"select\n"
							+ "  st.id,\n"
							+ "  st.\"name\",\n"
							+ "  st.\"sinceVersion\",\n"
							+ "  st.\"lastVersion\",\n"
							+ "  st.display,\n"
							+ "  st.\"isPublic\",\n"
							+ "  coalesce(st.description, d.description, '') as description\n"
							+ "from\n"
							+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
							+ "             \"schema\".\"Table\"                        st\n"
							+ "  inner join \"schema\".\"Schema\"                        s on st.\"schema\"       =                s.id\n"
							+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
							+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  =  \"lastVersion\".version\n"
							+ "  left  join (\n"
							+ "    select\n"
							+ "      pn.nspname, pc.relname, pd.description\n"
							+ "    from\n"
							+ "                 pg_catalog.pg_namespace   pn\n"
							+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
							+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid=0\n"
							+ "  ) d on (s.\"name\", st.\"name\") = (d.nspname, d.relname)\n"
							+ "where\n"
							+ "  client_ap.version=?\n"
							+ "  and client_ap.created >= \"sinceVersion\".created\n"
							+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
							+ "order by\n"
							+ "  st.id"
						);
						try {
							pstmt.setString(1, source.getProtocolVersion().getVersion());

							ResultSet results=pstmt.executeQuery();
							try {
								int clientTableID=0;
								Table tempST=new Table();
								while(results.next()) {
									tempST.init(results);
									clientTables.add(
										new Table(
											clientTableID++,
											tempST.getName(),
											tempST.getSinceVersion_version(),
											tempST.getLastVersion_version(),
											tempST.getDisplay(),
											tempST.isPublic(),
											tempST.getDescription()
										)
									);
								}
							} finally {
								results.close();
							}
						} catch(SQLException err) {
							System.err.println("Error from query: "+pstmt.toString());
							throw err;
						} finally {
							pstmt.close();
						}
						MasterServer.writeObjects(
							source,
							out,
							provideProgress,
							clientTables
						);
					}
					break;
				case SCHEMA_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Type(),
						"select\n"
						+ "  st.id,\n"
						+ "  st.\"name\",\n"
						+ "  st.\"sinceVersion\",\n"
						+ "  st.\"lastVersion\"\n"
						+ "from\n"
						+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
						+ "             \"schema\".\"Type\"           st\n"
						+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
						+ "  left  join \"schema\".\"AoservProtocol\" \"lastVersion\"  on st.\"lastVersion\"  =  \"lastVersion\".version\n"
						+ "where\n"
						+ "  client_ap.version=?\n"
						+ "  and client_ap.created >= \"sinceVersion\".created\n"
						+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
						+ "order by\n"
						+ "  st.id",
						source.getProtocolVersion().getVersion()
					);
					break;
				case SERVER_FARMS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ServerFarm(),
							"select * from infrastructure.\"ServerFarm\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new ServerFarm(),
							"select distinct\n"
							+ "  sf.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms,\n"
							+ "            net.\"Host\"          se\n"
							+ "  left join backup.\"FileReplication\" ffr on  se.id             = ffr.server\n"
							+ "  left join backup.\"BackupPartition\" bp  on ffr.backup_partition =  bp.id\n"
							+ "  left join net.\"Host\"          fs  on  bp.ao_server        =  fs.id,\n"
							+ "  infrastructure.\"ServerFarm\" sf\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and ms.server=se.id\n"
							+ "  and (\n"
							+ "    se.farm=sf.name\n"
							+ "    or fs.farm=sf.name\n"
							+ "  )",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ServerFarm(),
						"select distinct\n"
						+ "  sf.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						+ "  net.\"Host\" se,\n"
						+ "  infrastructure.\"ServerFarm\" sf\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and (\n"
						+ "    (\n"
						+ "      pk.accounting=bs.accounting\n"
						+ "      and bs.server=se.id\n"
						+ "      and se.farm=sf.name\n"
						+ "    ) or pk.id=sf.owner\n"
						+ "  )",
						username
					);
					break;
				case SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Host(),
							"select * from net.\"Host\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Host(),
							"select distinct\n"
							+ "  se.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  left join linux.\"Server\" ao on ms.server=ao.server\n"
							// Allow its failover parent
							+ "  left join linux.\"Server\" ff on ao.failover_server=ff.server\n"
							// Allow its failover children
							+ "  left join linux.\"Server\" fs on ao.server=fs.failover_server\n"
							// Allow servers it replicates to
							+ "  left join backup.\"FileReplication\" ffr on ms.server=ffr.server\n"
							+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
							+ "  net.\"Host\" se\n"
							+ "where\n"
							+ "  ms.username=?\n"
							+ "  and (\n"
							// Allow direct access
							+ "    ms.server=se.id\n"
							// Allow its failover parent
							+ "    or ff.server=se.id\n"
							// Allow its failover children
							+ "    or fs.server=se.id\n"
							// Allow servers it replicates to
							+ "    or bp.ao_server=se.id\n"
							+ "  )",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Host(),
						"select distinct\n"
						+ "  se.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  net.\"Host\" se\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=se.id\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=se.id\n"
						+ "  )",
						username
					);
					break;
				case SHELLS :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Shell(),
						"select * from linux.\"Shell\""
					);
					break;
				case SIGNUP_REQUEST_OPTIONS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Option(),
								"select * from signup.\"Option\""
							);
						} else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Option(),
							"select\n"
							+ "  sro.*\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  signup.\"Request\" sr,\n"
							+ "  signup.\"Option\" sro\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk1.name\n"
							+ "  and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=sr.brand\n"
							+ "  and sr.id=sro.request",
							username
						);
					}
					break;
				case SIGNUP_REQUESTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Request(),
								"select\n"
								+ "  id\n"
								+ "  brand,\n"
								+ "  \"time\",\n"
								+ "  host(ip_address) as ip_address,\n"
								+ "  package_definition,\n"
								+ "  business_name,\n"
								+ "  business_phone,\n"
								+ "  business_fax,\n"
								+ "  business_address1,\n"
								+ "  business_address2,\n"
								+ "  business_city,\n"
								+ "  business_state,\n"
								+ "  business_country,\n"
								+ "  business_zip,\n"
								+ "  ba_name,\n"
								+ "  ba_title,\n"
								+ "  ba_work_phone,\n"
								+ "  ba_cell_phone,\n"
								+ "  ba_home_phone,\n"
								+ "  ba_fax,\n"
								+ "  ba_email,\n"
								+ "  ba_address1,\n"
								+ "  ba_address2,\n"
								+ "  ba_city,\n"
								+ "  ba_state,\n"
								+ "  ba_country,\n"
								+ "  ba_zip,\n"
								+ "  ba_username,\n"
								+ "  billing_contact,\n"
								+ "  billing_email,\n"
								+ "  billing_use_monthly,\n"
								+ "  billing_pay_one_year,\n"
								+ "  encrypted_data,\n"
								+ "  encryption_from,\n"
								+ "  encryption_recipient,\n"
								+ "  completed_by,\n"
								+ "  completed_time\n"
								+ "from\n"
								+ "  signup.\"Request\""
							);
						} else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Request(),
							"select\n"
							+ "  sr.id\n"
							+ "  sr.brand,\n"
							+ "  sr.\"time\",\n"
							+ "  host(sr.ip_address) as ip_address,\n"
							+ "  sr.package_definition,\n"
							+ "  sr.business_name,\n"
							+ "  sr.business_phone,\n"
							+ "  sr.business_fax,\n"
							+ "  sr.business_address1,\n"
							+ "  sr.business_address2,\n"
							+ "  sr.business_city,\n"
							+ "  sr.business_state,\n"
							+ "  sr.business_country,\n"
							+ "  sr.business_zip,\n"
							+ "  sr.ba_name,\n"
							+ "  sr.ba_title,\n"
							+ "  sr.ba_work_phone,\n"
							+ "  sr.ba_cell_phone,\n"
							+ "  sr.ba_home_phone,\n"
							+ "  sr.ba_fax,\n"
							+ "  sr.ba_email,\n"
							+ "  sr.ba_address1,\n"
							+ "  sr.ba_address2,\n"
							+ "  sr.ba_city,\n"
							+ "  sr.ba_state,\n"
							+ "  sr.ba_country,\n"
							+ "  sr.ba_zip,\n"
							+ "  sr.ba_username,\n"
							+ "  sr.billing_contact,\n"
							+ "  sr.billing_email,\n"
							+ "  sr.billing_use_monthly,\n"
							+ "  sr.billing_pay_one_year,\n"
							+ "  sr.encrypted_data,\n"
							+ "  sr.encryption_from,\n"
							+ "  sr.encryption_recipient,\n"
							+ "  sr.completed_by,\n"
							+ "  sr.completed_time\n"
							+ "from\n"
							+ "  account.\"Username\" un,\n"
							+ "  billing.\"Package\" pk1,\n"
							+ TableHandler.BU1_PARENTS_JOIN
							+ "  signup.\"Request\" sr\n"
							+ "where\n"
							+ "  un.username=?\n"
							+ "  and un.package=pk1.name\n"
							+ "  and (\n"
							+ TableHandler.PK1_BU1_PARENTS_WHERE
							+ "  )\n"
							+ "  and bu1.accounting=sr.brand",
							username
						);
					}
					break;
				case SSL_CERTIFICATE_NAMES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CertificateName(),
							"select * from pki.\"CertificateName\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CertificateName(),
							"select\n"
							+ "  scn.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join pki.\"Certificate\" sc on ms.server=sc.ao_server\n"
							+ "  inner join pki.\"CertificateName\" scn on sc.id=scn.ssl_certificate\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new CertificateName(),
						"select\n"
						+ "  scn.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  pki.\"Certificate\" sc,\n"
						+ "  pki.\"CertificateName\" scn\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.id=sc.package\n"
						+ "  and sc.id=scn.ssl_certificate",
						username
					);
					break;
				case SSL_CERTIFICATE_OTHER_USES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CertificateOtherUse(),
							"select * from pki.\"CertificateOtherUse\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new CertificateOtherUse(),
							"select\n"
							+ "  scou.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join pki.\"Certificate\" sc on ms.server=sc.ao_server\n"
							+ "  inner join pki.\"CertificateOtherUse\" scou on sc.id=scou.ssl_certificate\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new CertificateOtherUse(),
						"select\n"
						+ "  scou.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  pki.\"Certificate\" sc,\n"
						+ "  pki.\"CertificateOtherUse\" scou\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.id=sc.package\n"
						+ "  and sc.id=scou.ssl_certificate",
						username
					);
					break;
				case SSL_CERTIFICATES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Certificate(),
							"select * from pki.\"Certificate\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Certificate(),
							"select\n"
							+ "  sc.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join pki.\"Certificate\" sc on ms.server=sc.ao_server\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Certificate(),
						"select\n"
						+ "  sc.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk1,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  billing.\"Package\" pk2,\n"
						+ "  pki.\"Certificate\" sc\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk1.name\n"
						+ "  and (\n"
						+ TableHandler.PK1_BU1_PARENTS_WHERE
						+ "  )\n"
						+ "  and bu1.accounting=pk2.accounting\n"
						+ "  and pk2.id=sc.package",
						username
					);
					break;
				case TICKET_ACTION_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new ActionType(),
						"select * from ticket.\"ActionType\""
					);
					break;
				case TICKET_ACTIONS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Action(),
							"select\n"
							+ "  id,\n"
							+ "  ticket,\n"
							+ "  administrator,\n"
							+ "  time,\n"
							+ "  action_type,\n"
							+ "  old_accounting,\n"
							+ "  new_accounting,\n"
							+ "  old_priority,\n"
							+ "  new_priority,\n"
							+ "  old_type,\n"
							+ "  new_type,\n"
							+ "  old_status,\n"
							+ "  new_status,\n"
							+ "  old_assigned_to,\n"
							+ "  new_assigned_to,\n"
							+ "  old_category,\n"
							+ "  new_category,\n"
							+ "  from_address,\n"
							+ "  summary\n"
							+ "from\n"
							+ "  ticket.\"Action\""
						); else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						if(TicketHandler.isTicketAdmin(conn, source)) {
							// If a ticket admin, can see all ticket.Action
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Action(),
								"select\n"
								+ "  ta.id,\n"
								+ "  ta.ticket,\n"
								+ "  ta.administrator,\n"
								+ "  ta.time,\n"
								+ "  ta.action_type,\n"
								+ "  ta.old_accounting,\n"
								+ "  ta.new_accounting,\n"
								+ "  ta.old_priority,\n"
								+ "  ta.new_priority,\n"
								+ "  ta.old_type,\n"
								+ "  ta.new_type,\n"
								+ "  ta.old_status,\n"
								+ "  ta.new_status,\n"
								+ "  ta.old_assigned_to,\n"
								+ "  ta.new_assigned_to,\n"
								+ "  ta.old_category,\n"
								+ "  ta.new_category,\n"
								+ "  ta.from_address,\n"
								+ "  ta.summary\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk1,\n"
								+ TableHandler.BU1_PARENTS_JOIN
								+ "  ticket.\"Ticket\" ti,\n"
								+ "  ticket.\"Action\" ta\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk1.name\n"
								+ "  and (\n"
								+ TableHandler.PK1_BU1_PARENTS_WHERE
								+ "  )\n"
								+ "  and (\n"
								+ "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
								+ "    or bu1.accounting=ti.brand\n" // Has access to brand
								+ "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
								+ "  )\n"
								+ "  and ti.id=ta.ticket",
								username
							);
						} else {
							// Can only see non-admin types and statuses
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Action(),
								"select\n"
								+ "  ta.id,\n"
								+ "  ta.ticket,\n"
								+ "  ta.administrator,\n"
								+ "  ta.time,\n"
								+ "  ta.action_type,\n"
								+ "  ta.old_accounting,\n"
								+ "  ta.new_accounting,\n"
								+ "  ta.old_priority,\n"
								+ "  ta.new_priority,\n"
								+ "  ta.old_type,\n"
								+ "  ta.new_type,\n"
								+ "  ta.old_status,\n"
								+ "  ta.new_status,\n"
								+ "  ta.old_assigned_to,\n"
								+ "  ta.new_assigned_to,\n"
								+ "  ta.old_category,\n"
								+ "  ta.new_category,\n"
								+ "  ta.from_address,\n"
								+ "  ta.summary\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk1,\n"
								+ TableHandler.BU1_PARENTS_JOIN
								+ "  ticket.\"Ticket\" ti,\n"
								+ "  ticket.\"Action\" ta,\n"
								+ "  ticket.\"ActionType\" tat\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk1.name\n"
								+ "  and (\n"
								+ TableHandler.PK1_BU1_PARENTS_WHERE
								+ "  )\n"
								+ "  and bu1.accounting=ti.accounting\n"
								+ "  and ti.status not in ('junk', 'deleted')\n"
								+ "  and ti.id=ta.ticket\n"
								+ "  and ta.action_type=tat.type\n"
								+ "  and not tat.visible_admin_only",
								username
							);
						}
					}
					break;
				case TICKET_ASSIGNMENTS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new Assignment(),
							"select * from ticket.\"Assignment\""
						); else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else {
						if(TicketHandler.isTicketAdmin(conn, source)) {
							// Only ticket admin can see assignments
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Action(),
								"select\n"
								+ "  ta.*\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk1,\n"
								+ TableHandler.BU1_PARENTS_JOIN
								+ "  ticket.\"Ticket\" ti,\n"
								+ "  ticket.\"Assignment\" ta\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk1.name\n"
								+ "  and (\n"
								+ TableHandler.PK1_BU1_PARENTS_WHERE
								+ "  )\n"
								+ "  and (\n"
								+ "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
								+ "    or bu1.accounting=ti.brand\n" // Has access to brand
								+ "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
								+ "  )\n"
								+ "  and ti.id=ta.ticket",
								username
							);
						} else {
							// Non-admins don't get any assignment details
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					}
					break;
				case TICKET_BRAND_CATEGORIES :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new BrandCategory(),
							"select * from reseller.\"BrandCategory\""
						); else {
							MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
						}
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new BrandCategory(),
						"select\n"
						+ "  tbc.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ TableHandler.BU1_PARENTS_JOIN
						+ "  reseller.\"BrandCategory\" tbc\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and (\n"
						+ TableHandler.PK_BU1_PARENTS_WHERE
						+ "  ) and bu1.accounting=tbc.brand",
						username
					);
					break;
				case TICKET_CATEGORIES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Category(),
						"select * from reseller.\"Category\""
					);
					break;
				case TICKET_PRIORITIES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Priority(),
						"select * from ticket.\"Priority\""
					);
					break;
				case TICKET_STATI :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new Status(),
						"select * from ticket.\"Status\""
					);
					break;
				case TICKET_TYPES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new TicketType(),
						"select * from ticket.\"TicketType\""
					);
					break;
				case TICKETS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Ticket(),
								"select\n"
								+ "  id,\n"
								+ "  brand,\n"
								+ "  reseller,\n"
								+ "  accounting,\n"
								+ "  language,\n"
								+ "  created_by,\n"
								+ "  category,\n"
								+ "  ticket_type,\n"
								+ "  from_address,\n"
								+ "  summary,\n"
								+ "  open_date,\n"
								+ "  client_priority,\n"
								+ "  admin_priority,\n"
								+ "  status,\n"
								+ "  status_timeout,\n"
								+ "  contact_emails,\n"
								+ "  contact_phone_numbers\n"
								+ "from\n"
								+ "  ticket.\"Ticket\""
							);
						} else {
							// AOServDaemon only needs access to its own open logs ticket.Ticket
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Ticket(),
								"select\n"
								+ "  ti.id,\n"
								+ "  ti.brand,\n"
								+ "  ti.reseller,\n"
								+ "  ti.accounting,\n"
								+ "  ti.language,\n"
								+ "  ti.created_by,\n"
								+ "  ti.category,\n"
								+ "  ti.ticket_type,\n"
								+ "  ti.from_address,\n"
								+ "  ti.summary,\n"
								+ "  ti.open_date,\n"
								+ "  ti.client_priority,\n"
								+ "  ti.admin_priority,\n"
								+ "  ti.status,\n"
								+ "  ti.status_timeout,\n"
								+ "  ti.contact_emails,\n"
								+ "  ti.contact_phone_numbers\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk,\n"
								+ "  ticket.\"Ticket\" ti\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk.name\n"
								+ "  and pk.accounting=ti.brand\n"
								+ "  and pk.accounting=ti.accounting\n"
								+ "  and ti.status in (?,?,?)\n"
								+ "  and ti.ticket_type=?",
								username,
								Status.OPEN,
								Status.HOLD,
								Status.BOUNCED,
								TicketType.LOGS
							);
						}
					} else {
						if(TicketHandler.isTicketAdmin(conn, source)) {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Ticket(),
								"select distinct\n"
								+ "  ti.id,\n"
								+ "  ti.brand,\n"
								+ "  ti.reseller,\n"
								+ "  ti.accounting,\n"
								+ "  ti.language,\n"
								+ "  ti.created_by,\n"
								+ "  ti.category,\n"
								+ "  ti.ticket_type,\n"
								+ "  ti.from_address,\n"
								+ "  ti.summary,\n"
								+ "  ti.open_date,\n"
								+ "  ti.client_priority,\n"
								+ "  ti.admin_priority,\n"
								+ "  ti.status,\n"
								+ "  ti.status_timeout,\n"
								+ "  ti.contact_emails,\n"
								+ "  ti.contact_phone_numbers\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk1,\n"
								+ TableHandler.BU1_PARENTS_JOIN
								+ "  ticket.\"Ticket\" ti\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk1.name\n"
								+ "  and (\n"
								+ TableHandler.PK1_BU1_PARENTS_WHERE
								+ "  )\n"
								+ "  and (\n"
								+ "    bu1.accounting=ti.accounting\n" // Has access to ticket accounting
								+ "    or bu1.accounting=ti.brand\n" // Has access to brand
								+ "    or bu1.accounting=ti.reseller\n" // Has access to assigned reseller
								+ "  )",
								username
							);
						} else {
							MasterServer.writeObjects(
								conn,
								source,
								out,
								provideProgress,
								new Ticket(),
								"select\n"
								+ "  ti.id,\n"
								+ "  ti.brand,\n"
								+ "  null::text,\n" // reseller
								+ "  ti.accounting,\n"
								+ "  ti.language,\n"
								+ "  ti.created_by,\n"
								+ "  ti.category,\n"
								+ "  ti.ticket_type,\n"
								+ "  ti.from_address,\n"
								+ "  ti.summary,\n"
								+ "  ti.open_date,\n"
								+ "  ti.client_priority,\n"
								+ "  null,\n" // admin_priority
								+ "  ti.status,\n"
								+ "  ti.status_timeout,\n"
								+ "  ti.contact_emails,\n"
								+ "  ti.contact_phone_numbers\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  billing.\"Package\" pk1,\n"
								+ TableHandler.BU1_PARENTS_JOIN
								+ "  ticket.\"Ticket\" ti\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk1.name\n"
								+ "  and (\n"
								+ TableHandler.PK1_BU1_PARENTS_WHERE
								+ "  )\n"
								+ "  and bu1.accounting=ti.accounting\n"
								+ "  and ti.status not in ('junk', 'deleted')",
								username
							);
						}
					}
					break;
				case TIME_ZONES :
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new TimeZone(),
						"select * from linux.\"TimeZone\""
					);
					break;
				case VIRTUAL_DISKS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualDisk(),
							"select * from infrastructure.\"VirtualDisk\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualDisk(),
							"select distinct\n"
							+ "  vd.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join infrastructure.\"VirtualDisk\" vd on ms.server=vd.virtual_server\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new VirtualDisk(),
						"select distinct\n"
						+ "  vd.*\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  infrastructure.\"VirtualDisk\" vd\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=vd.virtual_server\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=vd.virtual_server\n"
						+ "  )",
						username
					);
					break;
				case VIRTUAL_SERVERS :
					if(masterUser != null) {
						assert masterServers != null;
						if(masterServers.length == 0) MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualServer(),
							"select * from infrastructure.\"VirtualServer\""
						); else MasterServer.writeObjects(
							conn,
							source,
							out,
							provideProgress,
							new VirtualServer(),
							"select distinct\n"
							+ "  vs.*\n"
							+ "from\n"
							+ "  master.\"UserHost\" ms\n"
							+ "  inner join infrastructure.\"VirtualServer\" vs on ms.server=vs.server\n"
							+ "where\n"
							+ "  ms.username=?",
							username
						);
					} else MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						new VirtualServer(),
						"select distinct\n"
						+ "  vs.server,\n"
						+ "  vs.primary_ram,\n"
						+ "  vs.primary_ram_target,\n"
						+ "  vs.secondary_ram,\n"
						+ "  vs.secondary_ram_target,\n"
						+ "  vs.minimum_processor_type,\n"
						+ "  vs.minimum_processor_architecture,\n"
						+ "  vs.minimum_processor_speed,\n"
						+ "  vs.minimum_processor_speed_target,\n"
						+ "  vs.processor_cores,\n"
						+ "  vs.processor_cores_target,\n"
						+ "  vs.processor_weight,\n"
						+ "  vs.processor_weight_target,\n"
						+ "  vs.primary_physical_server_locked,\n"
						+ "  vs.secondary_physical_server_locked,\n"
						+ "  vs.requires_hvm,\n"
						+ "  case\n"
						+ "    when vs.vnc_password is null then null\n"
						// Only provide the password when the user can connect to VNC console
						+ "    when (\n"
						+ "      select bs2.id from account.\"AccountHost\" bs2 where bs2.accounting=pk.accounting and bs2.server=vs.server and bs2.can_vnc_console limit 1\n"
						+ "    ) is not null then vs.vnc_password\n"
						+ "    else '"+AoservProtocol.FILTERED+"'::text\n"
						+ "  end\n"
						+ "from\n"
						+ "  account.\"Username\" un,\n"
						+ "  billing.\"Package\" pk,\n"
						+ "  account.\"AccountHost\" bs,\n"
						// Allow servers it replicates to
						//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
						//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
						+ "  infrastructure.\"VirtualServer\" vs\n"
						+ "where\n"
						+ "  un.username=?\n"
						+ "  and un.package=pk.name\n"
						+ "  and pk.accounting=bs.accounting\n"
						+ "  and (\n"
						+ "    bs.server=vs.server\n"
						// Allow servers it replicates to
						//+ "    or bp.ao_server=vs.server\n"
						+ "  )",
						username
					);
					break;
				default :
					throw new IOException("Unknown table ID: "+tableID);
			}
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