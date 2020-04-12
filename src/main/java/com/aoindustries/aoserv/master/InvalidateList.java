/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.collections.IntArrayList;
import com.aoindustries.collections.IntCollection;
import com.aoindustries.collections.IntList;
import com.aoindustries.collections.SortedArrayList;
import com.aoindustries.dbc.DatabaseAccess;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In the request lifecycle, table invalidations occur after the database connection has been committed
 * and released.  This ensures that all data is available for the processes that react to the table
 * updates.  For efficiency, each host and account will only be notified once per table per
 * request.
 *
 * @author  AO Industries, Inc.
 */
// TODO: This should use HashSet instead of SortedArrayList
final public class InvalidateList {

	/**
	 * The invalidate list is used as part of the error logging, so it is not
	 * logged to the ticket system.
	 */
	private static final Logger logger = Logger.getLogger(InvalidateList.class.getName());

	/** Copy once to avoid repeated copies. */
	final private static Table.TableID[] tableIDs = Table.TableID.values();
	// TODO: Unused 2018-11-18: final private static int numTables = tableIDs.length;

	// TODO: Unused 2018-11-18: final private static String[] tableNames=new String[numTables];

	/**
	 * Indicates that all hosts or account.Account should receive the invalidate signal.
	 */
	public static final List<Account.Name> allAccounts = Collections.unmodifiableList(new ArrayList<>());
	public static final IntList allHosts = new IntArrayList();

	private final Map<Table.TableID,List<Integer>> hostLists = new EnumMap<>(Table.TableID.class);
	private final Map<Table.TableID,List<Account.Name>> accountLists = new EnumMap<>(Table.TableID.class);

	/**
	 * Resets back to default state.
	*/
	public void reset() {
		hostLists.clear();
		accountLists.clear();
	}

	public void addTable(
		DatabaseAccess conn,
		Table.TableID tableID,
		Account.Name account,
		int host,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			conn,
			tableID,
			getAccountCollection(account),
			getHostCollection(host),
			recurse
		);
	}

	public void addTable(
		DatabaseAccess conn,
		Table.TableID tableID,
		Collection<Account.Name> accounts,
		int host,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			conn,
			tableID,
			accounts,
			getHostCollection(host),
			recurse
		);
	}

	public void addTable(
		DatabaseAccess conn,
		Table.TableID tableID,
		Account.Name account,
		IntCollection hosts,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			conn,
			tableID,
			getAccountCollection(account),
			hosts,
			recurse
		);
	}

	public void addTable(
		DatabaseAccess conn,
		Table.TableID tableID,
		Collection<Account.Name> accounts,
		IntCollection hosts,
		boolean recurse
	) throws IOException, SQLException {
		// TODO: Unused 2018-11-18: if(tableNames[tableID.ordinal()]==null) tableNames[tableID.ordinal()]=TableHandler.getTableName(conn, tableID);

		// Add to the account lists
		if(accounts==null || accounts==allAccounts) {
			accountLists.put(tableID, allAccounts);
		} else {
			if(!accounts.isEmpty()) {
				List<Account.Name> SV = accountLists.get(tableID);
				// TODO: Just use HashSet here
				if(SV == null) accountLists.put(tableID, SV = new SortedArrayList<>());
				for(Account.Name account : accounts) {
					if(account == null) logger.log(Level.WARNING, null, new RuntimeException("Warning: account is null"));
					else if(!SV.contains(account)) SV.add(account);
				}
			}
		}

		// Add to the host lists
		if(hosts==null || hosts==allHosts) {
			hostLists.put(tableID, allHosts);
		} else if(!hosts.isEmpty()) {
			List<Integer> SV = hostLists.get(tableID);
			// TODO: Just use HashSet here
			if(SV == null) hostLists.put(tableID, SV = new SortedArrayList<>());
			for(Integer id : hosts) {
				if(id == null) logger.log(Level.WARNING, null, new RuntimeException("Warning: id is null"));
				else if(!SV.contains(id)) SV.add(id);
			}
		}

		// Recursively invalidate those tables who's filters might have been effected
		if(recurse) {
			switch(tableID) {
				case AO_SERVERS :
					addTable(conn, Table.TableID.FIREWALLD_ZONES,       accounts, hosts, true);
					addTable(conn, Table.TableID.LINUX_SERVER_ACCOUNTS, accounts, hosts, true);
					addTable(conn, Table.TableID.LINUX_SERVER_GROUPS,   accounts, hosts, true);
					addTable(conn, Table.TableID.MYSQL_SERVERS,         accounts, hosts, true);
					addTable(conn, Table.TableID.POSTGRES_SERVERS,      accounts, hosts, true);
					break;
				case BUSINESS_SERVERS :
					addTable(conn, Table.TableID.SERVERS, accounts, hosts, true);
					break;
				case BUSINESSES :
					addTable(conn, Table.TableID.BUSINESS_PROFILES, accounts, hosts, true);
					break;
				case CYRUS_IMAPD_BINDS :
					addTable(conn, Table.TableID.CYRUS_IMAPD_SERVERS, accounts, hosts, false);
					break;
				case CYRUS_IMAPD_SERVERS :
					addTable(conn, Table.TableID.CYRUS_IMAPD_BINDS, accounts, hosts, false);
					break;
				case EMAIL_DOMAINS :
					addTable(conn, Table.TableID.EMAIL_ADDRESSES,   accounts, hosts, true);
					addTable(conn, Table.TableID.MAJORDOMO_SERVERS, accounts, hosts, true);
					break;
				case FAILOVER_FILE_REPLICATIONS :
					addTable(conn, Table.TableID.SERVERS,      accounts, hosts, true);
					addTable(conn, Table.TableID.NET_DEVICES,  accounts, hosts, true);
					addTable(conn, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					addTable(conn, Table.TableID.NET_BINDS,    accounts, hosts, true);
					break;
				case IP_REPUTATION_LIMITER_SETS:
					// Sets are only visible when used by at least one limiter in the same server farm
					addTable(conn, Table.TableID.IP_REPUTATION_SETS,         accounts, hosts, true);
					addTable(conn, Table.TableID.IP_REPUTATION_SET_HOSTS,    accounts, hosts, true);
					addTable(conn, Table.TableID.IP_REPUTATION_SET_NETWORKS, accounts, hosts, true);
					break;
				case HTTPD_BINDS :
					addTable(conn, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					addTable(conn, Table.TableID.NET_BINDS,    accounts, hosts, false);
					break;
				case HTTPD_SITE_BINDS :
					addTable(conn, Table.TableID.HTTPD_BINDS,             accounts, hosts, true);
					addTable(conn, Table.TableID.HTTPD_SITE_BIND_HEADERS, accounts, hosts, false);
					addTable(conn, Table.TableID.RewriteRule,             accounts, hosts, false);
					break;
				case HTTPD_TOMCAT_SITES :
					addTable(conn, Table.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounts, hosts, false);
					break;
				case IP_ADDRESSES :
					addTable(conn, Table.TableID.IpAddressMonitoring, accounts, hosts, false);
					break;
				case LINUX_ACCOUNTS :
					addTable(conn, Table.TableID.FTP_GUEST_USERS, accounts, hosts, true);
					addTable(conn, Table.TableID.USERNAMES,       accounts, hosts, true);
					break;
				case LINUX_SERVER_ACCOUNTS :
					addTable(conn, Table.TableID.LINUX_ACCOUNTS,       accounts, hosts, true);
					addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
					break;
				case LINUX_SERVER_GROUPS :
					addTable(conn, Table.TableID.EMAIL_LISTS,          accounts, hosts, true);
					addTable(conn, Table.TableID.LINUX_GROUPS,         accounts, hosts, true);
					addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
					break;
				case MAJORDOMO_SERVERS :
					addTable(conn, Table.TableID.MAJORDOMO_LISTS, accounts, hosts, true);
					break;
				case MYSQL_SERVER_USERS :
					addTable(conn, Table.TableID.MYSQL_USERS, accounts, hosts, true);
					break;
				case MYSQL_SERVERS :
					addTable(conn, Table.TableID.NET_BINDS,          accounts, hosts, true);
					addTable(conn, Table.TableID.MYSQL_DATABASES,    accounts, hosts, true);
					addTable(conn, Table.TableID.MYSQL_SERVER_USERS, accounts, hosts, true);
					break;
				case NET_BINDS :
					addTable(conn, Table.TableID.HTTPD_BINDS,              accounts, hosts, false);
					addTable(conn, Table.TableID.NET_BIND_FIREWALLD_ZONES, accounts, hosts, false);
					break;
				case NET_BIND_FIREWALLD_ZONES :
					// Presence of "public" firewalld zone determines compatibility "open_firewall" for clients
					// version <= 1.80.2
					addTable(conn, Table.TableID.NET_BINDS, accounts, hosts, false);
					break;
				case NET_DEVICES :
					addTable(conn, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					break;
				case NOTICE_LOG :
					addTable(conn, Table.TableID.NoticeLogBalance, accounts, hosts, false);
					break;
				case NoticeLogBalance :
					// Added for compatibility "balance" for pre-1.83.0 clients
					addTable(conn, Table.TableID.NOTICE_LOG, accounts, hosts, false);
					break;
				case PACKAGE_DEFINITIONS :
					addTable(conn, Table.TableID.PACKAGE_DEFINITION_LIMITS, accounts, hosts, true);
					break;
				case PACKAGES :
					addTable(conn, Table.TableID.PACKAGE_DEFINITIONS, accounts, hosts, true);
					break;
				case POSTGRES_SERVER_USERS :
					addTable(conn, Table.TableID.POSTGRES_USERS, accounts, hosts, true);
					break;
				case POSTGRES_SERVERS :
					addTable(conn, Table.TableID.NET_BINDS,             accounts, hosts, true);
					addTable(conn, Table.TableID.POSTGRES_DATABASES,    accounts, hosts, true);
					addTable(conn, Table.TableID.POSTGRES_SERVER_USERS, accounts, hosts, true);
					break;
				case SENDMAIL_BINDS :
					addTable(conn, Table.TableID.SENDMAIL_SERVERS, accounts, hosts, false);
					break;
				case SENDMAIL_SERVERS :
					addTable(conn, Table.TableID.SENDMAIL_BINDS, accounts, hosts, false);
					break;
				case SERVERS :
					addTable(conn, Table.TableID.AO_SERVERS,      accounts, hosts, true);
					addTable(conn, Table.TableID.IP_ADDRESSES,    accounts, hosts, true);
					addTable(conn, Table.TableID.NET_DEVICES,     accounts, hosts, true);
					addTable(conn, Table.TableID.VIRTUAL_SERVERS, accounts, hosts, true);
					break;
				case SSL_CERTIFICATES :
					addTable(conn, Table.TableID.SSL_CERTIFICATE_NAMES,      accounts, hosts, false);
					addTable(conn, Table.TableID.SSL_CERTIFICATE_OTHER_USES, accounts, hosts, false);
					break;
				case USERNAMES :
					addTable(conn, Table.TableID.BUSINESS_ADMINISTRATORS, accounts, hosts, true);
					break;
				case VIRTUAL_SERVERS :
					addTable(conn, Table.TableID.VIRTUAL_DISKS, accounts, hosts, true);
					break;
				case WhoisHistoryAccount :
					addTable(conn, Table.TableID.WhoisHistory, accounts, hosts, false);
					break;
			}
		}
	}

	public List<Account.Name> getAffectedAccounts(Table.TableID tableID) {
		List<Account.Name> SV=accountLists.get(tableID);
		if(SV != null || hostLists.containsKey(tableID)) {
			return (SV == null) ? allAccounts : SV;
		} else {
			return null;
		}
	}

	public List<Integer> getAffectedHosts(Table.TableID tableID) {
		List<Integer> SV=hostLists.get(tableID);
		if(SV != null || accountLists.containsKey(tableID)) {
			return (SV == null) ? allHosts : SV;
		} else {
			return null;
		}
	}

	public void invalidateMasterCaches() {
		for(Table.TableID tableID : tableIDs) {
			if(hostLists.containsKey(tableID) || accountLists.containsKey(tableID)) {
				AccountHandler.invalidateTable(tableID);
				CvsHandler.invalidateTable(tableID);
				DaemonHandler.invalidateTable(tableID);
				// TODO: Have each service register to receive invalidation signals
				try {
					MasterServer.getService(DnsService.class).invalidateTable(tableID);
				} catch(NoServiceException e) {
					// OK when running batch credit card processing from command line
				}
				EmailHandler.invalidateTable(tableID);
				WebHandler.invalidateTable(tableID);
				LinuxAccountHandler.invalidateTable(tableID);
				MasterServer.invalidateTable(tableID);
				MysqlHandler.invalidateTable(tableID);
				PackageHandler.invalidateTable(tableID);
				PostgresqlHandler.invalidateTable(tableID);
				NetHostHandler.invalidateTable(tableID);
				TableHandler.invalidateTable(tableID);
				AccountUserHandler.invalidateTable(tableID);
			}
		}
	}

	public boolean isInvalid(Table.TableID tableID) {
		return hostLists.containsKey(tableID) || accountLists.containsKey(tableID);
	}

	public static Collection<Account.Name> getAccountCollection(Account.Name ... accounts) {
		if(accounts.length == 0) return Collections.emptyList();
		Collection<Account.Name> coll = new ArrayList<>(accounts.length);
		Collections.addAll(coll, accounts);
		return coll;
	}

	public static IntCollection getHostCollection(int ... hosts) throws IOException, SQLException {
		if(hosts.length == 0) return new IntArrayList(0);
		IntCollection coll = new IntArrayList(hosts.length);
		for(int host : hosts) coll.add(host);
		return coll;
	}
}
