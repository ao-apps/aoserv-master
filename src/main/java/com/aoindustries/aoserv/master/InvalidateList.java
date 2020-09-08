/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
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
		DatabaseAccess db,
		Table.TableID tableID,
		Account.Name account,
		int host,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			db,
			tableID,
			getAccountCollection(account),
			getHostCollection(host),
			recurse
		);
	}

	public void addTable(
		DatabaseAccess db,
		Table.TableID tableID,
		Collection<Account.Name> accounts,
		int host,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			db,
			tableID,
			accounts,
			getHostCollection(host),
			recurse
		);
	}

	public void addTable(
		DatabaseAccess db,
		Table.TableID tableID,
		Account.Name account,
		IntCollection hosts,
		boolean recurse
	) throws IOException, SQLException {
		addTable(
			db,
			tableID,
			getAccountCollection(account),
			hosts,
			recurse
		);
	}

	public void addTable(
		DatabaseAccess db,
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
					addTable(db, Table.TableID.FIREWALLD_ZONES,       accounts, hosts, true);
					addTable(db, Table.TableID.LINUX_SERVER_ACCOUNTS, accounts, hosts, true);
					addTable(db, Table.TableID.LINUX_SERVER_GROUPS,   accounts, hosts, true);
					addTable(db, Table.TableID.MYSQL_SERVERS,         accounts, hosts, true);
					addTable(db, Table.TableID.POSTGRES_SERVERS,      accounts, hosts, true);
					break;
				case BUSINESS_SERVERS :
					addTable(db, Table.TableID.SERVERS, accounts, hosts, true);
					break;
				case BUSINESSES :
					addTable(db, Table.TableID.BUSINESS_PROFILES, accounts, hosts, true);
					break;
				case CYRUS_IMAPD_BINDS :
					addTable(db, Table.TableID.CYRUS_IMAPD_SERVERS, accounts, hosts, false);
					break;
				case CYRUS_IMAPD_SERVERS :
					addTable(db, Table.TableID.CYRUS_IMAPD_BINDS, accounts, hosts, false);
					break;
				case EMAIL_DOMAINS :
					addTable(db, Table.TableID.EMAIL_ADDRESSES,   accounts, hosts, true);
					addTable(db, Table.TableID.MAJORDOMO_SERVERS, accounts, hosts, true);
					break;
				case FAILOVER_FILE_REPLICATIONS :
					addTable(db, Table.TableID.SERVERS,      accounts, hosts, true);
					addTable(db, Table.TableID.NET_DEVICES,  accounts, hosts, true);
					addTable(db, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					addTable(db, Table.TableID.NET_BINDS,    accounts, hosts, true);
					break;
				case IP_REPUTATION_LIMITER_SETS:
					// Sets are only visible when used by at least one limiter in the same server farm
					addTable(db, Table.TableID.IP_REPUTATION_SETS,         accounts, hosts, true);
					addTable(db, Table.TableID.IP_REPUTATION_SET_HOSTS,    accounts, hosts, true);
					addTable(db, Table.TableID.IP_REPUTATION_SET_NETWORKS, accounts, hosts, true);
					break;
				case HTTPD_BINDS :
					addTable(db, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					addTable(db, Table.TableID.NET_BINDS,    accounts, hosts, false);
					break;
				case HTTPD_SITE_BINDS :
					addTable(db, Table.TableID.HTTPD_BINDS,             accounts, hosts, true);
					addTable(db, Table.TableID.HTTPD_SITE_BIND_HEADERS, accounts, hosts, false);
					addTable(db, Table.TableID.RewriteRule,             accounts, hosts, false);
					break;
				case HTTPD_TOMCAT_SITES :
					addTable(db, Table.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounts, hosts, false);
					break;
				case IP_ADDRESSES :
					addTable(db, Table.TableID.IpAddressMonitoring, accounts, hosts, false);
					break;
				case LINUX_ACCOUNTS :
					addTable(db, Table.TableID.FTP_GUEST_USERS, accounts, hosts, true);
					addTable(db, Table.TableID.USERNAMES,       accounts, hosts, true);
					break;
				case LINUX_SERVER_ACCOUNTS :
					addTable(db, Table.TableID.LINUX_ACCOUNTS,       accounts, hosts, true);
					addTable(db, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
					break;
				case LINUX_SERVER_GROUPS :
					addTable(db, Table.TableID.EMAIL_LISTS,          accounts, hosts, true);
					addTable(db, Table.TableID.LINUX_GROUPS,         accounts, hosts, true);
					addTable(db, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, hosts, true);
					break;
				case MAJORDOMO_SERVERS :
					addTable(db, Table.TableID.MAJORDOMO_LISTS, accounts, hosts, true);
					break;
				case MYSQL_SERVER_USERS :
					addTable(db, Table.TableID.MYSQL_USERS, accounts, hosts, true);
					break;
				case MYSQL_SERVERS :
					addTable(db, Table.TableID.NET_BINDS,          accounts, hosts, true);
					addTable(db, Table.TableID.MYSQL_DATABASES,    accounts, hosts, true);
					addTable(db, Table.TableID.MYSQL_SERVER_USERS, accounts, hosts, true);
					break;
				case NET_BINDS :
					addTable(db, Table.TableID.HTTPD_BINDS,              accounts, hosts, false);
					addTable(db, Table.TableID.NET_BIND_FIREWALLD_ZONES, accounts, hosts, false);
					break;
				case NET_BIND_FIREWALLD_ZONES :
					// Presence of "public" firewalld zone determines compatibility "open_firewall" for clients
					// version <= 1.80.2
					addTable(db, Table.TableID.NET_BINDS, accounts, hosts, false);
					break;
				case NET_DEVICES :
					addTable(db, Table.TableID.IP_ADDRESSES, accounts, hosts, true);
					break;
				case NOTICE_LOG :
					addTable(db, Table.TableID.NoticeLogBalance, accounts, hosts, false);
					break;
				case NoticeLogBalance :
					// Added for compatibility "balance" for pre-1.83.0 clients
					addTable(db, Table.TableID.NOTICE_LOG, accounts, hosts, false);
					break;
				case PACKAGE_DEFINITIONS :
					addTable(db, Table.TableID.PACKAGE_DEFINITION_LIMITS, accounts, hosts, true);
					break;
				case PACKAGES :
					addTable(db, Table.TableID.PACKAGE_DEFINITIONS, accounts, hosts, true);
					break;
				case POSTGRES_SERVER_USERS :
					addTable(db, Table.TableID.POSTGRES_USERS, accounts, hosts, true);
					break;
				case POSTGRES_SERVERS :
					addTable(db, Table.TableID.NET_BINDS,             accounts, hosts, true);
					addTable(db, Table.TableID.POSTGRES_DATABASES,    accounts, hosts, true);
					addTable(db, Table.TableID.POSTGRES_SERVER_USERS, accounts, hosts, true);
					break;
				case SENDMAIL_BINDS :
					addTable(db, Table.TableID.SENDMAIL_SERVERS, accounts, hosts, false);
					break;
				case SENDMAIL_SERVERS :
					addTable(db, Table.TableID.SENDMAIL_BINDS, accounts, hosts, false);
					break;
				case SERVERS :
					addTable(db, Table.TableID.AO_SERVERS,      accounts, hosts, true);
					addTable(db, Table.TableID.IP_ADDRESSES,    accounts, hosts, true);
					addTable(db, Table.TableID.NET_DEVICES,     accounts, hosts, true);
					addTable(db, Table.TableID.VIRTUAL_SERVERS, accounts, hosts, true);
					break;
				case SSL_CERTIFICATES :
					addTable(db, Table.TableID.SSL_CERTIFICATE_NAMES,      accounts, hosts, false);
					addTable(db, Table.TableID.SSL_CERTIFICATE_OTHER_USES, accounts, hosts, false);
					break;
				case USERNAMES :
					addTable(db, Table.TableID.BUSINESS_ADMINISTRATORS, accounts, hosts, true);
					break;
				case VIRTUAL_SERVERS :
					addTable(db, Table.TableID.VIRTUAL_DISKS, accounts, hosts, true);
					break;
				case WhoisHistoryAccount :
					addTable(db, Table.TableID.WhoisHistory, accounts, hosts, false);
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
