/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.collections.IntList;
import com.aoapps.collections.LongArrayList;
import com.aoapps.collections.LongList;
import com.aoapps.collections.SortedIntArrayList;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.net.DomainName;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.distribution.OperatingSystemVersion;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>ServerHandler</code> handles all the accesses to the Host tables.
 *
 * @author  AO Industries, Inc.
 */
public final class NetHostHandler {

	private static final Logger logger = Logger.getLogger(NetHostHandler.class.getName());

	private NetHostHandler() {
	}

	private static Map<com.aoindustries.aoserv.client.account.User.Name, List<Integer>> userHosts;

	/*
	public static int addBackupServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String hostname,
		String farm,
		int owner,
		String description,
		int os_version,
		String username,
		String password,
		String contact_phone,
		String contact_email
	) throws IOException, SQLException {
		// Security and validity checks
		String account=UsernameHandler.getAccountForUsername(conn, source.getUsername());
		if(
			!conn.queryBoolean("select can_add_backup_server from account.\"Account\" where accounting=?", account)
		) throw new SQLException("Not allowed to add_backup_server: "+source.getUsername());

		MasterServer.checkAccessHostname(conn, source, "addBackupServer", hostname);

		String farm_owner=conn.queryString(
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  infrastructure."ServerFarm" sf,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  sf.name=?\n"
			+ "  and sf.owner=pk.id",
			farm
		);
		if(!AccountHandler.isAccountOrParent(conn, account, farm_owner)) throw new SQLException("Not able to access farm: "+farm);

		PackageHandler.checkAccessPackage(conn, source, "addBackupServer", owner);

		String check = Username.checkUsername(username, Locale.getDefault());
		if(check!=null) throw new SQLException(check);

		PasswordChecker.Result[] results = Administrator.checkPassword(Locale.getDefault(), username, password);
		if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Password strength check failed: "+PasswordChecker.getResultsString(results).replace('\n', '|'));

		int host = conn.updateInt(
			"INSERT INTO\n"
			+ "  net."Host"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  'orion',\n"
			+ "  ?,\n"
			+ "  null,\n"
			+ "  ?,\n"
			+ "  null\n"
			+ ") RETURNING id",
			hostname,
			farm,
			PackageHandler.getAccountForPackage(conn, owner),
			description,
			os_version
		);
		invalidateList.addTable(conn, Table.TableID.SERVERS, account, InvalidateList.allServers, false);

		// Build a stack of parents, adding each business_server
		Stack<String> bus=new Stack<String>();
		String packageAccount=PackageHandler.getAccountForPackage(conn, owner);
		String currentAccount=packageAccount;
		while(true) {
			bus.push(currentAccount);
			if(currentAccount.equals(AccountHandler.getRootAccount())) break;
			currentAccount=AccountHandler.getParentAccount(conn, currentAccount);
		}
		while(!bus.isEmpty()) {
			AccountHandler.addBusinessServer(conn, invalidateList, bus.pop(), host, true);
		}

		UsernameHandler.addUsername(conn, source, invalidateList, PackageHandler.getNameForPackage(conn, owner), username, false);

		AccountHandler.addAdministrator(
			conn,
			source,
			invalidateList,
			username,
			hostname+" backup",
			null,
			-1,
			true,
			contact_phone,
			null,
			null,
			null,
			contact_email,
			null,
			null,
			null,
			null,
			null,
			null
		);
		conn.update("insert into master.\"User\" values(?,true,false,false,false,false,false,false)", username);
		invalidateList.addTable(conn, Table.TableID.MASTER_USERS, packageAccount, InvalidateList.allServers, false);
		conn.update("insert into master.\"UserHost\"(username, server) values(?,?)", username, host);
		invalidateList.addTable(conn, Table.TableID.MASTER_SERVERS, packageAccount, InvalidateList.allServers, false);
		AccountHandler.setAdministratorPassword(conn, source, invalidateList, username, password);

		return host;
	}*/

	/*public static void checkAccessServer(DatabaseConnection conn, RequestSource source, String action, String server) throws IOException, SQLException {
		if(!canAccessServer(conn, source, server)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access server: action='"
				+action
				+", hostname="
				+server
			;
			UserHost.reportSecurityMessage(source, message);
			throw new SQLException(message);
		}
	}*/

	public static void checkAccessHost(DatabaseConnection conn, RequestSource source, String action, int host) throws IOException, SQLException {
		if(!canAccessHost(conn, source, host)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access server: action='"
				+action
				+", server.id="
				+host
			;
			throw new SQLException(message);
		}
	}

	/*
	public static boolean canAccessServer(DatabaseConnection conn, RequestSource source, String server) throws IOException, SQLException {
		return getAllowedServers(conn, source).contains(server);
	}*/

	public static boolean canAccessHost(DatabaseAccess db, RequestSource source, int host) throws IOException, SQLException {
		return getAllowedHosts(db, source).contains(host);
	}

	/**
	 * Gets the servers that are allowed for the provided username.
	 */
	static List<Integer> getAllowedHosts(DatabaseAccess db, RequestSource source) throws IOException, SQLException {
		synchronized(NetHostHandler.class) {
			com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
			if(userHosts==null) userHosts=new HashMap<>();
			List<Integer> sv = userHosts.get(currentAdministrator);
			if(sv == null) {
				sv = new SortedIntArrayList();
					User mu = MasterServer.getUser(db, currentAdministrator);
					if(mu!=null) {
						UserHost[] masterServers = MasterServer.getUserHosts(db, currentAdministrator);
						if(masterServers.length!=0) {
							for(UserHost masterServer : masterServers) {
								sv.add(masterServer.getServerPKey());
							}
						} else {
							sv.addAll(db.queryIntList("select id from net.\"Host\""));
						}
					} else {
						sv.addAll(
							db.queryIntList(
								"select\n"
								+ "  bs.server\n"
								+ "from\n"
								+ "  account.\"User\" un,\n"
								+ "  billing.\"Package\" pk,\n"
								+ "  account.\"AccountHost\" bs\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk.name\n"
								+ "  and pk.accounting=bs.accounting",
								currentAdministrator
							)
						);
					}
				userHosts.put(currentAdministrator, sv);
			}
			return sv;
		}
	}

	public static List<Account.Name> getAccountsForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.queryList(
			ObjectFactories.accountNameFactory,
			"select accounting from account.\"AccountHost\" where server=?",
			host
		);
	}

	// TODO: Move to LinuxServerHandler
	private static final Map<Integer, Integer> failoverServers=new HashMap<>();
	public static int getFailoverServer(DatabaseAccess db, int linuxServer) throws IOException, SQLException {
		synchronized(failoverServers) {
			if(failoverServers.containsKey(linuxServer)) return failoverServers.get(linuxServer);
			int failoverServer = db.queryInt(
				"select\n"
				+ "  coalesce(\n"
				+ "    (\n"
				+ "      select\n"
				+ "        failover_server\n"
				+ "      from\n"
				+ "        linux.\"Server\"\n"
				+ "      where\n"
				+ "        server=?\n"
				+ "    ), -1\n"
				+ "  )",
				linuxServer
			);
			failoverServers.put(linuxServer, failoverServer);
			return failoverServer;
		}
	}

	private static final Map<Integer, String> farmForHosts = new HashMap<>();
	public static String getFarmForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		Integer i = host;
		synchronized(farmForHosts) {
			String farm = farmForHosts.get(i);
			if(farm == null) {
				farm = conn.queryString("select farm from net.\"Host\" where id=?", host);
				farmForHosts.put(i, farm);
			}
			return farm;
		}
	}

	// TODO: Move to LinuxServerHandler
	private static final Map<Integer, DomainName> hostnamesForLinuxServers = new HashMap<>();
	public static DomainName getHostnameForLinuxServer(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		Integer mapKey = linuxServer;
		synchronized(hostnamesForLinuxServers) {
			DomainName hostname = hostnamesForLinuxServers.get(mapKey);
			if(hostname == null) {
				hostname = database.queryObject(
					ObjectFactories.domainNameFactory,
					"select hostname from linux.\"Server\" where server=?",
					linuxServer
				);
				hostnamesForLinuxServers.put(mapKey, hostname.intern());
			}
			return hostname;
		}
	}

	/**
	 * Gets the operating system version for a server or <code>-1</code> if not available.
	 */
	public static int getOperatingSystemVersionForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.queryInt("select coalesce((select operating_system_version from net.\"Host\" where id=?), -1)", host);
	}

	// TODO: Move to LinuxServerHandler
	private static final Map<DomainName, Integer> hostsForLinuxServerHostnames = new HashMap<>();
	public static int getHostForLinuxServerHostname(DatabaseAccess db, DomainName hostname) throws IOException, SQLException {
		synchronized(hostsForLinuxServerHostnames) {
			Integer i = hostsForLinuxServerHostnames.get(hostname);
			int host;
			if(i == null) {
				host = db.queryInt("select server from linux.\"Server\" where hostname=?", hostname);
				hostsForLinuxServerHostnames.put(hostname, host);
			} else {
				host = i;
			}
			return host;
		}
	}

	public static int getHostForPackageAndName(DatabaseAccess database, int packageId, String name) throws IOException, SQLException {
		return database.queryInt("select id from net.\"Host\" where package=? and name=?", packageId, name);
	}

	public static IntList getHosts(DatabaseConnection conn) throws IOException, SQLException {
		return conn.queryIntList("select id from net.\"Host\"");
	}

	// TODO: Move to VirtualServerHandler
	/**
	 * Gets all of the Xen physical servers.
	 */
	public static IntList getEnabledXenPhysicalServers(DatabaseAccess database) throws IOException, SQLException {
		return database.queryIntList(
			"select se.id from net.\"Host\" se inner join infrastructure.\"PhysicalServer\" ps on se.id=ps.server where se.operating_system_version in (?,?,?) and se.monitoring_enabled",
			OperatingSystemVersion.CENTOS_5_DOM0_I686,
			OperatingSystemVersion.CENTOS_5_DOM0_X86_64,
			OperatingSystemVersion.CENTOS_7_DOM0_X86_64
		);
	}

	// TODO: Move to LinuxServerHandler
	private static final Map<Integer, Boolean> linuxServers = new HashMap<>();
	public static boolean isLinuxServer(DatabaseConnection conn, int host) throws IOException, SQLException {
		Integer i = host;
		synchronized(linuxServers) {
			if(linuxServers.containsKey(i)) return linuxServers.get(i);
			boolean isLinuxServer = conn.queryBoolean(
				"select (select server from linux.\"Server\" where server=?) is not null",
				host
			);
			linuxServers.put(i, isLinuxServer);
			return isLinuxServer;
		}
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.AO_SERVERS) {
			synchronized(linuxServers) {
				linuxServers.clear();
			}
		} else if(tableID==Table.TableID.BUSINESS_SERVERS) {
			synchronized(NetHostHandler.class) {
				userHosts=null;
			}
		} else if(tableID==Table.TableID.MASTER_SERVERS) {
			synchronized(NetHostHandler.class) {
				userHosts=null;
			}
		} else if(tableID==Table.TableID.SERVERS) {
			synchronized(failoverServers) {
				failoverServers.clear();
			}
			synchronized(farmForHosts) {
				farmForHosts.clear();
			}
			synchronized(hostnamesForLinuxServers) {
				hostnamesForLinuxServers.clear();
			}
			synchronized(hostsForLinuxServerHostnames) {
				hostsForLinuxServerHostnames.clear();
			}
		} else if(tableID==Table.TableID.SERVER_FARMS) {
		}
	}

	private static final Object invalidateSyncLock = new Object();

	/**
	 * HashMap(server)->HashMap(Long(id))->RequestSource
	 */
	private static final Map<Integer, Map<Long, RequestSource>> invalidateSyncEntries = new HashMap<>();

	/**
	 * HashMap(Host)->Long(lastID)
	 */
	private static final Map<Integer, Long> lastIDs = new HashMap<>();

	public static Long addInvalidateSyncEntry(int host, RequestSource source) {
		Integer s = host;
		// Note: No notify() or notifyAll() since notification is done in removeInvalidateSyncEntry(int, Long) below
		synchronized(invalidateSyncLock) {
			long id;
			Long l = lastIDs.get(s);
			if(l == null) id = 0;
			else id = l;
			Long idLong = id;
			lastIDs.put(s, idLong);

			Map<Long, RequestSource> ids=invalidateSyncEntries.get(s);
			if(ids == null) invalidateSyncEntries.put(s, ids = new HashMap<>());
			ids.put(idLong, source);

			return idLong;
		}
	}

	public static void removeInvalidateSyncEntry(int host, Long id) {
		Integer s = host;
		synchronized(invalidateSyncLock) {
			Map<Long, RequestSource> ids = invalidateSyncEntries.get(s);
			if(ids != null) ids.remove(id);
			invalidateSyncLock.notify(); // notifyAll() not needed: each waiting thread also calls notify() before returning
		}
	}

	public static void waitForInvalidates(int host) {
		Integer s = host;
		synchronized(invalidateSyncLock) {
			Long l = lastIDs.get(s);
			if(l != null) {
				long lastID = l;
				Map<Long, RequestSource> ids = invalidateSyncEntries.get(s);
				if(ids != null) {
					// Wait until the most recent ID and all previous IDs have been completed, but do
					// not wait for more than 60 seconds total to prevent locked-up daemons from
					// locking up everything.
					long startTime=System.currentTimeMillis();
					while(!Thread.currentThread().isInterrupted()) {
						long maxWait=startTime+60000-System.currentTimeMillis();
						if(maxWait>0 && maxWait<=60000) {
							LongList closedIDs=null;
							Iterator<Long> iter = ids.keySet().iterator();
							boolean foundOlder = false;
							while(iter.hasNext()) {
								Long idLong = iter.next();
								RequestSource source=ids.get(idLong);
								if(source.isClosed()) {
									if(closedIDs==null) closedIDs=new LongArrayList();
									closedIDs.add(idLong);
								} else {
									long id=idLong;
									if(id<=lastID) {
										foundOlder=true;
										break;
									}
								}
							}
							if(closedIDs!=null) {
								int size=closedIDs.size();
								for(int c = 0; c < size; c++) {
									ids.remove(closedIDs.get(c));
								}
							}
							if(foundOlder) {
								try {
									invalidateSyncLock.wait(maxWait);
								} catch(InterruptedException err) {
									logger.log(Level.WARNING, null, err);
									// Restore the interrupted status
									Thread.currentThread().interrupt();
								}
							} else {
								invalidateSyncLock.notify(); // notifyAll() not needed: each waiting thread also calls notify() before returning
								return;
							}
						} else {
							logger.log(Level.WARNING, "waitForInvalidates has taken more than 60 seconds, returning even though the invalidates have not completed synchronization: {0}", host);
							invalidateSyncLock.notify(); // notifyAll() not needed: each waiting thread also calls notify() before returning
							return;
						}
					}
				}
			}
		}
	}

	/**
	 * Gets the package that owns the server.
	 */
	public static int getPackageForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.queryInt("select package from net.\"Host\" where id=?", host);
	}

	/**
	 * Gets the per-package unique name of the server.
	 */
	public static String getNameForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.queryString("select name from net.\"Host\" where id=?", host);
	}
}
