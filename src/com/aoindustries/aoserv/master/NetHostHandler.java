/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.distribution.OperatingSystemVersion;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.DomainName;
import com.aoindustries.util.IntList;
import com.aoindustries.util.LongArrayList;
import com.aoindustries.util.LongList;
import com.aoindustries.util.SortedIntArrayList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
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
final public class NetHostHandler {

	private static final Logger logger = LogFactory.getLogger(NetHostHandler.class);

	private NetHostHandler() {
	}

	private static Map<com.aoindustries.aoserv.client.account.User.Name,List<Integer>> userHosts;

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
			!conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select can_add_backup_server from account.\"Account\" where accounting=?", account)
		) throw new SQLException("Not allowed to add_backup_server: "+source.getUsername());

		MasterServer.checkAccessHostname(conn, source, "addBackupServer", hostname);

		String farm_owner=conn.executeStringQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			true,
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

		int host = conn.executeIntUpdate(
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
		conn.executeUpdate("insert into master.\"User\" values(?,true,false,false,false,false,false,false)", username);
		invalidateList.addTable(conn, Table.TableID.MASTER_USERS, packageAccount, InvalidateList.allServers, false);
		conn.executeUpdate("insert into master.\"UserHost\"(username, server) values(?,?)", username, host);
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

	public static boolean canAccessHost(DatabaseConnection conn, RequestSource source, int host) throws IOException, SQLException {
		return getAllowedHosts(conn, source).contains(host);
	}

	/**
	 * Gets the servers that are allowed for the provided username.
	 */
	static List<Integer> getAllowedHosts(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		synchronized(NetHostHandler.class) {
			com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
			if(userHosts==null) userHosts=new HashMap<>();
			List<Integer> SV=userHosts.get(currentAdministrator);
			if(SV==null) {
				SV=new SortedIntArrayList();
					User mu = MasterServer.getUser(conn, currentAdministrator);
					if(mu!=null) {
						UserHost[] masterServers = MasterServer.getUserHosts(conn, currentAdministrator);
						if(masterServers.length!=0) {
							for(UserHost masterServer : masterServers) {
								SV.add(masterServer.getServerPKey());
							}
						} else {
							SV.addAll(conn.executeIntListQuery("select id from net.\"Host\""));
						}
					} else {
						SV.addAll(
							conn.executeIntListQuery(
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
				userHosts.put(currentAdministrator, SV);
			}
			return SV;
		}
	}

	public static List<Account.Name> getAccountsForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.executeObjectCollectionQuery(new ArrayList<>(),
			ObjectFactories.accountNameFactory,
			"select accounting from account.\"AccountHost\" where server=?",
			host
		);
	}

	// TODO: Move to LinuxServerHandler
	final private static Map<Integer,Integer> failoverServers=new HashMap<>();
	public static int getFailoverServer(DatabaseConnection conn, int linuxServer) throws IOException, SQLException {
		synchronized(failoverServers) {
			if(failoverServers.containsKey(linuxServer)) return failoverServers.get(linuxServer);
			int failoverServer=conn.executeIntQuery(
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

	final private static Map<Integer,String> farmForHosts = new HashMap<>();
	public static String getFarmForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		Integer I=host;
		synchronized(farmForHosts) {
			String farm=farmForHosts.get(I);
			if(farm==null) {
				farm=conn.executeStringQuery("select farm from net.\"Host\" where id=?", host);
				farmForHosts.put(I, farm);
			}
			return farm;
		}
	}

	// TODO: Move to LinuxServerHandler
	final private static Map<Integer,DomainName> hostnamesForLinuxServers = new HashMap<>();
	public static DomainName getHostnameForLinuxServer(DatabaseAccess database, int linuxServer) throws IOException, SQLException {
		Integer mapKey = linuxServer;
		synchronized(hostnamesForLinuxServers) {
			DomainName hostname = hostnamesForLinuxServers.get(mapKey);
			if(hostname == null) {
				hostname = database.executeObjectQuery(
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
		return conn.executeIntQuery("select coalesce((select operating_system_version from net.\"Host\" where id=?), -1)", host);
	}

	// TODO: Move to LinuxServerHandler
	final private static Map<DomainName,Integer> hostsForLinuxServerHostnames = new HashMap<>();
	public static int getHostForLinuxServerHostname(DatabaseConnection conn, DomainName hostname) throws IOException, SQLException {
		synchronized(hostsForLinuxServerHostnames) {
			Integer I = hostsForLinuxServerHostnames.get(hostname);
			int host;
			if(I == null) {
				host = conn.executeIntQuery("select server from linux.\"Server\" where hostname=?", hostname);
				hostsForLinuxServerHostnames.put(hostname, host);
			} else {
				host = I;
			}
			return host;
		}
	}

	public static int getHostForPackageAndName(DatabaseAccess database, int packageId, String name) throws IOException, SQLException {
		return database.executeIntQuery("select id from net.\"Host\" where package=? and name=?", packageId, name);
	}

	public static IntList getHosts(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntListQuery("select id from net.\"Host\"");
	}

	// TODO: Move to VirtualServerHandler
	/**
	 * Gets all of the Xen physical servers.
	 */
	public static IntList getEnabledXenPhysicalServers(DatabaseAccess database) throws IOException, SQLException {
		return database.executeIntListQuery(
			"select se.id from net.\"Host\" se inner join infrastructure.\"PhysicalServer\" ps on se.id=ps.server where se.operating_system_version in (?,?,?) and se.monitoring_enabled",
			OperatingSystemVersion.CENTOS_5_DOM0_I686,
			OperatingSystemVersion.CENTOS_5_DOM0_X86_64,
			OperatingSystemVersion.CENTOS_7_DOM0_X86_64
		);
	}

	// TODO: Move to LinuxServerHandler
	final private static Map<Integer,Boolean> linuxServers = new HashMap<>();
	public static boolean isLinuxServer(DatabaseConnection conn, int host) throws IOException, SQLException {
		Integer I = host;
		synchronized(linuxServers) {
			if(linuxServers.containsKey(I)) return linuxServers.get(I);
			boolean isLinuxServer = conn.executeBooleanQuery(
				"select (select server from linux.\"Server\" where server=?) is not null",
				host
			);
			linuxServers.put(I, isLinuxServer);
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
	private static final Map<Integer,Map<Long,RequestSource>> invalidateSyncEntries = new HashMap<>();

	/**
	 * HashMap(Host)->Long(lastID)
	 */
	private static final Map<Integer,Long> lastIDs = new HashMap<>();

	public static Long addInvalidateSyncEntry(int host, RequestSource source) {
		Integer S=host;
		synchronized(invalidateSyncLock) {
			long id;
			Long L=lastIDs.get(S);
			if(L==null) id=0;
			else id=L;
			Long idLong=id;
			lastIDs.put(S, idLong);

			Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
			if(ids==null) invalidateSyncEntries.put(S, ids=new HashMap<>());
			ids.put(idLong, source);

			return idLong;
		}
	}

	public static void removeInvalidateSyncEntry(int host, Long id) {
		Integer S=host;
		synchronized(invalidateSyncLock) {
			Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
			if(ids!=null) ids.remove(id);
			invalidateSyncLock.notify();
		}
	}

	public static void waitForInvalidates(int host) {
		Integer S=host;
		synchronized(invalidateSyncLock) {
			Long L=lastIDs.get(S);
			if(L!=null) {
				long lastID=L;
				Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
				if(ids!=null) {
					// Wait until the most recent ID and all previous IDs have been completed, but do
					// not wait for more than 60 seconds total to prevent locked-up daemons from
					// locking up everything.
					long startTime=System.currentTimeMillis();
					while(true) {
						long maxWait=startTime+60000-System.currentTimeMillis();
						if(maxWait>0 && maxWait<=60000) {
							LongList closedIDs=null;
							Iterator<Long> I=ids.keySet().iterator();
							boolean foundOlder=false;
							while(I.hasNext()) {
								Long idLong=I.next();
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
								for(int c=0;c<size;c++) ids.remove(closedIDs.get(c));
							}
							if(foundOlder) {
								try {
									invalidateSyncLock.wait(maxWait);
								} catch(InterruptedException err) {
									logger.log(Level.WARNING, null, err);
								}
							} else {
								invalidateSyncLock.notify();
								return;
							}
						} else {
							System.err.println("waitForInvalidates has taken more than 60 seconds, returning even though the invalidates have not completed synchronization: "+host);
							Thread.dumpStack();
							invalidateSyncLock.notify();
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
		return conn.executeIntQuery("select package from net.\"Host\" where id=?", host);
	}

	/**
	 * Gets the per-package unique name of the server.
	 */
	public static String getNameForHost(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.executeStringQuery("select name from net.\"Host\" where id=?", host);
	}
}
