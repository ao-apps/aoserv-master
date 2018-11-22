/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
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
 * The <code>ServerHandler</code> handles all the accesses to the Server tables.
 *
 * @author  AO Industries, Inc.
 */
final public class ServerHandler {

	private static final Logger logger = LogFactory.getLogger(ServerHandler.class);

	private ServerHandler() {
	}

	private static Map<UserId,List<Integer>> usernameServers;

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
		String accounting=UsernameHandler.getBusinessForUsername(conn, source.getUsername());
		if(
			!conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select can_add_backup_server from account.\"Account\" where accounting=?", accounting)
		) throw new SQLException("Not allowed to add_backup_server: "+source.getUsername());

		MasterServer.checkAccessHostname(conn, source, "addBackupServer", hostname);

		String farm_owner=conn.executeStringQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			true,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  server_farms sf,\n"
			+ "  packages pk\n"
			+ "where\n"
			+ "  sf.name=?\n"
			+ "  and sf.owner=pk.pkey",
			farm
		);
		if(!BusinessHandler.isBusinessOrParent(conn, accounting, farm_owner)) throw new SQLException("Not able to access farm: "+farm);

		PackageHandler.checkAccessPackage(conn, source, "addBackupServer", owner);

		String check = Username.checkUsername(username, Locale.getDefault());
		if(check!=null) throw new SQLException(check);

		PasswordChecker.Result[] results = BusinessAdministrator.checkPassword(Locale.getDefault(), username, password);
		if(PasswordChecker.hasResults(Locale.getDefault(), results)) throw new SQLException("Password strength check failed: "+PasswordChecker.getResultsString(results).replace('\n', '|'));

		int serverPKey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  servers\n"
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
			+ ") RETURNING pkey",
			hostname,
			farm,
			PackageHandler.getBusinessForPackage(conn, owner),
			description,
			os_version
		);
		invalidateList.addTable(conn, SchemaTable.TableID.SERVERS, accounting, InvalidateList.allServers, false);

		// Build a stack of parents, adding each business_server
		Stack<String> bus=new Stack<String>();
		String packageAccounting=PackageHandler.getBusinessForPackage(conn, owner);
		String currentAccounting=packageAccounting;
		while(true) {
			bus.push(currentAccounting);
			if(currentAccounting.equals(BusinessHandler.getRootBusiness())) break;
			currentAccounting=BusinessHandler.getParentBusiness(conn, currentAccounting);
		}
		while(!bus.isEmpty()) {
			BusinessHandler.addBusinessServer(conn, invalidateList, bus.pop(), serverPKey, true);
		}

		UsernameHandler.addUsername(conn, source, invalidateList, PackageHandler.getNameForPackage(conn, owner), username, false);

		BusinessHandler.addBusinessAdministrator(
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
		conn.executeUpdate("insert into master_users values(?,true,false,false,false,false,false,false)", username);
		invalidateList.addTable(conn, SchemaTable.TableID.MASTER_USERS, packageAccounting, InvalidateList.allServers, false);
		conn.executeUpdate("insert into master_servers(username, server) values(?,?)", username, serverPKey);
		invalidateList.addTable(conn, SchemaTable.TableID.MASTER_SERVERS, packageAccounting, InvalidateList.allServers, false);
		BusinessHandler.setBusinessAdministratorPassword(conn, source, invalidateList, username, password);

		return serverPKey;
	}*/

	/*public static void checkAccessServer(DatabaseConnection conn, RequestSource source, String action, String server) throws IOException, SQLException {
		if(!canAccessServer(conn, source, server)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access server: action='"
				+action
				+", hostname="
				+server
			;
			MasterServer.reportSecurityMessage(source, message);
			throw new SQLException(message);
		}
	}*/

	public static void checkAccessServer(DatabaseConnection conn, RequestSource source, String action, int server) throws IOException, SQLException {
		if(!canAccessServer(conn, source, server)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access server: action='"
				+action
				+", server.pkey="
				+server
			;
			throw new SQLException(message);
		}
	}

	/*
	public static boolean canAccessServer(DatabaseConnection conn, RequestSource source, String server) throws IOException, SQLException {
		return getAllowedServers(conn, source).contains(server);
	}*/

	public static boolean canAccessServer(DatabaseConnection conn, RequestSource source, int server) throws IOException, SQLException {
		return getAllowedServers(conn, source).contains(server);
	}

	/**
	 * Gets the servers that are allowed for the provided username.
	 */
	static List<Integer> getAllowedServers(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		synchronized(ServerHandler.class) {
			UserId username=source.getUsername();
			if(usernameServers==null) usernameServers=new HashMap<>();
			List<Integer> SV=usernameServers.get(username);
			if(SV==null) {
				SV=new SortedIntArrayList();
					MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
					if(mu!=null) {
						com.aoindustries.aoserv.client.MasterServer[] masterServers = MasterServer.getMasterServers(conn, source.getUsername());
						if(masterServers.length!=0) {
							for(com.aoindustries.aoserv.client.MasterServer masterServer : masterServers) {
								SV.add(masterServer.getServerPKey());
							}
						} else {
							SV.addAll(conn.executeIntListQuery("select pkey from servers"));
						}
					} else {
						SV.addAll(
							conn.executeIntListQuery(
								"select\n"
								+ "  bs.server\n"
								+ "from\n"
								+ "  account.\"Username\" un,\n"
								+ "  packages pk,\n"
								+ "  business_servers bs\n"
								+ "where\n"
								+ "  un.username=?\n"
								+ "  and un.package=pk.name\n"
								+ "  and pk.accounting=bs.accounting",
								username
							)
						);
					}
				usernameServers.put(username, SV);
			}
			return SV;
		}
	}

	public static List<AccountingCode> getBusinessesForServer(DatabaseConnection conn, int server) throws IOException, SQLException {
		return conn.executeObjectCollectionQuery(
			new ArrayList<AccountingCode>(),
			ObjectFactories.accountingCodeFactory,
			"select accounting from business_servers where server=?",
			server
		);
	}

	final private static Map<Integer,Integer> failoverServers=new HashMap<>();
	public static int getFailoverServer(DatabaseConnection conn, int aoServer) throws IOException, SQLException {
		synchronized(failoverServers) {
			if(failoverServers.containsKey(aoServer)) return failoverServers.get(aoServer);
			int failoverServer=conn.executeIntQuery(
				"select\n"
				+ "  coalesce(\n"
				+ "    (\n"
				+ "      select\n"
				+ "        failover_server\n"
				+ "      from\n"
				+ "        ao_servers\n"
				+ "      where\n"
				+ "        server=?\n"
				+ "    ), -1\n"
				+ "  )",
				aoServer
			);
			failoverServers.put(aoServer, failoverServer);
			return failoverServer;
		}
	}

	final private static Map<Integer,String> farmsForServers=new HashMap<>();
	public static String getFarmForServer(DatabaseConnection conn, int server) throws IOException, SQLException {
		Integer I=server;
		synchronized(farmsForServers) {
			String farm=farmsForServers.get(I);
			if(farm==null) {
				farm=conn.executeStringQuery("select farm from servers where pkey=?", server);
				farmsForServers.put(I, farm);
			}
			return farm;
		}
	}

	final private static Map<Integer,DomainName> hostnamesForAOServers=new HashMap<>();
	public static DomainName getHostnameForAOServer(DatabaseAccess database, int aoServer) throws IOException, SQLException {
		Integer mapKey = aoServer;
		synchronized(hostnamesForAOServers) {
			DomainName hostname = hostnamesForAOServers.get(mapKey);
			if(hostname == null) {
				hostname = database.executeObjectQuery(
					ObjectFactories.domainNameFactory,
					"select hostname from ao_servers where server=?",
					aoServer
				);
				hostnamesForAOServers.put(mapKey, hostname.intern());
			}
			return hostname;
		}
	}

	/**
	 * Gets the operating system version for a server or <code>-1</code> if not available.
	 */
	public static int getOperatingSystemVersionForServer(DatabaseConnection conn, int server) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce((select operating_system_version from servers where pkey=?), -1)", server);
	}

	final private static Map<DomainName,Integer> serversForAOServers = new HashMap<>();
	public static int getServerForAOServerHostname(DatabaseConnection conn, DomainName aoServerHostname) throws IOException, SQLException {
		synchronized(serversForAOServers) {
			Integer I = serversForAOServers.get(aoServerHostname);
			int server;
			if(I == null) {
				server = conn.executeIntQuery("select server from ao_servers where hostname=?", aoServerHostname);
				serversForAOServers.put(aoServerHostname, server);
			} else {
				server = I;
			}
			return server;
		}
	}

	public static int getServerForPackageAndName(DatabaseAccess database, int pack, String name) throws IOException, SQLException {
		return database.executeIntQuery("select pkey from servers where package=? and name=?", pack, name);
	}

	public static IntList getServers(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from servers");
	}

	/**
	 * Gets all of the Xen physical servers.
	 */
	public static IntList getEnabledXenPhysicalServers(DatabaseAccess database) throws IOException, SQLException {
		return database.executeIntListQuery(
			"select se.pkey from servers se inner join physical_servers ps on se.pkey=ps.server where se.operating_system_version in (?,?,?) and se.monitoring_enabled",
			OperatingSystemVersion.CENTOS_5_DOM0_I686,
			OperatingSystemVersion.CENTOS_5_DOM0_X86_64,
			OperatingSystemVersion.CENTOS_7_DOM0_X86_64
		);
	}

	final private static Map<Integer,Boolean> aoServers=new HashMap<>();
	public static boolean isAOServer(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		Integer I=pkey;
		synchronized(aoServers) {
			if(aoServers.containsKey(I)) return aoServers.get(I);
			boolean isAOServer=conn.executeBooleanQuery(
				"select (select server from ao_servers where server=?) is not null",
				pkey
			);
			aoServers.put(I, isAOServer);
			return isAOServer;
		}
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.AO_SERVERS) {
			synchronized(aoServers) {
				aoServers.clear();
			}
		} else if(tableID==SchemaTable.TableID.BUSINESS_SERVERS) {
			synchronized(ServerHandler.class) {
				usernameServers=null;
			}
		} else if(tableID==SchemaTable.TableID.MASTER_SERVERS) {
			synchronized(ServerHandler.class) {
				usernameServers=null;
			}
		} else if(tableID==SchemaTable.TableID.SERVERS) {
			synchronized(failoverServers) {
				failoverServers.clear();
			}
			synchronized(farmsForServers) {
				farmsForServers.clear();
			}
			synchronized(hostnamesForAOServers) {
				hostnamesForAOServers.clear();
			}
			synchronized(serversForAOServers) {
				serversForAOServers.clear();
			}
		} else if(tableID==SchemaTable.TableID.SERVER_FARMS) {
		}
	}

	private static final Object invalidateSyncLock=new Object();

	/**
	 * HashMap(server)->HashMap(Long(id))->RequestSource
	 */
	private static final Map<Integer,Map<Long,RequestSource>> invalidateSyncEntries=new HashMap<>();

	/**
	 * HashMap(Server)->Long(lastID)
	 */
	private static final Map<Integer,Long> lastIDs=new HashMap<>();

	public static Long addInvalidateSyncEntry(int server, RequestSource source) {
		Integer S=server;
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

	public static void removeInvalidateSyncEntry(int server, Long id) {
		Integer S=server;
		synchronized(invalidateSyncLock) {
			Map<Long,RequestSource> ids=invalidateSyncEntries.get(S);
			if(ids!=null) ids.remove(id);
			invalidateSyncLock.notify();
		}
	}

	public static void waitForInvalidates(int server) {
		Integer S=server;
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
							System.err.println("waitForInvalidates has taken more than 60 seconds, returning even though the invalidates have not completed synchronization: "+server);
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
	public static int getPackageForServer(DatabaseConnection conn, int server) throws IOException, SQLException {
		return conn.executeIntQuery("select package from servers where pkey=?", server);
	}

	/**
	 * Gets the per-package unique name of the server.
	 */
	public static String getNameForServer(DatabaseConnection conn, int server) throws IOException, SQLException {
		return conn.executeStringQuery("select name from servers where pkey=?", server);
	}
}
