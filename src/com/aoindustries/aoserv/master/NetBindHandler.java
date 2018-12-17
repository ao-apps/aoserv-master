/*
 * Copyright 2001-2013, 2014, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * The <code>NetBindHandler</code> handles all the accesses to the <code>net.Bind</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetBindHandler {

	private NetBindHandler() {
	}

	/**
	 * This lock is used to avoid a race condition between check and insert when allocating net.Bind.
	 */
	private static final Object netBindLock = new Object();

	public static int addNetBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int server,
		Account.Name packageName,
		int ipAddress,
		Port port,
		String appProtocol,
		boolean monitoringEnabled,
		Set<FirewallZone.Name> firewalldZones
	) throws IOException, SQLException {
		if(
			conn.executeBooleanQuery("select (select protocol from net.\"AppProtocol\" where protocol=?) is null", appProtocol)
		) throw new SQLException("Unable to find in table net.AppProtocol: "+appProtocol);

		User mu=MasterServer.getUser(conn, source.getUsername());
		if(mu==null) {
			// Must be a user service
			if(
				!conn.executeBooleanQuery("select is_user_service from net.\"AppProtocol\" where protocol=?", appProtocol)
			) throw new SQLException("Only master users may add non-user net.Bind.");

			// Must match the default port
			Port defaultPort = conn.executeObjectQuery(
				ObjectFactories.portFactory,
				"select port, net_protocol from net.\"AppProtocol\" where protocol=?",
				appProtocol
			);
			if(port != defaultPort) throw new SQLException("Only master users may override the port for a service.");
		}

		ServerHandler.checkAccessServer(conn, source, "addNetBind", server);
		PackageHandler.checkAccessPackage(conn, source, "addNetBind", packageName);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add net bind, package disabled: "+packageName);
		IPAddressHandler.checkAccessIPAddress(conn, source, "addNetBind", ipAddress);
		InetAddress inetAddress = IPAddressHandler.getInetAddressForIPAddress(conn, ipAddress);

		int id;
		synchronized(netBindLock) {
			if(inetAddress.isUnspecified()) {
				// Wildcard must be unique per server
				if(
					conn.executeBooleanQuery(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      id\n"
						+ "    from\n"
						+ "      net.\"Bind\"\n"
						+ "    where\n"
						+ "      server=?\n"
						+ "      and port=?::\"com.aoindustries.net\".\"Port\"\n"
						+ "      and net_protocol=?::\"com.aoindustries.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						server,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+server+"->"+inetAddress.toBracketedString()+":"+port);
			} else if(inetAddress.isLoopback()) {
				// Loopback must be unique per server and not have wildcard
				if(
					conn.executeBooleanQuery(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      nb.id\n"
						+ "    from\n"
						+ "      net.\"Bind\" nb\n"
						+ "      inner join net.\"IpAddress\" ia on nb.\"ipAddress\"=ia.id\n"
						+ "    where\n"
						+ "      nb.server=?\n"
						+ "      and ia.\"inetAddress\" in (?,?)\n"
						+ "      and nb.port=?::\"com.aoindustries.net\".\"Port\"\n"
						+ "      and nb.net_protocol=?::\"com.aoindustries.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						server,
						IpAddress.WILDCARD_IP,
						IpAddress.LOOPBACK_IP,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+server+"->"+inetAddress.toBracketedString()+":"+port);
			} else {
				// Make sure that this port is not already allocated within the server on this IP or the wildcard
				if(
					conn.executeBooleanQuery(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      nb.id\n"
						+ "    from\n"
						+ "      net.\"Bind\" nb\n"
						+ "      inner join net.\"IpAddress\" ia on nb.\"ipAddress\"=ia.id\n"
						+ "    where\n"
						+ "      nb.server=?\n"
						+ "      and (\n"
						+ "        ia.\"inetAddress\"=?::inet\n"
						+ "        or nb.\"ipAddress\"=?\n"
						+ "      )\n"
						+ "      and nb.port=?::\"com.aoindustries.net\".\"Port\"\n"
						+ "      and nb.net_protocol=?::\"com.aoindustries.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						server,
						IpAddress.WILDCARD_IP,
						ipAddress,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+server+"->"+inetAddress.toBracketedString()+":"+port);
			}

			// Add the port to the DB
			id = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  net.\"Bind\"\n"
				+ "VALUES (\n"
				+ "  DEFAULT,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?::\"com.aoindustries.net\".\"Port\",\n"
				+ "  ?::\"com.aoindustries.net\".\"Protocol\",\n"
				+ "  ?,\n"
				+ "  ?\n"
				+ ") RETURNING id",
				packageName,
				server,
				ipAddress,
				port.getPort(),
				port.getProtocol(),
				appProtocol,
				monitoringEnabled
			);
		}
		Account.Name business = PackageHandler.getBusinessForPackage(conn, packageName);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			business,
			server,
			false
		);
		if(!firewalldZones.isEmpty()) {
			for(FirewallZone.Name firewalldZone : firewalldZones) {
				conn.executeUpdate(
					"insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
					+ "  ?,\n"
					+ "  (select id from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
					+ ")",
					id,
					server,
					firewalldZone
				);
			}
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BIND_FIREWALLD_ZONES,
				business,
				server,
				false
			);
		}
		return id;
	}

	public static int allocateNetBind(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int server,
		int ipAddress,
		com.aoindustries.net.Protocol netProtocol,
		String appProtocol,
		Account.Name pack,
		int minimumPort
	) throws IOException, SQLException {
		int id;
		synchronized(netBindLock) {
			id = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  net.\"Bind\"\n"
				+ "VALUES (\n"
				+ "  default,\n"
				+ "  ?,\n" // package
				+ "  ?,\n" // server
				+ "  ?,\n" // ipAddress
				+ "  net.find_unused_port(\n"
				+ "    ?,\n"
				+ "    ?,\n"
				+ "    ?::\"com.aoindustries.net\".\"Port\",\n"
				+ "    ?::\"com.aoindustries.net\".\"Protocol\",\n"
				+ "    ?\n"
			    + "  ),\n" // port
				+ "  ?::\"com.aoindustries.net\".\"Protocol\",\n" // net_protocol
				+ "  ?,\n" // app_protocol
				+ "  true,\n" // monitoring_enabled
				+ "  null\n" // monitoring_parameters
				+ ") RETURNING id",
				pack,
				server,
				ipAddress,
				server,
				ipAddress,
				minimumPort,
				netProtocol.name(),
				appProtocol,
				netProtocol.name(),
				appProtocol
			);
		}
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			PackageHandler.getBusinessForPackage(conn, pack),
			server,
			false
		);
		return id;
	}

	public static Account.Name getBusinessForNetBind(
		DatabaseConnection conn,
		int id
	) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select pk.accounting from net.\"Bind\" nb, billing.\"Package\" pk where nb.id=? and nb.package=pk.name",
			id
		);
	}

	public static int getNetBind(
		DatabaseConnection conn,
		int server,
		int ipAddress,
		Port port
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        id\n"
			+ "      from\n"
			+ "        net.\"Bind\"\n"
			+ "      where\n"
			+ "        server=?\n"
			+ "        and \"ipAddress\"=?\n"
			+ "        and port=?::\"com.aoindustries.net\".\"Port\"\n"
			+ "        and net_protocol=?::\"com.aoindustries.net\".\"Protocol\"\n"
			+ "    ), -1\n"
			+ "  )",
			server,
			ipAddress,
			port.getPort(),
			port.getProtocol().name()
		);
	}

	public static int getServerForNetBind(
		DatabaseConnection conn,
		int id
	) throws IOException, SQLException {
		return conn.executeIntQuery("select server from net.\"Bind\" where id=?", id);
	}

	public static Account.Name getPackageForNetBind(
		DatabaseConnection conn,
		int id
	) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select package from net.\"Bind\" where id=?",
			id
		);
	}

	public static void removeNetBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		// Security checks
		PackageHandler.checkAccessPackage(conn, source, "removeNetBind", getPackageForNetBind(conn, id));

		// Do the remove
		removeNetBind(conn, invalidateList, id);
	}

	public static void removeNetBind(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		Account.Name business = getBusinessForNetBind(conn, id);
		int server=getServerForNetBind(conn, id);

		if(conn.executeUpdate("delete from net.\"TcpRedirect\" where net_bind=?", id) > 0) {
			invalidateList.addTable(
				conn,
				Table.TableID.NET_TCP_REDIRECTS,
				business,
				server,
				false
			);
		}

		if(conn.executeUpdate("delete from ftp.\"PrivateServer\" where net_bind=?", id) > 0) {
			invalidateList.addTable(
				conn,
				Table.TableID.PRIVATE_FTP_SERVERS,
				business,
				server,
				false
			);
		}

		conn.executeUpdate("delete from net.\"Bind\" where id=?", id);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			business,
			server,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BIND_FIREWALLD_ZONES,
			business,
			server,
			false
		);
	}

	public static void setNetBindFirewalldZones(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		Set<FirewallZone.Name> firewalldZones
	) throws IOException, SQLException {
		PackageHandler.checkAccessPackage(conn, source, "setNetBindFirewalldZones", getPackageForNetBind(conn, id));

		boolean updated = false;
		int server = getServerForNetBind(conn, id);
		if(firewalldZones.isEmpty()) {
			if(conn.executeUpdate("delete from net.\"BindFirewallZone\" where net_bind=?", id) != 0) {
				updated = true;
			}
		} else {
			// Find the set that exists
			Set<FirewallZone.Name> existing = conn.executeObjectCollectionQuery(new HashSet<>(),
				ObjectFactories.firewallZoneNameFactory,
				"select\n"
				+ "  fz.\"name\"\n"
				+ "from\n"
				+ "  net.\"BindFirewallZone\" nbfz\n"
				+ "  inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
				+ "where\n"
				+ "  nbfz.net_bind=?",
				id
			);
			// Delete extra
			for(FirewallZone.Name name : existing) {
				if(!firewalldZones.contains(name)) {
					conn.executeUpdate(
						"delete from net.\"BindFirewallZone\" where id=(\n"
						+ "  select\n"
						+ "    nbfz.id\n"
						+ "  from\n"
						+ "    net.\"BindFirewallZone\" nbfz\n"
						+ "    inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
						+ "  where\n"
						+ "    nbfz.net_bind=?\n"
						+ "    and fz.\"name\"=?\n"
						+ ")",
						id,
						name
					);
					updated = true;
				}
			}
			// Add new
			for(FirewallZone.Name name : firewalldZones) {
				if(!existing.contains(name)) {
					conn.executeUpdate(
						"insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
						+ "  ?,\n"
						+ "  (select id from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
						+ ")",
						id,
						server,
						name
					);
					updated = true;
				}
			}
		}
		if(updated) {
			Account.Name business = getBusinessForNetBind(conn, id);
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BINDS,
				business,
				server,
				false
			);
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BIND_FIREWALLD_ZONES,
				business,
				server,
				false
			);
		}
	}

	public static void setNetBindMonitoringEnabled(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		boolean enabled
	) throws IOException, SQLException {
		PackageHandler.checkAccessPackage(conn, source, "setNetBindMonitoringEnabled", getPackageForNetBind(conn, id));

		conn.executeUpdate("update net.\"Bind\" set monitoring_enabled=? where id=?", enabled, id);

		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			getBusinessForNetBind(conn, id),
			getServerForNetBind(conn, id),
			false
		);
	}

	/**
	 * This exists for compatibility with older clients (versions &lt;= 1.80.2) only.
	 * This has been implemented by adding and removing the public zone from the net_bind.
	 */
	public static void setNetBindOpenFirewall(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		boolean open_firewall
	) throws IOException, SQLException {
		AoservProtocol.Version clientVersion = source.getProtocolVersion();
		if(clientVersion.compareTo(AoservProtocol.Version.VERSION_1_80_2) > 0) {
			throw new IOException("This compatibility method only remains for clients version <= 1.80.2: Client is version " + clientVersion);
		}

		PackageHandler.checkAccessPackage(conn, source, "setNetBindOpenFirewall", getPackageForNetBind(conn, id));

		int server = getServerForNetBind(conn, id);
		if(open_firewall) {
			// Add the public zone if missing
			int fz = conn.executeIntQuery("select id from net.\"FirewallZone\" where server=? and \"name\"=?", server, FirewallZone.PUBLIC);
			boolean updated;
			synchronized(netBindLock) {
				if(
					conn.executeBooleanQuery("select (select id from net.\"BindFirewallZone\" where net_bind=? and firewalld_zone=?) is null", id, fz)
				) {
					conn.executeUpdate("insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (?,?)", id, fz);
					updated = true;
				} else {
					updated = false;
				}
			}
			if(updated) {
				Account.Name business = getBusinessForNetBind(conn, id);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BINDS,
					business,
					server,
					false
				);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BIND_FIREWALLD_ZONES,
					business,
					server,
					false
				);
			}
		} else {
			// Remove the public zone if present
			if(
				conn.executeUpdate(
					"delete from net.\"BindFirewallZone\" where net_bind=? and firewalld_zone=(select id from net.\"FirewallZone\" where server=? and \"name\"=?)",
					id,
					server,
					FirewallZone.PUBLIC
				) != 0
			) {
				Account.Name business = getBusinessForNetBind(conn, id);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BINDS,
					business,
					server,
					false
				);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BIND_FIREWALLD_ZONES,
					business,
					server,
					false
				);
			}
		}
	}
}
