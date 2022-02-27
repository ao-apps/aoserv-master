/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.collections.AoCollections;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.net.InetAddress;
import com.aoapps.net.Port;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * The <code>NetBindHandler</code> handles all the accesses to the <code>net.Bind</code> table.
 *
 * @author  AO Industries, Inc.
 */
public final class NetBindHandler {

	/** Make no instances. */
	private NetBindHandler() {throw new AssertionError();}

	/**
	 * This lock is used to avoid a race condition between check and insert when allocating net.Bind.
	 */
	private static final Object netBindLock = new Object();

	public static int addBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int host,
		Account.Name packageName,
		int ipAddress,
		Port port,
		String appProtocol,
		boolean monitoringEnabled,
		Set<FirewallZone.Name> firewalldZones
	) throws IOException, SQLException {
		if(
			conn.queryBoolean("select (select protocol from net.\"AppProtocol\" where protocol=?) is null", appProtocol)
		) throw new SQLException("Unable to find in table net.AppProtocol: "+appProtocol);

		User mu=MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu==null) {
			// Must be a user service
			if(
				!conn.queryBoolean("select is_user_service from net.\"AppProtocol\" where protocol=?", appProtocol)
			) throw new SQLException("Only master users may add non-user net.Bind.");

			// Must match the default port
			Port defaultPort = conn.queryObject(
				ObjectFactories.portFactory,
				"select port, net_protocol from net.\"AppProtocol\" where protocol=?",
				appProtocol
			);
			if(port != defaultPort) throw new SQLException("Only master users may override the port for a service.");
		}

		NetHostHandler.checkAccessHost(conn, source, "addBind", host);
		PackageHandler.checkAccessPackage(conn, source, "addBind", packageName);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add net bind, package disabled: "+packageName);
		IpAddressHandler.checkAccessIpAddress(conn, source, "addBind", ipAddress);
		InetAddress inetAddress = IpAddressHandler.getInetAddressForIpAddress(conn, ipAddress);

		int bind;
		synchronized(netBindLock) {
			if(inetAddress.isUnspecified()) {
				// Wildcard must be unique per server
				if(
					conn.queryBoolean(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      id\n"
						+ "    from\n"
						+ "      net.\"Bind\"\n"
						+ "    where\n"
						+ "      server=?\n"
						+ "      and port=?::\"com.aoapps.net\".\"Port\"\n"
						+ "      and net_protocol=?::\"com.aoapps.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						host,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+host+"->"+inetAddress.toBracketedString()+":"+port);
			} else if(inetAddress.isLoopback()) {
				// Loopback must be unique per server and not have wildcard
				if(
					conn.queryBoolean(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      nb.id\n"
						+ "    from\n"
						+ "      net.\"Bind\" nb\n"
						+ "      inner join net.\"IpAddress\" ia on nb.\"ipAddress\"=ia.id\n"
						+ "    where\n"
						+ "      nb.server=?\n"
						+ "      and ia.\"inetAddress\" in (\n"
						+ "        ?::\"com.aoapps.net\".\"InetAddress\",\n"
						+ "        ?::\"com.aoapps.net\".\"InetAddress\"\n"
						+ "      )\n"
						+ "      and nb.port=?::\"com.aoapps.net\".\"Port\"\n"
						+ "      and nb.net_protocol=?::\"com.aoapps.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						host,
						IpAddress.WILDCARD_IP,
						IpAddress.LOOPBACK_IP,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+host+"->"+inetAddress.toBracketedString()+":"+port);
			} else {
				// Make sure that this port is not already allocated within the server on this IP or the wildcard
				if(
					conn.queryBoolean(
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
						+ "        ia.\"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\"\n"
						+ "        or nb.\"ipAddress\"=?\n"
						+ "      )\n"
						+ "      and nb.port=?::\"com.aoapps.net\".\"Port\"\n"
						+ "      and nb.net_protocol=?::\"com.aoapps.net\".\"Protocol\"\n"
						+ "    limit 1\n"
						+ "  ) is not null",
						host,
						IpAddress.WILDCARD_IP,
						ipAddress,
						port.getPort(),
						port.getProtocol().name()
					)
				) throw new SQLException("Bind already in use: "+host+"->"+inetAddress.toBracketedString()+":"+port);
			}

			// Add the port to the DB
			bind = conn.updateInt(
				"INSERT INTO\n"
				+ "  net.\"Bind\"\n"
				+ "VALUES (\n"
				+ "  DEFAULT,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?::\"com.aoapps.net\".\"Port\",\n"
				+ "  ?::\"com.aoapps.net\".\"Protocol\",\n"
				+ "  ?,\n"
				+ "  ?\n"
				+ ") RETURNING id",
				packageName,
				host,
				ipAddress,
				port.getPort(),
				port.getProtocol(),
				appProtocol,
				monitoringEnabled
			);
		}
		Account.Name business = PackageHandler.getAccountForPackage(conn, packageName);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			business,
			host,
			false
		);
		if(!firewalldZones.isEmpty()) {
			for(FirewallZone.Name firewalldZone : firewalldZones) {
				conn.update(
					"insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
					+ "  ?,\n"
					+ "  (select id from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
					+ ")",
					bind,
					host,
					firewalldZone
				);
			}
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BIND_FIREWALLD_ZONES,
				business,
				host,
				false
			);
		}
		return bind;
	}

	public static int allocateBind(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int host,
		int ipAddress,
		com.aoapps.net.Protocol netProtocol,
		String appProtocol,
		Account.Name packageName,
		int minimumPort
	) throws IOException, SQLException {
		int bind;
		synchronized(netBindLock) {
			bind = conn.updateInt(
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
				+ "    ?::\"com.aoapps.net\".\"Port\",\n"
				+ "    ?::\"com.aoapps.net\".\"Protocol\",\n"
				+ "    ?\n"
			    + "  ),\n" // port
				+ "  ?::\"com.aoapps.net\".\"Protocol\",\n" // net_protocol
				+ "  ?,\n" // app_protocol
				+ "  true,\n" // monitoring_enabled
				+ "  null\n" // monitoring_parameters
				+ ") RETURNING id",
				packageName,
				host,
				ipAddress,
				host,
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
			PackageHandler.getAccountForPackage(conn, packageName),
			host,
			false
		);
		return bind;
	}

	public static Account.Name getAccountForBind(DatabaseConnection conn, int bind) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select pk.accounting from net.\"Bind\" nb, billing.\"Package\" pk where nb.id=? and nb.package=pk.name",
			bind
		);
	}

	public static int getBind(
		DatabaseConnection conn,
		int host,
		int ipAddress,
		Port port
	) throws IOException, SQLException {
		return conn.queryInt(
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
			+ "        and port=?::\"com.aoapps.net\".\"Port\"\n"
			+ "        and net_protocol=?::\"com.aoapps.net\".\"Protocol\"\n"
			+ "    ), -1\n"
			+ "  )",
			host,
			ipAddress,
			port.getPort(),
			port.getProtocol().name()
		);
	}

	public static int getHostForBind(DatabaseConnection conn, int bind) throws IOException, SQLException {
		return conn.queryInt("select server from net.\"Bind\" where id=?", bind);
	}

	public static Account.Name getPackageForBind(DatabaseConnection conn, int bind) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select package from net.\"Bind\" where id=?",
			bind
		);
	}

	public static void removeBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int bind
	) throws IOException, SQLException {
		// Security checks
		PackageHandler.checkAccessPackage(conn, source, "removeBind", getPackageForBind(conn, bind));

		// Do the remove
		removeBind(conn, invalidateList, bind);
	}

	public static void removeBind(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int bind
	) throws IOException, SQLException {
		Account.Name business = getAccountForBind(conn, bind);
		int host = getHostForBind(conn, bind);

		if(conn.update("delete from net.\"TcpRedirect\" where net_bind=?", bind) > 0) {
			invalidateList.addTable(
				conn,
				Table.TableID.NET_TCP_REDIRECTS,
				business,
				host,
				false
			);
		}

		if(conn.update("delete from ftp.\"PrivateServer\" where net_bind=?", bind) > 0) {
			invalidateList.addTable(
				conn,
				Table.TableID.PRIVATE_FTP_SERVERS,
				business,
				host,
				false
			);
		}

		conn.update("delete from net.\"Bind\" where id=?", bind);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BINDS,
			business,
			host,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.NET_BIND_FIREWALLD_ZONES,
			business,
			host,
			false
		);
	}

	public static void setBindFirewalldZones(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int bind,
		Set<FirewallZone.Name> firewalldZones
	) throws IOException, SQLException {
		PackageHandler.checkAccessPackage(conn, source, "setBindFirewalldZones", getPackageForBind(conn, bind));

		boolean updated = false;
		int host = getHostForBind(conn, bind);
		if(firewalldZones.isEmpty()) {
			if(conn.update("delete from net.\"BindFirewallZone\" where net_bind=?", bind) != 0) {
				updated = true;
			}
		} else {
			// Find the set that exists
			Set<FirewallZone.Name> existing = conn.queryNewCollection(
				AoCollections::newHashSet,
				ObjectFactories.firewallZoneNameFactory,
				"select\n"
				+ "  fz.\"name\"\n"
				+ "from\n"
				+ "  net.\"BindFirewallZone\" nbfz\n"
				+ "  inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
				+ "where\n"
				+ "  nbfz.net_bind=?",
				bind
			);
			// Delete extra
			for(FirewallZone.Name name : existing) {
				if(!firewalldZones.contains(name)) {
					conn.update(
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
						bind,
						name
					);
					updated = true;
				}
			}
			// Add new
			for(FirewallZone.Name name : firewalldZones) {
				if(!existing.contains(name)) {
					conn.update(
						"insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
						+ "  ?,\n"
						+ "  (select id from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
						+ ")",
						bind,
						host,
						name
					);
					updated = true;
				}
			}
		}
		if(updated) {
			Account.Name business = getAccountForBind(conn, bind);
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BINDS,
				business,
				host,
				false
			);
			invalidateList.addTable(
				conn,
				Table.TableID.NET_BIND_FIREWALLD_ZONES,
				business,
				host,
				false
			);
		}
	}

	public static void setBindMonitoringEnabled(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int bind,
		boolean monitoringEnabled
	) throws IOException, SQLException {
		PackageHandler.checkAccessPackage(conn, source, "setBindMonitoringEnabled", getPackageForBind(conn, bind));

		conn.update("update net.\"Bind\" set monitoring_enabled=? where id=?", monitoringEnabled, bind);

		invalidateList.addTable(conn,
			Table.TableID.NET_BINDS,
			getAccountForBind(conn, bind),
			getHostForBind(conn, bind),
			false
		);
	}

	/**
	 * This exists for compatibility with older clients (versions &lt;= 1.80.2) only.
	 * This has been implemented by adding and removing the public zone from the net_bind.
	 */
	public static void setBindOpenFirewall(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int bind,
		boolean openFirewall
	) throws IOException, SQLException {
		AoservProtocol.Version clientVersion = source.getProtocolVersion();
		if(clientVersion.compareTo(AoservProtocol.Version.VERSION_1_80_2) > 0) {
			throw new IOException("This compatibility method only remains for clients version <= 1.80.2: Client is version " + clientVersion);
		}

		PackageHandler.checkAccessPackage(conn, source, "setBindOpenFirewall", getPackageForBind(conn, bind));

		int host = getHostForBind(conn, bind);
		if(openFirewall) {
			// Add the public zone if missing
			int fz = conn.queryInt("select id from net.\"FirewallZone\" where server=? and \"name\"=?", host, FirewallZone.PUBLIC);
			boolean updated;
			synchronized(netBindLock) {
				if(
					conn.queryBoolean("select (select id from net.\"BindFirewallZone\" where net_bind=? and firewalld_zone=?) is null", bind, fz)
				) {
					conn.update("insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (?,?)", bind, fz);
					updated = true;
				} else {
					updated = false;
				}
			}
			if(updated) {
				Account.Name business = getAccountForBind(conn, bind);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BINDS,
					business,
					host,
					false
				);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BIND_FIREWALLD_ZONES,
					business,
					host,
					false
				);
			}
		} else {
			// Remove the public zone if present
			if(
				conn.update(
					"delete from net.\"BindFirewallZone\" where net_bind=? and firewalld_zone=(select id from net.\"FirewallZone\" where server=? and \"name\"=?)",
					bind,
					host,
					FirewallZone.PUBLIC
				) != 0
			) {
				Account.Name business = getAccountForBind(conn, bind);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BINDS,
					business,
					host,
					false
				);
				invalidateList.addTable(
					conn,
					Table.TableID.NET_BIND_FIREWALLD_ZONES,
					business,
					host,
					false
				);
			}
		}
	}
}
