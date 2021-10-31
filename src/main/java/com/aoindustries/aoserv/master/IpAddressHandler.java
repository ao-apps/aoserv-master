/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2014, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoapps.net.InetAddress;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.dns.Zone;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.DeviceId;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.commons.lang3.NotImplementedException;

/**
 * The <code>IPAddressHandler</code> handles all the accesses to the <code>net.IpAddress</code> table.
 *
 * @author  AO Industries, Inc.
 */
public final class IpAddressHandler {

	public static void checkAccessIpAddress(DatabaseConnection conn, RequestSource source, String action, int ipAddressId) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				NetHostHandler.checkAccessHost(conn, source, action, getHostForIpAddress(conn, ipAddressId));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForIpAddress(conn, ipAddressId));
		}
	}

	public static boolean isDhcpAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.queryBoolean(
			"select \"isDhcp\" from net.\"IpAddress\" where id=?",
			ipAddress
		);
	}

	@SuppressWarnings("deprecation")
	public static DomainName getUnassignedHostname(
		DatabaseConnection conn,
		int ipAddress
	) throws IOException, SQLException {
		try {
			final InetAddress inetAddress = getInetAddressForIpAddress(conn, ipAddress);
			switch(inetAddress.getAddressFamily()) {
				case INET : {
					String ip = inetAddress.toString();
					int pos=ip.lastIndexOf('.');
					final String octet=ip.substring(pos+1);
					int host = getHostForIpAddress(conn, ipAddress);
					final String net;
					if(ip.startsWith("66.160.183.")) net = "net1.";
					else if(ip.startsWith("64.62.174.")) net = "net2.";
					else if(ip.startsWith("64.71.144.")) net = "net3.";
					else net = "";
					final String farm=NetHostHandler.getFarmForHost(conn, host);
					String hostname="unassigned"+octet+"."+net+farm+'.'+Zone.API_ZONE;
					while(hostname.endsWith(".")) hostname = hostname.substring(0, hostname.length()-1);
					return DomainName.valueOf(hostname);
				}
				case INET6 : {
					throw new NotImplementedException();
				}
				default :
					throw new AssertionError();
			}
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	}

	public static void moveIpAddress(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		int toHost
	) throws IOException, SQLException {
		checkAccessIpAddress(conn, source, "moveIpAddress", ipAddress);
		NetHostHandler.checkAccessHost(conn, source, "moveIpAddress", toHost);
		int fromHost = getHostForIpAddress(conn, ipAddress);
		NetHostHandler.checkAccessHost(conn, source, "moveIpAddress", fromHost);

		Account.Name account = getAccountForIpAddress(conn, ipAddress);

		// Update net.IpAddress
		int toDevice = conn.queryInt(
			"select id from net.\"Device\" where server=? and \"deviceId\"=?",
			toHost,
			DeviceId.ETH0
		);
		conn.update(
			"update net.\"IpAddress\" set device=? where id=?",
			toDevice,
			ipAddress
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			account,
			fromHost,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			account,
			toHost,
			false
		);
	}

	/**
	 * Sets the IP address for a DHCP-enabled IP address.
	 */
	public static void setDhcpAddressDestination(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int dhcpAddress,
		InetAddress inetAddress
	) throws IOException, SQLException {
		checkAccessIpAddress(conn, source, "setIPAddressDHCPAddress", dhcpAddress);
		if(!isDhcpAddress(conn, dhcpAddress)) throw new SQLException("net.IpAddress is not DHCP-enabled: " + dhcpAddress);

		Account.Name account = getAccountForIpAddress(conn, dhcpAddress);
		int host = getHostForIpAddress(conn, dhcpAddress);

		// Update the table
		conn.update("update net.\"IpAddress\" set \"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\" where id=?", inetAddress, dhcpAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			account,
			host,
			false
		);

		// Update any DNS records that follow this IP address
		MasterServer.getService(DnsService.class).updateDhcpDnsRecords(conn, invalidateList, dhcpAddress, inetAddress);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIpAddressHostname(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		DomainName hostname
	) throws IOException, SQLException {
		checkAccessIpAddress(conn, source, "setIPAddressHostname", ipAddress);
		MasterServer.checkAccessHostname(conn, source, "setIPAddressHostname", hostname.toString());

		setIpAddressHostname(conn, invalidateList, ipAddress, hostname);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIpAddressHostname(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		DomainName hostname
	) throws IOException, SQLException {
		// Can't set the hostname on a disabled package
		//String packageName=getPackageForIPAddress(conn, ipAddress);
		//if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to set hostname for an IP address, package disabled: "+packageName);

		InetAddress ip = getInetAddressForIpAddress(conn, ipAddress);
		if(
			ip.isLoopback()
			|| ip.isUnspecified()
		) throw new SQLException("Not allowed to set the hostname for "+ip);

		// Update the table
		conn.update("update net.\"IpAddress\" set hostname=? where id=?", hostname.toString(), ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			getAccountForIpAddress(conn, ipAddress),
			getHostForIpAddress(conn, ipAddress),
			false
		);

		// Update any reverse DNS matchins this IP address
		MasterServer.getService(DnsService.class).updateReverseDnsIfExists(conn, invalidateList, ip, hostname);
	}

	public static void setIpAddressMonitoringEnabled(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		boolean monitoringEnabled
	) throws IOException, SQLException {
		checkAccessIpAddress(conn, source, "setIPAddressMonitoringEnabled", ipAddress);

		setIpAddressMonitoringEnabled(conn, invalidateList, ipAddress, monitoringEnabled);
	}

	public static void setIpAddressMonitoringEnabled(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		boolean monitoringEnabled
	) throws IOException, SQLException {
		// Update the table
		// TODO: Add row when first enabled or column set to non-default
		// TODO: Remove row when disabled and other columns match defaults
		conn.update("update \"net.monitoring\".\"IpAddressMonitoring\" set enabled=? where id=?", monitoringEnabled, ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			getAccountForIpAddress(conn, ipAddress),
			getHostForIpAddress(conn, ipAddress),
			false
		);
	}

	/**
	 * Sets the Package owner of an net.IpAddress.
	 */
	public static void setIpAddressPackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		Account.Name newPackage
	) throws IOException, SQLException {
		checkAccessIpAddress(conn, source, "setIPAddressPackage", ipAddress);
		PackageHandler.checkAccessPackage(conn, source, "setIPAddressPackage", newPackage);

		setIpAddressPackage(conn, invalidateList, ipAddress, newPackage);
	}

	/**
	 * Sets the Package owner of an net.IpAddress.
	 */
	public static void setIpAddressPackage(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		Account.Name newPackage
	) throws IOException, SQLException {
		Account.Name oldAccounting = getAccountForIpAddress(conn, ipAddress);
		Account.Name newAccounting = PackageHandler.getAccountForPackage(conn, newPackage);
		int host = getHostForIpAddress(conn, ipAddress);

		// Make sure that the IP Address is not in use
		int count=conn.queryInt(
			  "select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  net.\"Bind\"\n"
			+ "where\n"
			+ "  \"ipAddress\"=?",
			ipAddress
		);
		if(count!=0) throw new SQLException("Unable to set Package, net.IpAddress in use by "+count+(count==1?" row":" rows")+" in net.Bind: "+ipAddress);

		// Update the table
		conn.update("update net.\"IpAddress\" set package=(select id from billing.\"Package\" where name=?), \"isAvailable\"=false where id=?", newPackage, ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			InvalidateList.getAccountCollection(oldAccounting, newAccounting),
			host,
			false
		);
	}

	public static int getSharedHttpdIpAddress(DatabaseConnection conn, int linuxServer) throws IOException, SQLException {
		return conn.queryInt(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        ia.id\n"
			+ "      from\n"
			+ "        net.\"IpAddress\" ia,\n"
			+ "        net.\"Device\" nd\n"
			+ "        left join net.\"Bind\" nb on nd.server=nb.server and nb.port in (80, 443) and nb.net_protocol=?::\"com.aoapps.net\".\"Protocol\"\n"
			+ "        left join web.\"HttpdBind\" hb on nb.id=hb.net_bind\n"
			+ "        left join web.\"HttpdServer\" hs on hb.httpd_server=hs.id\n"
			+ "      where\n"
			+ "        ia.\"isOverflow\"\n"
			+ "        and ia.device=nd.id\n"
			+ "        and nd.server=?\n"
			+ "        and (\n"
			+ "          nb.\"ipAddress\" is null\n"
			+ "          or ia.id=nb.\"ipAddress\"\n"
			+ "        )\n"
			+ "      order by\n"
			+ "        (\n"
			+ "          select\n"
			+ "            count(*)\n"
			+ "          from\n"
			+ "            net.\"Bind\" nb2,\n"
			+ "            web.\"VirtualHost\" hsb2\n"
			+ "          where\n"
			+ "            nb2.server=?\n"
			+ "            and nb2.\"ipAddress\"=ia.id\n"
			+ "            and (\n"
			+ "              nb2.port=80\n"
			+ "              or nb2.port=443\n"
			+ "            ) and nb2.id=hsb2.httpd_bind\n"
			+ "        )\n"
			+ "      limit 1\n"
			+ "    ), -1\n"
			+ "  )",
			com.aoapps.net.Protocol.TCP.name(),
			linuxServer,
			linuxServer
		);
	}

	public static Account.Name getPackageForIpAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.name\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.id\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddress
		);
	}

	public static Account.Name getAccountForIpAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.id\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddress
		);
	}

	public static int getHostForIpAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.queryInt("select nd.server from net.\"IpAddress\" ia, net.\"Device\" nd where ia.id=? and ia.device=nd.id", ipAddress);
	}

	public static InetAddress getInetAddressForIpAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.inetAddressFactory,
			"select host(\"inetAddress\") from net.\"IpAddress\" where id=?",
			ipAddress
		);
	}

	public static int getWildcardIpAddress(DatabaseConnection conn) throws IOException, SQLException {
		return conn.queryInt("select id from net.\"IpAddress\" where \"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\"", IpAddress.WILDCARD_IP); // No limit, must always be 1 row and error otherwise
	}

	public static int getLoopbackIpAddress(DatabaseConnection conn, int host) throws IOException, SQLException {
		return conn.queryInt(
			"select\n"
			+ "  ia.id\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia,\n"
			+ "  net.\"Device\" nd\n"
			+ "where\n"
			+ "  ia.\"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\"\n"
			+ "  and ia.device=nd.id\n"
			+ "  and nd.server=?\n"
			+ "limit 1",
			IpAddress.LOOPBACK_IP,
			host
		);
	}

	public static void releaseIpAddress(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress
	) throws IOException, SQLException {
		setIpAddressHostname(
			conn,
			invalidateList,
			ipAddress,
			getUnassignedHostname(conn, ipAddress)
		);

		conn.update(
			"update net.\"IpAddress\" set \"isAvailable\"=true, \"isOverflow\"=false where id=?",
			ipAddress
		);
		conn.update(
			"update \"net.monitoring\".\"IpAddressMonitoring\" set enabled=true, \"pingMonitorEnabled\"=false, \"checkBlacklistsOverSmtp\"=false, \"verifyDnsPtr\"=true, \"verifyDnsA\"=false where id=?",
			ipAddress
		);
		invalidateList.addTable(conn,
			Table.TableID.IP_ADDRESSES,
			getAccountForIpAddress(conn, ipAddress),
			getHostForIpAddress(conn, ipAddress),
			false
		);
	}

	private IpAddressHandler() {
	}
}
