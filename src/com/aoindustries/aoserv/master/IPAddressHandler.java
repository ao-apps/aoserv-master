/*
 * Copyright 2001-2013, 2014, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.dns.Zone;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.DeviceId;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.lang.NotImplementedException;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.InetAddress;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;

/**
 * The <code>IPAddressHandler</code> handles all the accesses to the <code>net.IpAddress</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class IPAddressHandler {

	public static void checkAccessIPAddress(DatabaseConnection conn, RequestSource source, String action, int ipAddressId) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getServerForIPAddress(conn, ipAddressId));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForIPAddress(conn, ipAddressId));
		}
	}

	public static boolean isDHCPAddress(DatabaseConnection conn, int ipAddressId) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select \"isDhcp\" from net.\"IpAddress\" where id=?",
			ipAddressId
		);
	}

	public static DomainName getUnassignedHostname(
		DatabaseConnection conn,
		int ipAddressId
	) throws IOException, SQLException {
		try {
			final InetAddress inetAddress = getInetAddressForIPAddress(conn, ipAddressId);
			switch(inetAddress.getAddressFamily()) {
				case INET : {
					String ip = inetAddress.toString();
					int pos=ip.lastIndexOf('.');
					final String octet=ip.substring(pos+1);
					int server=getServerForIPAddress(conn, ipAddressId);
					final String net;
					if(ip.startsWith("66.160.183.")) net = "net1.";
					else if(ip.startsWith("64.62.174.")) net = "net2.";
					else if(ip.startsWith("64.71.144.")) net = "net3.";
					else net = "";
					final String farm=ServerHandler.getFarmForServer(conn, server);
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

	public static void moveIPAddress(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddressId,
		int toServerId
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "moveIPAddress", ipAddressId);
		ServerHandler.checkAccessServer(conn, source, "moveIPAddress", toServerId);
		int fromServerId=getServerForIPAddress(conn, ipAddressId);
		ServerHandler.checkAccessServer(conn, source, "moveIPAddress", fromServerId);

		AccountingCode accounting=getBusinessForIPAddress(conn, ipAddressId);

		// Update net.IpAddress
		int netDeviceId = conn.executeIntQuery(
			"select id from net.\"Device\" where server=? and \"deviceId\"=?",
			toServerId,
			DeviceId.ETH0
		);
		conn.executeUpdate(
			"update net.\"IpAddress\" set device=? where id=?",
			netDeviceId,
			ipAddressId
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			accounting,
			fromServerId,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			accounting,
			toServerId,
			false
		);
	}

	/**
	 * Sets the IP address for a DHCP-enabled IP address.
	 */
	public static void setIPAddressDHCPAddress(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddressId,
		InetAddress dhcpAddress
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressDHCPAddress", ipAddressId);
		if(!isDHCPAddress(conn, ipAddressId)) throw new SQLException("net.IpAddress is not DHCP-enabled: "+ipAddressId);

		AccountingCode accounting=getBusinessForIPAddress(conn, ipAddressId);
		int server=getServerForIPAddress(conn, ipAddressId);

		// Update the table
		conn.executeUpdate("update net.\"IpAddress\" set \"inetAddress\"=? where id=?", dhcpAddress, ipAddressId);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			accounting,
			server,
			false
		);

		// Update any DNS records that follow this IP address
		DNSHandler.updateDhcpDnsRecords(conn, invalidateList, ipAddressId, dhcpAddress);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIPAddressHostname(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddressId,
		DomainName hostname
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressHostname", ipAddressId);
		MasterServer.checkAccessHostname(conn, source, "setIPAddressHostname", hostname.toString());

		setIPAddressHostname(conn, invalidateList, ipAddressId, hostname);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIPAddressHostname(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddressId,
		DomainName hostname
	) throws IOException, SQLException {
		// Can't set the hostname on a disabled package
		//String packageName=getPackageForIPAddress(conn, ipAddress);
		//if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to set hostname for an IP address, package disabled: "+packageName);

		InetAddress ip = getInetAddressForIPAddress(conn, ipAddressId);
		if(
			ip.isLoopback()
			|| ip.isUnspecified()
		) throw new SQLException("Not allowed to set the hostname for "+ip);

		// Update the table
		conn.executeUpdate("update net.\"IpAddress\" set hostname=? where id=?", hostname.toString(), ipAddressId);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddressId),
			getServerForIPAddress(conn, ipAddressId),
			false
		);

		// Update any reverse DNS matchins this IP address
		DNSHandler.updateReverseDnsIfExists(conn, invalidateList, ip, hostname);
	}

	public static void setIPAddressMonitoringEnabled(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddressId,
		boolean enabled
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressMonitoringEnabled", ipAddressId);

		setIPAddressMonitoringEnabled(conn, invalidateList, ipAddressId, enabled);
	}

	public static void setIPAddressMonitoringEnabled(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddressId,
		boolean enabled
	) throws IOException, SQLException {
		// Update the table
		// TODO: Add row when first enabled or column set to non-default
		// TODO: Remove row when disabled and other columns match defaults
		conn.executeUpdate("update \"net.monitoring\".\"IpAddressMonitoring\" set enabled=? where id=?", enabled, ipAddressId);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddressId),
			getServerForIPAddress(conn, ipAddressId),
			false
		);
	}

	/**
	 * Sets the Package owner of an net.IpAddress.
	 */
	public static void setIPAddressPackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddressId,
		AccountingCode newPackage
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressPackage", ipAddressId);
		PackageHandler.checkAccessPackage(conn, source, "setIPAddressPackage", newPackage);

		setIPAddressPackage(conn, invalidateList, ipAddressId, newPackage);
	}

	/**
	 * Sets the Package owner of an net.IpAddress.
	 */
	public static void setIPAddressPackage(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddressId,
		AccountingCode newPackage
	) throws IOException, SQLException {
		AccountingCode oldAccounting = getBusinessForIPAddress(conn, ipAddressId);
		AccountingCode newAccounting = PackageHandler.getBusinessForPackage(conn, newPackage);
		int server=getServerForIPAddress(conn, ipAddressId);

		// Make sure that the IP Address is not in use
		int count=conn.executeIntQuery(
			  "select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  net.\"Bind\"\n"
			+ "where\n"
			+ "  \"ipAddress\"=?",
			ipAddressId
		);
		if(count!=0) throw new SQLException("Unable to set Package, net.IpAddress in use by "+count+(count==1?" row":" rows")+" in net.Bind: "+ipAddressId);

		// Update the table
		conn.executeUpdate("update net.\"IpAddress\" set package=(select id from billing.\"Package\" where name=?), \"isAvailable\"=false where id=?", newPackage, ipAddressId);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			InvalidateList.getCollection(oldAccounting, newAccounting),
			server,
			false
		);
	}

	public static int getSharedHttpdIP(DatabaseConnection conn, int serverId) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        ia.id\n"
			+ "      from\n"
			+ "        net.\"IpAddress\" ia,\n"
			+ "        net.\"Device\" nd\n"
			+ "        left join net.\"Bind\" nb on nd.server=nb.server and nb.port in (80, 443) and nb.net_protocol=?\n"
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
			com.aoindustries.net.Protocol.TCP.name().toLowerCase(Locale.ROOT),
			serverId,
			serverId
		);
	}

	public static AccountingCode getPackageForIPAddress(DatabaseConnection conn, int ipAddressId) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.name\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.id\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddressId
		);
	}

	public static AccountingCode getBusinessForIPAddress(DatabaseConnection conn, int ipAddressId) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.id\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddressId
		);
	}

	public static int getServerForIPAddress(DatabaseConnection conn, int ipAddressId) throws IOException, SQLException {
		return conn.executeIntQuery("select nd.server from net.\"IpAddress\" ia, net.\"Device\" nd where ia.id=? and ia.device=nd.id", ipAddressId);
	}

	public static InetAddress getInetAddressForIPAddress(DatabaseConnection conn, int ipAddressId) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.inetAddressFactory,
			"select host(\"inetAddress\") from net.\"IpAddress\" where id=?",
			ipAddressId
		);
	}

	public static int getWildcardIPAddress(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntQuery("select id from net.\"IpAddress\" where \"inetAddress\"=?", IpAddress.WILDCARD_IP); // No limit, must always be 1 row and error otherwise
	}

	public static int getLoopbackIPAddress(DatabaseConnection conn, int serverId) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  ia.id\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia,\n"
			+ "  net.\"Device\" nd\n"
			+ "where\n"
			+ "  ia.\"inetAddress\"=?\n"
			+ "  and ia.device=nd.id\n"
			+ "  and nd.server=?\n"
			+ "limit 1",
			IpAddress.LOOPBACK_IP,
			serverId
		);
	}

	public static void releaseIPAddress(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddressId
	) throws IOException, SQLException {
		setIPAddressHostname(
			conn,
			invalidateList,
			ipAddressId,
			getUnassignedHostname(conn, ipAddressId)
		);

		conn.executeUpdate(
			"update net.\"IpAddress\" set \"isAvailable\"=true, \"isOverflow\"=false where id=?",
			ipAddressId
		);
		conn.executeUpdate(
			"update \"net.monitoring\".\"IpAddressMonitoring\" set enabled=true, \"pingMonitorEnabled\"=false, \"checkBlacklistsOverSmtp\"=false, \"verifyDnsPtr\"=true, \"verifyDnsA\"=false where id=?",
			ipAddressId
		);
		invalidateList.addTable(
			conn,
			Table.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddressId),
			getServerForIPAddress(conn, ipAddressId),
			false
		);
	}

	private IPAddressHandler() {
	}
}
