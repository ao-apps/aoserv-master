/*
 * Copyright 2001-2013, 2014, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.DNSZone;
import com.aoindustries.aoserv.client.IPAddress;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.NetDeviceID;
import com.aoindustries.aoserv.client.SchemaTable;
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
 * The <code>IPAddressHandler</code> handles all the accesses to the <code>IPAddress</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class IPAddressHandler {

	public static void checkAccessIPAddress(DatabaseConnection conn, RequestSource source, String action, int ipAddress) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getServerForIPAddress(conn, ipAddress));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForIPAddress(conn, ipAddress));
		}
	}

	public static boolean isDHCPAddress(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select \"isDhcp\" from \"IPAddress\" where id=?",
			id
		);
	}

	public static DomainName getUnassignedHostname(
		DatabaseConnection conn,
		int ipAddress
	) throws IOException, SQLException {
		try {
			final InetAddress inetAddress = getInetAddressForIPAddress(conn, ipAddress);
			switch(inetAddress.getAddressFamily()) {
				case INET : {
					String ip = inetAddress.toString();
					int pos=ip.lastIndexOf('.');
					final String octet=ip.substring(pos+1);
					int server=getServerForIPAddress(conn, ipAddress);
					final String net;
					if(ip.startsWith("66.160.183.")) net = "net1.";
					else if(ip.startsWith("64.62.174.")) net = "net2.";
					else if(ip.startsWith("64.71.144.")) net = "net3.";
					else net = "";
					final String farm=ServerHandler.getFarmForServer(conn, server);
					String hostname="unassigned"+octet+"."+net+farm+'.'+DNSZone.API_ZONE;
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
		int ipAddress,
		int toServer
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "moveIPAddress", ipAddress);
		ServerHandler.checkAccessServer(conn, source, "moveIPAddress", toServer);
		int fromServer=getServerForIPAddress(conn, ipAddress);
		ServerHandler.checkAccessServer(conn, source, "moveIPAddress", fromServer);

		AccountingCode accounting=getBusinessForIPAddress(conn, ipAddress);

		// Update IPAddress
		int netDevice=conn.executeIntQuery(
			"select pkey from net_devices where server=? and \"deviceID\"=?",
			toServer,
			NetDeviceID.ETH0
		);
		conn.executeUpdate(
			"update \"IPAddress\" set \"netDevice\"=? where id=?",
			netDevice,
			ipAddress
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			accounting,
			fromServer,
			false
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			accounting,
			toServer,
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
		int ipAddress,
		InetAddress dhcpAddress
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressDHCPAddress", ipAddress);
		if(!isDHCPAddress(conn, ipAddress)) throw new SQLException("IPAddress is not DHCP-enabled: "+ipAddress);

		AccountingCode accounting=getBusinessForIPAddress(conn, ipAddress);
		int server=getServerForIPAddress(conn, ipAddress);

		// Update the table
		conn.executeUpdate("update \"IPAddress\" set \"inetAddress\"=? where id=?", dhcpAddress, ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			accounting,
			server,
			false
		);

		// Update any DNS records that follow this IP address
		DNSHandler.updateDhcpDnsRecords(conn, invalidateList, ipAddress, dhcpAddress);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIPAddressHostname(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		DomainName hostname
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressHostname", ipAddress);
		MasterServer.checkAccessHostname(conn, source, "setIPAddressHostname", hostname.toString());

		setIPAddressHostname(conn, invalidateList, ipAddress, hostname);
	}

	/**
	 * Sets the hostname for an IP address.
	 */
	public static void setIPAddressHostname(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		DomainName hostname
	) throws IOException, SQLException {
		// Can't set the hostname on a disabled package
		//String packageName=getPackageForIPAddress(conn, ipAddress);
		//if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to set hostname for an IP address, package disabled: "+packageName);

		InetAddress ip = getInetAddressForIPAddress(conn, ipAddress);
		if(
			ip.isLoopback()
			|| ip.isUnspecified()
		) throw new SQLException("Not allowed to set the hostname for "+ip);

		// Update the table
		conn.executeUpdate("update \"IPAddress\" set hostname=? where id=?", hostname.toString(), ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddress),
			getServerForIPAddress(conn, ipAddress),
			false
		);

		// Update any reverse DNS matchins this IP address
		DNSHandler.updateReverseDnsIfExists(conn, invalidateList, ip, hostname);
	}

	public static void setIPAddressMonitoringEnabled(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		boolean enabled
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressMonitoringEnabled", ipAddress);

		setIPAddressMonitoringEnabled(conn, invalidateList, ipAddress, enabled);
	}

	public static void setIPAddressMonitoringEnabled(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		boolean enabled
	) throws IOException, SQLException {
		// Update the table
		// TODO: Add row when first enabled or column set to non-default
		// TODO: Remove row when disabled and other columns match defaults
		conn.executeUpdate("update monitoring.\"IPAddressMonitoring\" set enabled=? where id=?", enabled, ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddress),
			getServerForIPAddress(conn, ipAddress),
			false
		);
	}

	/**
	 * Sets the Package owner of an IPAddress.
	 */
	public static void setIPAddressPackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ipAddress,
		AccountingCode newPackage
	) throws IOException, SQLException {
		checkAccessIPAddress(conn, source, "setIPAddressPackage", ipAddress);
		PackageHandler.checkAccessPackage(conn, source, "setIPAddressPackage", newPackage);

		setIPAddressPackage(conn, invalidateList, ipAddress, newPackage);
	}

	/**
	 * Sets the Package owner of an IPAddress.
	 */
	public static void setIPAddressPackage(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress,
		AccountingCode newPackage
	) throws IOException, SQLException {
		AccountingCode oldAccounting = getBusinessForIPAddress(conn, ipAddress);
		AccountingCode newAccounting = PackageHandler.getBusinessForPackage(conn, newPackage);
		int server=getServerForIPAddress(conn, ipAddress);

		// Make sure that the IP Address is not in use
		int count=conn.executeIntQuery(
			  "select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  net_binds\n"
			+ "where\n"
			+ "  \"ipAddress\"=?",
			ipAddress
		);
		if(count!=0) throw new SQLException("Unable to set Package, IPAddress in use by "+count+(count==1?" row":" rows")+" in net_binds: "+ipAddress);

		// Update the table
		conn.executeUpdate("update \"IPAddress\" set package=(select pkey from billing.\"Package\" where name=?), available=false where id=?", newPackage, ipAddress);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			InvalidateList.getCollection(oldAccounting, newAccounting),
			server,
			false
		);
	}

	public static int getSharedHttpdIP(DatabaseConnection conn, int aoServer) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        ia.id\n"
			+ "      from\n"
			+ "        \"IPAddress\" ia,\n"
			+ "        net_devices nd\n"
			+ "        left join net_binds nb on nd.server=nb.server and nb.port in (80, 443) and nb.net_protocol=?\n"
			+ "        left join httpd_binds hb on nb.pkey=hb.net_bind\n"
			+ "        left join httpd_servers hs on hb.httpd_server=hs.pkey\n"
			+ "      where\n"
			+ "        ia.\"isOverflow\"\n"
			+ "        and ia.\"netDevice\"=nd.pkey\n"
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
			+ "            net_binds nb2,\n"
			+ "            httpd_site_binds hsb2\n"
			+ "          where\n"
			+ "            nb2.server=?\n"
			+ "            and nb2.\"ipAddress\"=ia.id\n"
			+ "            and (\n"
			+ "              nb2.port=80\n"
			+ "              or nb2.port=443\n"
			+ "            ) and nb2.pkey=hsb2.httpd_bind\n"
			+ "        )\n"
			+ "      limit 1\n"
			+ "    ), -1\n"
			+ "  )",
			com.aoindustries.net.Protocol.TCP.name().toLowerCase(Locale.ROOT),
			aoServer,
			aoServer
		);
	}

	public static AccountingCode getPackageForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.name\n"
			+ "from\n"
			+ "  \"IPAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.pkey\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddress
		);
	}

	public static AccountingCode getBusinessForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  \"IPAddress\" ia\n"
			+ "  inner join billing.\"Package\" pk on ia.package=pk.pkey\n"
			+ "where\n"
			+ "  ia.id=?",
			ipAddress
		);
	}

	public static int getServerForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.executeIntQuery("select nd.server from \"IPAddress\" ia, net_devices nd where ia.id=? and ia.\"netDevice\"=nd.pkey", ipAddress);
	}

	public static InetAddress getInetAddressForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.inetAddressFactory,
			"select host(\"inetAddress\") from \"IPAddress\" where id=?",
			ipAddress
		);
	}

	public static int getWildcardIPAddress(DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeIntQuery("select id from \"IPAddress\" where \"inetAddress\"=?", IPAddress.WILDCARD_IP); // No limit, must always be 1 row and error otherwise
	}

	public static int getLoopbackIPAddress(DatabaseConnection conn, int server) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  ia.id\n"
			+ "from\n"
			+ "  \"IPAddress\" ia,\n"
			+ "  net_devices nd\n"
			+ "where\n"
			+ "  ia.\"inetAddress\"=?\n"
			+ "  and ia.\"netDevice\"=nd.pkey\n"
			+ "  and nd.server=?\n"
			+ "limit 1",
			IPAddress.LOOPBACK_IP,
			server
		);
	}

	public static void releaseIPAddress(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ipAddress
	) throws IOException, SQLException {
		setIPAddressHostname(
			conn,
			invalidateList,
			ipAddress,
			getUnassignedHostname(conn, ipAddress)
		);

		conn.executeUpdate(
			"update \"IPAddress\" set available=true, \"isOverflow\"=false where id=?",
			ipAddress
		);
		conn.executeUpdate(
			"update monitoring.\"IPAddressMonitoring\" set enabled=true, \"pingMonitorEnabled\"=false, \"checkBlacklistsOverSmtp\"=false, \"verifyDnsPtr\"=true, \"verifyDnsA\"=false where id=?",
			ipAddress
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.IP_ADDRESSES,
			getBusinessForIPAddress(conn, ipAddress),
			getServerForIPAddress(conn, ipAddress),
			false
		);
	}

	private IPAddressHandler() {
	}
}
