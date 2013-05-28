/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.DNSZone;
import com.aoindustries.aoserv.client.IPAddress;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.NetDeviceID;
import com.aoindustries.aoserv.client.NetProtocol;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.DomainName;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>IPAddressHandler</code> handles all the accesses to the <code>ip_addresses</code> table.
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

    public static boolean isDHCPAddress(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeBooleanQuery(
            "select is_dhcp from ip_addresses where pkey=?",
            pkey
        );
    }

    public static DomainName getUnassignedHostname(
        DatabaseConnection conn,
        int ipAddress
    ) throws IOException, SQLException {
        try {
            final String ip=getIPStringForIPAddress(conn, ipAddress);
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
        } catch(ValidationException e) {
            SQLException exc = new SQLException(e.getLocalizedMessage());
            exc.initCause(e);
            throw exc;
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

        // Update ip_addresses
        int netDevice=conn.executeIntQuery(
            "select pkey from net_devices where server=? and device_id='"+NetDeviceID.ETH0+"'",
            toServer
        );
        conn.executeUpdate(
            "update ip_addresses set net_device=? where pkey=?",
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
        String dhcpAddress
    ) throws IOException, SQLException {
        checkAccessIPAddress(conn, source, "setIPAddressDHCPAddress", ipAddress);
        if(!IPAddress.isValidIPAddress(dhcpAddress)) throw new SQLException("Invalid DHCP IP address: "+dhcpAddress);
        if(!isDHCPAddress(conn, ipAddress)) throw new SQLException("IPAddress is not DHCP-enabled: "+ipAddress);

        AccountingCode accounting=getBusinessForIPAddress(conn, ipAddress);
        int server=getServerForIPAddress(conn, ipAddress);

        // Update the table
        conn.executeUpdate("update ip_addresses set ip_address=? where pkey=?", dhcpAddress, ipAddress);

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
        String packageName=getPackageForIPAddress(conn, ipAddress);
        if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to set hostname for an IP address, package disabled: "+packageName);

        String ip=getIPStringForIPAddress(conn, ipAddress);
        if(
            ip.equals(IPAddress.LOOPBACK_IP)
            || ip.equals(IPAddress.WILDCARD_IP)
        ) throw new SQLException("Not allowed to set the hostname for "+ip);

        AccountingCode accounting=getBusinessForIPAddress(conn, ipAddress);
        int server=getServerForIPAddress(conn, ipAddress);

        // Update the table
        conn.executeUpdate("update ip_addresses set hostname=? where pkey=?", hostname.toString(), ipAddress);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.IP_ADDRESSES,
            accounting,
            server,
            false
        );

        // Update any reverse DNS matchins this IP address
        DNSHandler.updateReverseDnsIfExists(conn, invalidateList, ip, hostname);
    }

    /**
     * Sets the Package owner of an IPAddress.
     */
    public static void setIPAddressPackage(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipAddress,
        String newPackage
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
        String newPackage
    ) throws IOException, SQLException {
        AccountingCode oldAccounting=getBusinessForIPAddress(conn, ipAddress);
        AccountingCode newAccounting=PackageHandler.getBusinessForPackage(conn, newPackage);
        int server=getServerForIPAddress(conn, ipAddress);

        // Make sure that the IP Address is not in use
        int count=conn.executeIntQuery(
              "select\n"
            + "  count(*)\n"
            + "from\n"
            + "  net_binds\n"
            + "where\n"
            + "  ip_address=?",
            ipAddress
        );
        if(count!=0) throw new SQLException("Unable to set Package, IPAddress in use by "+count+(count==1?" row":" rows")+" in net_binds: "+ipAddress);

        // Update the table
        conn.executeUpdate("update ip_addresses set package=? where pkey=?", newPackage, ipAddress);
        conn.executeUpdate("update ip_addresses set available=false where pkey=?", ipAddress);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.IP_ADDRESSES,
            InvalidateList.getCollection(oldAccounting, newAccounting),
            server,
            false
        );
    }

    public static int getSharedHttpdIP(DatabaseConnection conn, int aoServer, boolean supportsModJK) throws IOException, SQLException {
        return conn.executeIntQuery(
            "select\n"
            + "  coalesce(\n"
            + "    (\n"
            + "      select\n"
            + "        ia.pkey\n"
            + "      from\n"
            + "        ip_addresses ia,\n"
            + "        net_devices nd\n"
            + "        left join net_binds nb on nd.server=nb.server and nb.port in (80, 443) and nb.net_protocol='"+NetProtocol.TCP+"'\n"
            + "        left join httpd_binds hb on nb.pkey=hb.net_bind\n"
            + "        left join httpd_servers hs on hb.httpd_server=hs.pkey\n"
            + "      where\n"
            + "        ia.is_overflow\n"
            + "        and ia.net_device=nd.pkey\n"
            + "        and nd.server=?\n"
            + "        and (\n"
            + "          nb.ip_address is null\n"
            + "          or ia.pkey=nb.ip_address\n"
            + "        ) and (\n"
            + "          hs.pkey is null\n"
            + "          or hs.is_mod_jk\n"
            + "          or hs.is_mod_jk=?\n"
            + "        ) and (\n"
            + "          hb.net_bind is null\n"
            + "          or (\n"
            + "            select\n"
            + "              count(*)\n"
            + "            from\n"
            + "              httpd_site_binds hsb\n"
            + "            where\n"
            + "              hsb.httpd_bind=hb.net_bind\n"
            + "          )<(hs.max_binds-1)\n"
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
            + "            and nb2.ip_address=ia.pkey\n"
            + "            and (\n"
            + "              nb2.port=80\n"
            + "              or nb2.port=443\n"
            + "            ) and nb2.pkey=hsb2.httpd_bind\n"
            + "        )\n"
            + "      limit 1\n"
            + "    ), -1\n"
            + "  )",
            aoServer,
            supportsModJK,
            aoServer
        );
    }

    public static String getPackageForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        return conn.executeStringQuery("select package from ip_addresses where pkey=?", ipAddress);
    }

    public static AccountingCode getBusinessForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select pk.accounting from ip_addresses ia, packages pk where ia.pkey=? and ia.package=pk.name",
            ipAddress
        );
    }

    public static int getServerForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        return conn.executeIntQuery("select nd.server from ip_addresses ia, net_devices nd where ia.pkey=? and ia.net_device=nd.pkey", ipAddress);
    }

    public static String getIPStringForIPAddress(DatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        return conn.executeStringQuery("select ip_address from ip_addresses where pkey=?", ipAddress);
    }

    public static int getWildcardIPAddress(DatabaseConnection conn) throws IOException, SQLException {
        return conn.executeIntQuery("select pkey from ip_addresses where ip_address=? limit 1", IPAddress.WILDCARD_IP);
    }

    public static int getLoopbackIPAddress(DatabaseConnection conn, int server) throws IOException, SQLException {
        return conn.executeIntQuery(
            "select\n"
            + "  ia.pkey\n"
            + "from\n"
            + "  ip_addresses ia,\n"
            + "  net_devices nd\n"
            + "where\n"
            + "  ia.ip_address=?\n"
            + "  and ia.net_device=nd.pkey\n"
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
            "update ip_addresses set available=true where pkey=?",
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
}