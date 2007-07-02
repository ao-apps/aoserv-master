package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>IPAddressHandler</code> handles all the accesses to the <code>ip_addresses</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class IPAddressHandler {

    public static void checkAccessIPAddress(MasterDatabaseConnection conn, RequestSource source, String action, int ipAddress) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "checkAccessIPAddress(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForIPAddress(conn, ipAddress));
                }
            } else {
                PackageHandler.checkAccessPackage(conn, source, action, getPackageForIPAddress(conn, ipAddress));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isDHCPAddress(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "isDHCPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeBooleanQuery(
                "select is_dhcp from ip_addresses where pkey=?",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getUnassignedHostname(
        MasterDatabaseConnection conn,
        int ipAddress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, IPAddressHandler.class, "getUnassignedHostname(MasterDatabaseConnection,int)", null);
        try {
            String ip=getIPStringForIPAddress(conn, ipAddress);
            int pos=ip.lastIndexOf('.');
            String octet=ip.substring(pos+1);
            int aoServer=getAOServerForIPAddress(conn, ipAddress);
            String farm=ServerHandler.getFarmForServer(conn, aoServer);
            String hostname="unassigned"+octet+'.'+farm+'.'+DNSZone.API_ZONE;
            hostname=hostname.substring(0, hostname.length()-1);
            return hostname;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void moveIPAddress(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipAddress,
        int toServer
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "moveIPAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            checkAccessIPAddress(conn, source, "moveIPAddress", ipAddress);
            ServerHandler.checkAccessServer(conn, source, "moveIPAddress", toServer);
            int fromServer=getAOServerForIPAddress(conn, ipAddress);
            ServerHandler.checkAccessServer(conn, source, "moveIPAddress", fromServer);

            String accounting=getBusinessForIPAddress(conn, ipAddress);

            // Update ip_addresses
            int netDevice=conn.executeIntQuery(
                "select pkey from net_devices where ao_server=? and device_id='"+NetDeviceID.ETH0+"'",
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
                ServerHandler.getHostnameForServer(conn, fromServer),
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.IP_ADDRESSES,
                accounting,
                ServerHandler.getHostnameForServer(conn, toServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the IP address for a DHCP-enabled IP address.
     */
    public static void setIPAddressDHCPAddress(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipAddress,
        String dhcpAddress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "setIPAddressDHCPAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessIPAddress(conn, source, "setIPAddressDHCPAddress", ipAddress);
            if(!IPAddress.isValidIPAddress(dhcpAddress)) throw new SQLException("Invalid DHCP IP address: "+dhcpAddress);
            if(!isDHCPAddress(conn, ipAddress)) throw new SQLException("IPAddress is not DHCP-enabled: "+ipAddress);

            String accounting=getBusinessForIPAddress(conn, ipAddress);
            int aoServer=getAOServerForIPAddress(conn, ipAddress);

            // Update the table
            conn.executeUpdate("update ip_addresses set ip_address=? where pkey=?", dhcpAddress, ipAddress);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.IP_ADDRESSES,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );

            // Update any DNS records that follow this IP address
            DNSHandler.updateDhcpDnsRecords(conn, invalidateList, ipAddress, dhcpAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the hostname for an IP address.
     */
    public static void setIPAddressHostname(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipAddress,
        String hostname
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "setIPAddressHostname(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessIPAddress(conn, source, "setIPAddressHostname", ipAddress);
            MasterServer.checkAccessHostname(conn, source, "setIPAddressHostname", hostname);

            setIPAddressHostname(conn, invalidateList, ipAddress, hostname);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the hostname for an IP address.
     */
    public static void setIPAddressHostname(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int ipAddress,
        String hostname
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "setIPAddressHostname(MasterDatabaseConnection,InvalidateList,int,String)", null);
        try {
            if(!EmailDomain.isValidFormat(hostname)) throw new SQLException("Invalid hostname: "+hostname);

            // Can't set the hostname on a disabled package
            String packageName=getPackageForIPAddress(conn, ipAddress);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to set hostname for an IP address, package disabled: "+packageName);

            String ip=getIPStringForIPAddress(conn, ipAddress);
            if(
                ip.equals(IPAddress.LOOPBACK_IP)
                || ip.equals(IPAddress.WILDCARD_IP)
            ) throw new SQLException("Not allowed to set the hostname for "+ip);

            String accounting=getBusinessForIPAddress(conn, ipAddress);
            int aoServer=getAOServerForIPAddress(conn, ipAddress);

            // Update the table
            conn.executeUpdate("update ip_addresses set hostname=? where pkey=?", hostname, ipAddress);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.IP_ADDRESSES,
                accounting,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            
            // Update any reverse DNS matchins this IP address
            DNSHandler.updateReverseDnsIfExists(conn, invalidateList, ip, hostname);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the Package owner of an IPAddress.
     */
    public static void setIPAddressPackage(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipAddress,
        String newPackage
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "setIPAddressPackage(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            checkAccessIPAddress(conn, source, "setIPAddressPackage", ipAddress);
            PackageHandler.checkAccessPackage(conn, source, "setIPAddressPackage", newPackage);

            setIPAddressPackage(conn, invalidateList, ipAddress, newPackage);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Sets the Package owner of an IPAddress.
     */
    public static void setIPAddressPackage(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int ipAddress,
        String newPackage
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "setIPAddressPackage(MasterDatabaseConnection,InvalidateList,int,String)", null);
        try {
            String oldAccounting=getBusinessForIPAddress(conn, ipAddress);
            String newAccounting=PackageHandler.getBusinessForPackage(conn, newPackage);
            int aoServer=getAOServerForIPAddress(conn, ipAddress);

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
                aoServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getSharedHttpdIP(MasterDatabaseConnection conn, int aoServer, boolean supportsModJK) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getSharedIP(MasterDatabaseConnection,int,boolean)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  coalesce(\n"
                + "    (\n"
                + "      select\n"
                + "        ia.pkey\n"
                + "      from\n"
                + "        ip_addresses ia,\n"
                + "        net_devices nd\n"
                + "        left join net_binds nb on nd.ao_server=nb.ao_server and nb.port in (80, 443) and nb.net_protocol='"+NetProtocol.TCP+"'\n"
                + "        left join httpd_binds hb on nb.pkey=hb.net_bind\n"
                + "        left join httpd_servers hs on hb.httpd_server=hs.pkey\n"
                + "      where\n"
                + "        ia.is_overflow\n"
                + "        and ia.net_device=nd.pkey\n"
                + "        and nd.ao_server=?\n"
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
                + "            nb2.ao_server=?\n"
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForIPAddress(MasterDatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getPackageForIPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select package from ip_addresses where pkey=?", ipAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForIPAddress(MasterDatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getBusinessForIPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from ip_addresses ia, packages pk where ia.pkey=? and ia.package=pk.name", ipAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForIPAddress(MasterDatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getAOServerForIPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select nd.ao_server from ip_addresses ia, net_devices nd where ia.pkey=? and ia.net_device=nd.pkey", ipAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getIPStringForIPAddress(MasterDatabaseConnection conn, int ipAddress) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getIPStringForIPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select ip_address from ip_addresses where pkey=?", ipAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getWildcardIPAddress(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getWildcardIPAddress(MasterDatabaseConnection)", null);
        try {
            return conn.executeIntQuery("select pkey from ip_addresses where ip_address=? limit 1", IPAddress.WILDCARD_IP);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getLoopbackIPAddress(MasterDatabaseConnection conn, int aoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "getLoopbackIPAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  ia.pkey\n"
                + "from\n"
                + "  ip_addresses ia,\n"
                + "  net_devices nd\n"
                + "where\n"
                + "  ia.ip_address=?\n"
                + "  and ia.net_device=nd.pkey\n"
                + "  and nd.ao_server=?\n"
                + "limit 1",
                IPAddress.LOOPBACK_IP,
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void releaseIPAddress(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int ipAddress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, IPAddressHandler.class, "releaseIPAddress(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            setIPAddressHostname(
                conn,
                invalidateList,
                ipAddress,
                getUnassignedHostname(conn, ipAddress)
            );

            conn.executeUpdate(
                "update ip_addresses set available=true, price='0.00' where pkey=?",
                ipAddress
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.IP_ADDRESSES,
                getBusinessForIPAddress(conn, ipAddress),
                ServerHandler.getHostnameForServer(conn, getAOServerForIPAddress(conn, ipAddress)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}