package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>NetBindHandler</code> handles all the accesses to the <code>net_binds</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetBindHandler {

    private static final Object netBindLock=new Object();

    public static int addNetBind(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer,
        String packageName,
        int ipAddress,
        int port,
        String netProtocol,
        String appProtocol,
        boolean openFirewall,
        boolean monitoringEnabled
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "addNetBind(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,int,int,String,String,boolean,boolean)", null);
        try {
            if(
                conn.executeBooleanQuery("select (select protocol from protocols where protocol=?) is null", appProtocol)
            ) throw new SQLException("Unable to find in table protocols: "+appProtocol);

            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            if(mu==null) {
                // Must be a user service
                if(
                    !conn.executeBooleanQuery("select is_user_service from protocols where protocol=?", appProtocol)
                ) throw new SQLException("Only master users may add non-user net_binds.");

                // Must match the default port
                if(
                    port!=conn.executeIntQuery("select port from protocols where protocol=?", appProtocol)
                ) throw new SQLException("Only master users may override the port for a service.");

                // Must match the default net protocol
                if(
                    !netProtocol.equals(
                        conn.executeStringQuery("select net_protocol from protocols where protocol=?", appProtocol)
                    )
                ) throw new SQLException("Only master users may override the net protocol for a service.");
            }
            
            ServerHandler.checkAccessServer(conn, source, "addNetBind", aoServer);
            PackageHandler.checkAccessPackage(conn, source, "addNetBind", packageName);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add net bind, package disabled: "+packageName);
            IPAddressHandler.checkAccessIPAddress(conn, source, "addNetBind", ipAddress);
            String ipString=IPAddressHandler.getIPStringForIPAddress(conn, ipAddress);

            String farm=ServerHandler.getFarmForServer(conn, aoServer);

            int pkey;
            synchronized(netBindLock) {
                if(ipString.equals(IPAddress.WILDCARD_IP)) {
                    // Wildcard must be unique per farm, with the port completely free
                    if(
                        conn.executeBooleanQuery(
                            "select\n"
                            + "  (\n"
                            + "    select\n"
                            + "      nb.pkey\n"
                            + "    from\n"
                            + "      net_binds nb,\n"
                            + "      servers se\n"
                            + "    where\n"
                            + "      nb.ao_server=se.pkey\n"
                            + "      and se.farm=?\n"
                            + "      and nb.port=?\n"
                            + "      and nb.net_protocol=?\n"
                            + "    limit 1\n"
                            + "  ) is not null",
                            farm,
                            port,
                            netProtocol
                        )
                    ) throw new SQLException("NetBind already in use: "+aoServer+"->"+ipAddress+":"+port+" ("+netProtocol+')');
                } else if(ipString.equals(IPAddress.LOOPBACK_IP)) {
                    // Loopback must be unique per farm and not have wildcard
                    if(
                        conn.executeBooleanQuery(
                            "select\n"
                            + "  (\n"
                            + "    select\n"
                            + "      nb.pkey\n"
                            + "    from\n"
                            + "      net_binds nb,\n"
                            + "      servers se,\n"
                            + "      ip_addresses ia\n"
                            + "    where\n"
                            + "      nb.ao_server=se.pkey\n"
                            + "      and se.farm=?\n"
                            + "      and nb.ip_address=ia.pkey\n"
                            + "      and (\n"
                            + "        ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                            + "        or ia.ip_address='"+IPAddress.LOOPBACK_IP+"'\n"
                            + "      ) and nb.port=?\n"
                            + "      and nb.net_protocol=?\n"
                            + "    limit 1\n"
                            + "  ) is not null",
                            farm,
                            port,
                            netProtocol
                        )
                    ) throw new SQLException("NetBind already in use: "+aoServer+"->"+ipAddress+":"+port+" ("+netProtocol+')');
                } else {
                    // Make sure that this port is not already allocated within this farm on this IP or the wildcard
                    if(
                        conn.executeBooleanQuery(
                            "select\n"
                            + "  (\n"
                            + "    select\n"
                            + "      nb.pkey\n"
                            + "    from\n"
                            + "      net_binds nb,\n"
                            + "      servers se,\n"
                            + "      ip_addresses ia\n"
                            + "    where\n"
                            + "      nb.ao_server=se.pkey\n"
                            + "      and se.farm=?\n"
                            + "      and nb.ip_address=ia.pkey\n"
                            + "      and (\n"
                            + "        ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                            + "        or nb.ip_address=?\n"
                            + "      ) and nb.port=?\n"
                            + "      and nb.net_protocol=?\n"
                            + "    limit 1\n"
                            + "  ) is not null",
                            farm,
                            ipAddress,
                            port,
                            netProtocol
                        )
                    ) throw new SQLException("NetBind already in use: "+aoServer+"->"+ipAddress+":"+port+" ("+netProtocol+')');
                }

                // Add the port to the DB
                pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('net_binds_pkey_seq')");
                conn.executeUpdate(
                    "insert into\n"
                    + "  net_binds\n"
                    + "values(\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?,\n"
                    + "  ?\n"
                    + ")",
                    pkey,
                    packageName,
                    aoServer,
                    ipAddress,
                    port,
                    netProtocol,
                    appProtocol,
                    openFirewall,
                    monitoringEnabled
                );
            }

            invalidateList.addTable(
                conn,
                SchemaTable.NET_BINDS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int allocateNetBind(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int aoServer,
        int ipAddress,
        String netProtocol,
        String appProtocol,
        String pack
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "allocateNetBind(MasterDatabaseConnection,InvalidateList,int,int,String,String,String)", null);
        try {
            String farm=ServerHandler.getFarmForServer(conn, aoServer);
            String ipString=IPAddressHandler.getIPStringForIPAddress(conn, ipAddress);
            int pkey;
            synchronized(netBindLock) {
                pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('net_binds_pkey_seq')");
                if(ipString.equals(IPAddress.WILDCARD_IP)) {
                    conn.executeUpdate(
                        "insert into\n"
                        + "  net_binds\n"
                        + "values(\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  (\n"
                        + "    select\n"
                        + "      np.port\n"
                        + "    from\n"
                        + "      net_ports np\n"
                        + "    where\n"
                        + "      np.is_user\n"
                        + "      and np.port!="+HttpdWorker.ERROR_CAUSING_PORT+"\n"
                        + "      and (\n"
                        + "        select\n"
                        + "          nb.pkey\n"
                        + "        from\n"
                        + "          net_binds nb,\n"
                        + "          servers se\n"
                        + "        where\n"
                        + "          nb.ao_server=se.pkey\n"
                        + "          and se.farm=?\n"
                        + "          and np.port=nb.port\n"
                        + "          and nb.net_protocol=?\n"
                        + "        limit 1\n"
                        + "      ) is null\n"
                        + "    order by\n"
                        + "      port\n"
                        + "    limit 1\n"
                        + "  ),\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  false,\n"
                        + "  false\n"
                        + ")",
                        pkey,
                        pack,
                        aoServer,
                        ipAddress,
                        farm,
                        netProtocol,
                        netProtocol,
                        appProtocol
                    );
                } else {
                    conn.executeUpdate(
                        "insert into\n"
                        + "  net_binds\n"
                        + "values(\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  (\n"
                        + "    select\n"
                        + "      np.port\n"
                        + "    from\n"
                        + "      net_ports np\n"
                        + "    where\n"
                        + "      np.is_user\n"
                        + "      and np.port!="+HttpdWorker.ERROR_CAUSING_PORT+"\n"
                        + "      and (\n"
                        + "        select\n"
                        + "          nb.pkey\n"
                        + "        from\n"
                        + "          net_binds nb,\n"
                        + "          servers se,\n"
                        + "          ip_addresses ia\n"
                        + "        where\n"
                        + "          nb.ao_server=se.pkey\n"
                        + "          and se.farm=?\n"
                        + "          and nb.ip_address=ia.pkey\n"
                        + "          and (\n"
                        + "            nb.ip_address=?\n"
                        + "            or ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                        + "          )  and np.port=nb.port\n"
                        + "          and nb.net_protocol=?\n"
                        + "        limit 1\n"
                        + "      ) is null\n"
                        + "    order by\n"
                        + "      port\n"
                        + "    limit 1\n"
                        + "  ),\n"
                        + "  ?,\n"
                        + "  ?,\n"
                        + "  false,\n"
                        + "  false\n"
                        + ")",
                        pkey,
                        pack,
                        aoServer,
                        ipAddress,
                        farm,
                        ipAddress,
                        netProtocol,
                        netProtocol,
                        appProtocol
                    );
                }
            }
            invalidateList.addTable(
                conn,
                SchemaTable.NET_BINDS,
                PackageHandler.getBusinessForPackage(conn, pack),
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForNetBind(
        MasterDatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "getBusinessForNetBind(MasterDatabaseConnection,pkey)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from net_binds nb, packages pk where nb.pkey=? and nb.package=pk.name", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getNetBind(
        MasterDatabaseConnection conn,
        int aoServer,
        int ipAddress,
        int port,
        String netProtocol
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "getNetBind(MasterDatabaseConnection,int,int,int,String)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  coalesce(\n"
                + "    (\n"
                + "      select\n"
                + "        pkey\n"
                + "      from\n"
                + "        net_binds\n"
                + "      where\n"
                + "        ao_server=?\n"
                + "        and ip_address=?\n"
                + "        and port=?\n"
                + "        and net_protocol=?\n"
                + "    ), -1\n"
                + "  )",
                aoServer,
                ipAddress,
                port,
                netProtocol
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForNetBind(
        MasterDatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "getAOServerForNetBind(MasterDatabaseConnection,pkey)", null);
        try {
            return conn.executeIntQuery("select ao_server from net_binds where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForNetBind(
        MasterDatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "getPackageForNetBind(MasterDatabaseConnection,pkey)", null);
        try {
            return conn.executeStringQuery("select package from net_binds where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeNetBind(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "removeNetBind(MasterDatabaseConnection,RequestSource,InvalidateList,pkey)", null);
        try {
            // Security checks
            PackageHandler.checkAccessPackage(conn, source, "removeNetBind", getPackageForNetBind(conn, pkey));

            // Do the remove
            removeNetBind(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeNetBind(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "removeNetBind(MasterDatabaseConnection,InvalidateList,pkey)", null);
        try {
            String business=getBusinessForNetBind(conn, pkey);
            int aoServer=getAOServerForNetBind(conn, pkey);
            if(conn.executeBooleanQuery("select (select net_bind from net_tcp_redirects where net_bind=?) is not null", pkey)) {
                conn.executeUpdate("delete from net_tcp_redirects where net_bind=?", pkey);
                invalidateList.addTable(
                    conn,
                    SchemaTable.NET_TCP_REDIRECTS,
                    business,
                    ServerHandler.getHostnameForServer(conn, aoServer),
                    false
                );
            }
            
            if(conn.executeBooleanQuery("select (select net_bind from private_ftp_servers where net_bind=?) is not null", pkey)) {
                conn.executeUpdate("delete from private_ftp_servers where net_bind=?", pkey);
                invalidateList.addTable(
                    conn,
                    SchemaTable.PRIVATE_FTP_SERVERS,
                    business,
                    ServerHandler.getHostnameForServer(conn, aoServer),
                    false
                );
            }

            conn.executeUpdate("delete from net_binds where pkey=?", pkey);
            invalidateList.addTable(
                conn,
                SchemaTable.NET_BINDS,
                business,
                ServerHandler.getHostnameForServer(conn, aoServer),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setNetBindMonitoringEnabled(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean enabled
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "setNetBindMonitoringEnabled(MasterDatabaseConnection,RequestSource,InvalidateList,pkey,boolean)", null);
        try {
            PackageHandler.checkAccessPackage(conn, source, "setNetBindMonitoringEnabled", getPackageForNetBind(conn, pkey));

            conn.executeUpdate("update net_binds set monitoring_enabled=? where pkey=?", enabled, pkey);

            invalidateList.addTable(
                conn,
                SchemaTable.NET_BINDS,
                getBusinessForNetBind(conn, pkey),
                ServerHandler.getHostnameForServer(conn, getAOServerForNetBind(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setNetBindOpenFirewall(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean open_firewall
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, NetBindHandler.class, "setNetBindOpenFirewall(MasterDatabaseConnection,RequestSource,InvalidateList,pkey,boolean)", null);
        try {
            PackageHandler.checkAccessPackage(conn, source, "setNetBindOpenFirewall", getPackageForNetBind(conn, pkey));

            conn.executeUpdate("update net_binds set open_firewall=? where pkey=?", open_firewall, pkey);

            invalidateList.addTable(
                conn,
                SchemaTable.NET_BINDS,
                getBusinessForNetBind(conn, pkey),
                ServerHandler.getHostnameForServer(conn, getAOServerForNetBind(conn, pkey)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}