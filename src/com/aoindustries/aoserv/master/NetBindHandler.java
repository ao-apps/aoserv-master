/*
 * Copyright 2001-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.HttpdWorker;
import com.aoindustries.aoserv.client.IPAddress;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The <code>NetBindHandler</code> handles all the accesses to the <code>net_binds</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class NetBindHandler {

    private NetBindHandler() {
    }

    /**
     * This lock is used to avoid a race condition between check and insert when allocating net_binds.
     */
    private static final Object netBindLock=new Object();

    public static int addNetBind(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        String packageName,
        int ipAddress,
        int port,
        String netProtocol,
        String appProtocol,
        boolean openFirewall,
        boolean monitoringEnabled
    ) throws IOException, SQLException {
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

        ServerHandler.checkAccessServer(conn, source, "addNetBind", server);
        PackageHandler.checkAccessPackage(conn, source, "addNetBind", packageName);
        if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add net bind, package disabled: "+packageName);
        IPAddressHandler.checkAccessIPAddress(conn, source, "addNetBind", ipAddress);
        String ipString=IPAddressHandler.getIPStringForIPAddress(conn, ipAddress);

        // Now allocating unique to entire system for server portability between farms
        //String farm=ServerHandler.getFarmForServer(conn, aoServer);

        int pkey;
        synchronized(netBindLock) {
            if(ipString.equals(IPAddress.WILDCARD_IP)) {
                // Wildcard must be unique to AOServ system, with the port completely free
                if(
                    conn.executeBooleanQuery(
                        "select\n"
                        + "  (\n"
                        + "    select\n"
                        + "      nb.pkey\n"
                        + "    from\n"
                        + "      net_binds nb,\n"
                        + "      servers se\n"
						+ "      left outer join ao_servers ao on se.pkey=ao.server\n"
                        + "    where\n"
                        + "      nb.server=se.pkey\n"
                        //+ "      and se.farm=?\n"
                        + "      and nb.port=?\n"
                        + "      and nb.net_protocol=?\n"
						+ "      and (se.pkey=? or ao.server is not null)\n" // Per-server unique for unmanaged, per-AOServ unique for managed
                        + "    limit 1\n"
                        + "  ) is not null",
                        //farm,
                        port,
                        netProtocol,
						server
                    )
                ) throw new SQLException("NetBind already in use: "+server+"->"+ipAddress+":"+port+" ("+netProtocol+')');
            } else if(ipString.equals(IPAddress.LOOPBACK_IP)) {
                // Loopback must be unique to AOServ system and not have wildcard
                if(
                    conn.executeBooleanQuery(
                        "select\n"
                        + "  (\n"
                        + "    select\n"
                        + "      nb.pkey\n"
                        + "    from\n"
                        + "      net_binds nb,\n"
                        + "      servers se\n"
						+ "      left outer join ao_servers ao on se.pkey=ao.server,\n"
                        + "      ip_addresses ia\n"
                        + "    where\n"
                        + "      nb.server=se.pkey\n"
                        //+ "      and se.farm=?\n"
                        + "      and nb.ip_address=ia.pkey\n"
                        + "      and (\n"
                        + "        ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                        + "        or ia.ip_address='"+IPAddress.LOOPBACK_IP+"'\n"
                        + "      ) and nb.port=?\n"
                        + "      and nb.net_protocol=?\n"
						+ "      and (se.pkey=? or ao.server is not null)\n" // Per-server unique for unmanaged, per-AOServ unique for managed
                        + "    limit 1\n"
                        + "  ) is not null",
                        //farm,
                        port,
                        netProtocol,
						server
                    )
                ) throw new SQLException("NetBind already in use: "+server+"->"+ipAddress+":"+port+" ("+netProtocol+')');
            } else {
                // Make sure that this port is not already allocated within the system on this IP or the wildcard
                if(
                    conn.executeBooleanQuery(
                        "select\n"
                        + "  (\n"
                        + "    select\n"
                        + "      nb.pkey\n"
                        + "    from\n"
                        + "      net_binds nb,\n"
                        + "      servers se\n"
						+ "      left outer join ao_servers ao on se.pkey=ao.server,\n"
                        + "      ip_addresses ia\n"
                        + "    where\n"
                        + "      nb.server=se.pkey\n"
                        //+ "      and se.farm=?\n"
                        + "      and nb.ip_address=ia.pkey\n"
                        + "      and (\n"
                        + "        ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                        + "        or nb.ip_address=?\n"
                        + "      ) and nb.port=?\n"
                        + "      and nb.net_protocol=?\n"
						+ "      and (se.pkey=? or ao.server is not null)\n" // Per-server unique for unmanaged, per-AOServ unique for managed
                        + "    limit 1\n"
                        + "  ) is not null",
                        //farm,
                        ipAddress,
                        port,
                        netProtocol,
						server
                    )
                ) throw new SQLException("NetBind already in use: "+server+"->"+ipAddress+":"+port+" ("+netProtocol+')');
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
                server,
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
            SchemaTable.TableID.NET_BINDS,
            PackageHandler.getBusinessForPackage(conn, packageName),
            server,
            false
        );
        return pkey;
    }

    /**
     * Now allocating unique to entire system for server portability between farms
     */
    public static int allocateNetBind(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int server,
        int ipAddress,
        String netProtocol,
        String appProtocol,
        String pack,
        int minimumPort
    ) throws IOException, SQLException {
        //String farm=ServerHandler.getFarmForServer(conn, aoServer);
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
                    + "      and np.port>=?\n"
                    + "      and (\n"
                    + "        select\n"
                    + "          nb.pkey\n"
                    + "        from\n"
                    + "          net_binds nb,\n"
                    + "          servers se\n"
                    + "        where\n"
                    + "          nb.server=se.pkey\n"
                    // Now allocating unique to entire system for server portability between farms
                    //+ "          and se.farm=?\n"
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
                    server,
                    ipAddress,
                    minimumPort,
                    //farm,
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
                    + "      and np.port>=?\n"
                    + "      and (\n"
                    + "        select\n"
                    + "          nb.pkey\n"
                    + "        from\n"
                    + "          net_binds nb,\n"
                    + "          servers se,\n"
                    + "          ip_addresses ia\n"
                    + "        where\n"
                    + "          nb.server=se.pkey\n"
                    // Now allocating unique to entire system for server portability between farms
                    //+ "          and se.farm=?\n"
                    + "          and nb.ip_address=ia.pkey\n"
                    + "          and (\n"
                    + "            ia.ip_address=(select ip_address from ip_addresses where pkey=?)\n"
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
                    server,
                    ipAddress,
                    minimumPort,
                    //farm,
                    ipAddress,
                    netProtocol,
                    netProtocol,
                    appProtocol
                );
            }
        }
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.NET_BINDS,
            PackageHandler.getBusinessForPackage(conn, pack),
            server,
            false
        );
        return pkey;
    }

    public static AccountingCode getBusinessForNetBind(
        DatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select pk.accounting from net_binds nb, packages pk where nb.pkey=? and nb.package=pk.name",
            pkey
        );
    }

    public static int getNetBind(
        DatabaseConnection conn,
        int server,
        int ipAddress,
        int port,
        String netProtocol
    ) throws IOException, SQLException {
        return conn.executeIntQuery(
            "select\n"
            + "  coalesce(\n"
            + "    (\n"
            + "      select\n"
            + "        pkey\n"
            + "      from\n"
            + "        net_binds\n"
            + "      where\n"
            + "        server=?\n"
            + "        and ip_address=?\n"
            + "        and port=?\n"
            + "        and net_protocol=?\n"
            + "    ), -1\n"
            + "  )",
            server,
            ipAddress,
            port,
            netProtocol
        );
    }

    public static int getServerForNetBind(
        DatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        return conn.executeIntQuery("select server from net_binds where pkey=?", pkey);
    }

    public static String getPackageForNetBind(
        DatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        return conn.executeStringQuery("select package from net_binds where pkey=?", pkey);
    }

    public static void removeNetBind(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Security checks
        PackageHandler.checkAccessPackage(conn, source, "removeNetBind", getPackageForNetBind(conn, pkey));

        // Do the remove
        removeNetBind(conn, invalidateList, pkey);
    }

    public static void removeNetBind(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        AccountingCode business = getBusinessForNetBind(conn, pkey);
        int server=getServerForNetBind(conn, pkey);

        if(conn.executeBooleanQuery("select (select net_bind from httpd_binds where net_bind=?) is not null", pkey)) {
            conn.executeUpdate("delete from httpd_binds where net_bind=?", pkey);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.HTTPD_BINDS,
                business,
                server,
                false
            );
        }

        if(conn.executeBooleanQuery("select (select net_bind from net_tcp_redirects where net_bind=?) is not null", pkey)) {
            conn.executeUpdate("delete from net_tcp_redirects where net_bind=?", pkey);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.NET_TCP_REDIRECTS,
                business,
                server,
                false
            );
        }

        if(conn.executeBooleanQuery("select (select net_bind from private_ftp_servers where net_bind=?) is not null", pkey)) {
            conn.executeUpdate("delete from private_ftp_servers where net_bind=?", pkey);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.PRIVATE_FTP_SERVERS,
                business,
                server,
                false
            );
        }

        conn.executeUpdate("delete from net_binds where pkey=?", pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.NET_BINDS,
            business,
            server,
            false
        );
    }

    public static void setNetBindMonitoringEnabled(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean enabled
    ) throws IOException, SQLException {
        PackageHandler.checkAccessPackage(conn, source, "setNetBindMonitoringEnabled", getPackageForNetBind(conn, pkey));

        conn.executeUpdate("update net_binds set monitoring_enabled=? where pkey=?", enabled, pkey);

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.NET_BINDS,
            getBusinessForNetBind(conn, pkey),
            getServerForNetBind(conn, pkey),
            false
        );
    }

    public static void setNetBindOpenFirewall(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean open_firewall
    ) throws IOException, SQLException {
        PackageHandler.checkAccessPackage(conn, source, "setNetBindOpenFirewall", getPackageForNetBind(conn, pkey));

        conn.executeUpdate("update net_binds set open_firewall=? where pkey=?", open_firewall, pkey);

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.NET_BINDS,
            getBusinessForNetBind(conn, pkey),
            getServerForNetBind(conn, pkey),
            false
        );
    }
}