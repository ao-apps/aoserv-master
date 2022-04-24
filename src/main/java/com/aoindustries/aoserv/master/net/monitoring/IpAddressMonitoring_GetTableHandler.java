/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.net.monitoring;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.net.monitoring.IpAddressMonitoring;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import static com.aoindustries.aoserv.master.TableHandler.BU2_PARENTS_JOIN;
import static com.aoindustries.aoserv.master.TableHandler.PK3_BU2_PARENTS_WHERE;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class IpAddressMonitoring_GetTableHandler extends TableHandler.GetTableHandlerByRole {

  @Override
  public Set<Table.TableID> getTableIds() {
    return EnumSet.of(Table.TableID.IpAddressMonitoring);
  }

  @Override
  protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new IpAddressMonitoring(),
        "select\n"
            + "  iam.*\n"
            + "from\n"
            + "  net.\"IpAddress\" ia\n"
            + "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id"
    );
  }

  @Override
  protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new IpAddressMonitoring(),
        "select\n"
            + "  iam.*\n"
            + "from\n"
            + "  net.\"IpAddress\" ia\n"
            + "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
            + "where\n"
            + "  ia.id in (\n"
            + "    select\n"
            + "      ia2.id\n"
            + "    from\n"
            + "      master.\"UserHost\" ms\n"
            + "      left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
            + "      net.\"Device\" nd\n"
            + "      right outer join net.\"IpAddress\" ia2 on nd.id=ia2.device\n"
            + "    where\n"
            + "      ia2.\"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\" or (\n"
            + "        ms.username=?\n"
            + "        and (\n"
            + "          ms.server=nd.server\n"
            + "          or ff.server=nd.server\n"
            + "          or (\n"
            + "            select\n"
            + "              ffr.id\n"
            + "            from\n"
            + "              backup.\"FileReplication\" ffr\n"
            + "              inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
            + "              inner join linux.\"Server\" bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
            + "            where\n"
            + "              ms.server=ffr.server\n"
            + "              and bp.ao_server=nd.server\n"
            + "              and bpao.\"daemonDeviceId\"=nd.\"deviceId\"\n" // Only allow access to the device device ID for failovers
            + "            limit 1\n"
            + "          ) is not null\n"
            + "        )\n"
            + "      )\n"
            + "  )",
        IpAddress.WILDCARD_IP,
        source.getCurrentAdministrator()
    );
  }

  @Override
  protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    MasterServer.writeObjects(
        conn,
        source,
        out,
        provideProgress,
        CursorMode.AUTO,
        new IpAddressMonitoring(),
        "select\n"
            + "  iam.*\n"
            + "from\n"
            + "  net.\"IpAddress\" ia\n"
            + "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
            + "where\n"
            + "  ia.\"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\"\n"
            + "  or ia.id in (\n"
            + "    select\n"
            + "      ia2.id\n"
            + "    from\n"
            + "      account.\"User\" un1,\n"
            + "      billing.\"Package\" pk1,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "      billing.\"Package\" pk2,\n"
            + "      net.\"IpAddress\" ia2\n"
            + "    where\n"
            + "      un1.username=?\n"
            + "      and un1.package=pk1.name\n"
            + "      and (\n"
            + TableHandler.PK1_BU1_PARENTS_WHERE
            + "      )\n"
            + "      and bu1.accounting=pk2.accounting\n"
            + "      and pk2.id=ia2.package\n"
            + "  )\n"
            + "  or ia.id in (\n"
            + "    select\n"
            + "      nb.\"ipAddress\"\n"
            + "    from\n"
            + "      account.\"User\" un3,\n"
            + "      billing.\"Package\" pk3,\n"
            + BU2_PARENTS_JOIN
            + "      billing.\"Package\" pk4,\n"
            + "      web.\"Site\" hs,\n"
            + "      web.\"VirtualHost\" hsb,\n"
            + "      net.\"Bind\" nb\n"
            + "    where\n"
            + "      un3.username=?\n"
            + "      and un3.package=pk3.name\n"
            + "      and (\n"
            + PK3_BU2_PARENTS_WHERE
            + "      )\n"
            + "      and bu" + Account.MAXIMUM_BUSINESS_TREE_DEPTH + ".accounting=pk4.accounting\n"
            + "      and pk4.name=hs.package\n"
            + "      and hs.id=hsb.httpd_site\n"
            + "      and hsb.httpd_bind=nb.id\n"
            + "  ) or ia.id in (\n"
            + "    select\n"
            + "      ia5.id\n"
            + "    from\n"
            + "      account.\"User\" un5,\n"
            + "      billing.\"Package\" pk5,\n"
            + "      account.\"AccountHost\" bs5,\n"
            + "      net.\"Device\" nd5,\n"
            + "      net.\"IpAddress\" ia5\n"
            + "    where\n"
            + "      un5.username=?\n"
            + "      and un5.package=pk5.name\n"
            + "      and pk5.accounting=bs5.accounting\n"
            + "      and bs5.server=nd5.server\n"
            + "      and nd5.id=ia5.device\n"
            + "      and (ia5.\"inetAddress\"=?::\"com.aoapps.net\".\"InetAddress\" or ia5.\"isOverflow\")\n"
            /*+ "  ) or ia.id in (\n"
            + "    select\n"
            + "      ia6.id\n"
            + "    from\n"
            + "      account.\"User\" un6,\n"
            + "      billing.\"Package\" pk6,\n"
            + "      account.\"AccountHost\" bs6,\n"
            + "      backup.\"FileReplication\" ffr6,\n"
            + "      backup.\"BackupPartition\" bp6,\n"
            + "      linux.\"Server\" ao6,\n"
            + "      net.\"Device\" nd6,\n"
            + "      net.\"IpAddress\" ia6\n"
            + "    where\n"
            + "      un6.username=?\n"
            + "      and un6.package=pk6.name\n"
            + "      and pk6.accounting=bs6.accounting\n"
            + "      and bs6.server=ffr6.server\n"
            + "      and ffr6.backup_partition=bp6.id\n"
            + "      and bp6.ao_server=ao6.server\n"
            + "      and ao6.server=nd6.ao_server and ao6.\"daemonDeviceId\"=nd6.\"deviceId\"\n"
            + "      and nd6.id=ia6.device and not ia6.\"isAlias\"\n"*/
            + "  )",
        IpAddress.WILDCARD_IP,
        currentAdministrator,
        currentAdministrator,
        currentAdministrator,
        IpAddress.LOOPBACK_IP//,
    //username
    );
  }
}
