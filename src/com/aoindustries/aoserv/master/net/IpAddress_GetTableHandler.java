/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.IpAddress;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import static com.aoindustries.aoserv.master.TableHandler.BU2_PARENTS_JOIN;
import static com.aoindustries.aoserv.master.TableHandler.PK3_BU2_PARENTS_WHERE;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class IpAddress_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.IP_ADDRESSES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new IpAddress(),
			"select\n"
			+ "  ia.id,\n"
			+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
			+ "  ia.device,\n"
			+ "  ia.\"isAlias\",\n"
			+ "  ia.hostname,\n"
			+ "  ia.package,\n"
			+ "  ia.created,\n"
			+ "  ia.\"isAvailable\",\n"
			+ "  ia.\"isOverflow\",\n"
			+ "  ia.\"isDhcp\",\n"
			+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
			+ "  ia.netmask,\n"
			// Protocol conversion
			+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
			+ "  iam.\"pingMonitorEnabled\",\n"
			+ "  iam.\"checkBlacklistsOverSmtp\",\n"
			+ "  iam.enabled as \"monitoringEnabled\"\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id"
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new IpAddress(),
			"select\n"
			+ "  ia.id,\n"
			+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
			+ "  ia.device,\n"
			+ "  ia.\"isAlias\",\n"
			+ "  ia.hostname,\n"
			+ "  ia.package,\n"
			+ "  ia.created,\n"
			+ "  ia.\"isAvailable\",\n"
			+ "  ia.\"isOverflow\",\n"
			+ "  ia.\"isDhcp\",\n"
			+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
			+ "  ia.netmask,\n"
			// Protocol conversion
			+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
			+ "  iam.\"pingMonitorEnabled\",\n"
			+ "  iam.\"checkBlacklistsOverSmtp\",\n"
			+ "  iam.enabled as \"monitoringEnabled\"\n"
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
			+ "      ia2.\"inetAddress\"=?::inet or (\n"
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
			source.getUsername()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		UserId username = source.getUsername();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new IpAddress(),
			"select\n"
			+ "  ia.id,\n"
			+ "  host(ia.\"inetAddress\") as \"inetAddress\",\n"
			+ "  ia.device,\n"
			+ "  ia.\"isAlias\",\n"
			+ "  ia.hostname,\n"
			+ "  ia.package,\n"
			+ "  ia.created,\n"
			+ "  ia.\"isAvailable\",\n"
			+ "  ia.\"isOverflow\",\n"
			+ "  ia.\"isDhcp\",\n"
			+ "  host(ia.\"externalInetAddress\") as \"externalInetAddress\",\n"
			+ "  ia.netmask,\n"
			// Protocol conversion
			+ "  (select pk.name from billing.\"Package\" pk where pk.id = ia.package) as \"packageName\",\n"
			+ "  iam.\"pingMonitorEnabled\",\n"
			+ "  iam.\"checkBlacklistsOverSmtp\",\n"
			+ "  iam.enabled as \"monitoringEnabled\"\n"
			+ "from\n"
			+ "  net.\"IpAddress\" ia\n"
			+ "  inner join \"net.monitoring\".\"IpAddressMonitoring\" iam on ia.id = iam.id\n"
			+ "where\n"
			+ "  ia.\"inetAddress\"=?::inet\n"
			+ "  or ia.id in (\n"
			+ "    select\n"
			+ "      ia2.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un1,\n"
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
			+ "      account.\"Username\" un3,\n"
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
			+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
			+ "      and pk4.name=hs.package\n"
			+ "      and hs.id=hsb.httpd_site\n"
			+ "      and hsb.httpd_bind=nb.id\n"
			+ "  ) or ia.id in (\n"
			+ "    select\n"
			+ "      ia5.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un5,\n"
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
			+ "      and (ia5.\"inetAddress\"=?::inet or ia5.\"isOverflow\")\n"
			/*+ "  ) or ia.id in (\n"
			+ "    select \n"
			+ "      ia6.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un6,\n"
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
			username,
			username,
			username,
			IpAddress.LOOPBACK_IP//,
			//username
		);
	}
}
