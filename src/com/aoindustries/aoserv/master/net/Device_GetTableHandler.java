/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.Device;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Device_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.NET_DEVICES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Device(),
			"select"
			+ "  id,\n"
			+ "  server,\n"
			+ "  \"deviceId\",\n"
			+ "  description,\n"
			+ "  delete_route,\n"
			+ "  host(gateway) as gateway,\n"
			+ "  host(network) as network,\n"
			+ "  host(broadcast) as broadcast,\n"
			+ "  mac_address::text,\n"
			+ "  max_bit_rate,\n"
			+ "  monitoring_bit_rate_low,\n"
			+ "  monitoring_bit_rate_medium,\n"
			+ "  monitoring_bit_rate_high,\n"
			+ "  monitoring_bit_rate_critical,\n"
			+ "  monitoring_enabled\n"
			+ "from\n"
			+ "  net.\"Device\""
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
			new Device(),
			"select distinct\n"
			+ "  nd.id,\n"
			+ "  nd.server,\n"
			+ "  nd.\"deviceId\",\n"
			+ "  nd.description,\n"
			+ "  nd.delete_route,\n"
			+ "  host(nd.gateway) as gateway,\n"
			+ "  host(nd.network) as network,\n"
			+ "  host(nd.broadcast) as broadcast,\n"
			+ "  nd.mac_address::text,\n"
			+ "  nd.max_bit_rate,\n"
			+ "  nd.monitoring_bit_rate_low,\n"
			+ "  nd.monitoring_bit_rate_medium,\n"
			+ "  nd.monitoring_bit_rate_high,\n"
			+ "  nd.monitoring_bit_rate_critical,\n"
			+ "  nd.monitoring_enabled\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
			+ "  net.\"Device\" nd\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			+ "    ms.server=nd.server\n"
			+ "    or ff.server=nd.server\n"
			+ "    or (\n"
			+ "      select\n"
			+ "        ffr.id\n"
			+ "      from\n"
			+ "        backup.\"FileReplication\" ffr\n"
			+ "        inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
			+ "        inner join linux.\"Server\" bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
			+ "      where\n"
			+ "        ms.server=ffr.server\n"
			+ "        and bp.ao_server=nd.server\n"
			+ "        and bpao.\"daemonDeviceId\"=nd.\"deviceId\"\n" // Only allow access to the device device ID for failovers
			+ "      limit 1\n"
			+ "    ) is not null\n"
			+ "  )",
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Device(),
			"select\n" // distinct
			+ "  nd.id,\n"
			+ "  nd.server,\n"
			+ "  nd.\"deviceId\",\n"
			+ "  nd.description,\n"
			+ "  nd.delete_route,\n"
			+ "  host(nd.gateway) as gateway,\n"
			+ "  host(nd.network) as network,\n"
			+ "  host(nd.broadcast) as broadcast,\n"
			+ "  nd.mac_address::text,\n"
			+ "  nd.max_bit_rate,\n"
			+ "  nd.monitoring_bit_rate_low,\n"
			+ "  nd.monitoring_bit_rate_medium,\n"
			+ "  nd.monitoring_bit_rate_high,\n"
			+ "  nd.monitoring_bit_rate_critical,\n"
			+ "  nd.monitoring_enabled\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			// Allow failover destinations
			//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
			//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
			//+ "  left join linux.\"Server\" bpao on bp.ao_server=bpao.server,\n"
			+ "  net.\"Device\" nd\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    bs.server=nd.server\n"
			// Need distinct above when using this or
			//+ "    or (bp.ao_server=nd.ao_server and nd.\"deviceId\"=bpao.\"daemonDeviceId\")\n"
			+ "  )",
			source.getCurrentAdministrator()
		);
	}
}
