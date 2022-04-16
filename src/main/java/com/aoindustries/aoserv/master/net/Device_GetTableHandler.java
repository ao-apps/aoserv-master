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

package com.aoindustries.aoserv.master.net;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.Device;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
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
