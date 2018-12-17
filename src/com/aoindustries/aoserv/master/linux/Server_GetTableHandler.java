/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.linux;

import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Server_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.AO_SERVERS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Server(),
			"select\n"
			+ "  server,\n"
			+ "  hostname,\n"
			+ "  daemon_bind,\n"
			+ "  daemon_key,\n"
			+ "  pool_size,\n"
			+ "  distro_hour,\n"
			+ "  last_distro_time,\n"
			+ "  failover_server,\n"
			+ "  \"daemonDeviceId\",\n"
			+ "  daemon_connect_bind,\n"
			+ "  time_zone,\n"
			+ "  jilter_bind,\n"
			+ "  restrict_outbound_email,\n"
			+ "  daemon_connect_address,\n"
			+ "  failover_batch_size,\n"
			+ "  monitoring_load_low,\n"
			+ "  monitoring_load_medium,\n"
			+ "  monitoring_load_high,\n"
			+ "  monitoring_load_critical,\n"
			+ "  \"uidMin\",\n"
			+ "  \"gidMin\",\n"
			+ "  \"uidMax\",\n"
			+ "  \"gidMax\",\n"
			+ "  \"lastUid\",\n"
			+ "  \"lastGid\",\n"
			+ "  sftp_umask\n"
			+ "from\n"
			+ "  linux.\"Server\""
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
			new Server(),
			"select distinct\n"
			+ "  ao2.server,\n"
			+ "  ao2.hostname,\n"
			+ "  ao2.daemon_bind,\n"
			+ "  ao2.daemon_key,\n"
			+ "  ao2.pool_size,\n"
			+ "  ao2.distro_hour,\n"
			+ "  ao2.last_distro_time,\n"
			+ "  ao2.failover_server,\n"
			+ "  ao2.\"daemonDeviceId\",\n"
			+ "  ao2.daemon_connect_bind,\n"
			+ "  ao2.time_zone,\n"
			+ "  ao2.jilter_bind,\n"
			+ "  ao2.restrict_outbound_email,\n"
			+ "  ao2.daemon_connect_address,\n"
			+ "  ao2.failover_batch_size,\n"
			+ "  ao2.monitoring_load_low,\n"
			+ "  ao2.monitoring_load_medium,\n"
			+ "  ao2.monitoring_load_high,\n"
			+ "  ao2.monitoring_load_critical,\n"
			+ "  ao2.\"uidMin\",\n"
			+ "  ao2.\"gidMin\",\n"
			+ "  ao2.\"uidMax\",\n"
			+ "  ao2.\"gidMax\",\n"
			+ "  ao2.\"lastUid\",\n"
			+ "  ao2.\"lastGid\",\n"
			+ "  ao2.sftp_umask\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join linux.\"Server\" ao on ms.server=ao.server\n"
			// Allow its failover parent
			+ "  left join linux.\"Server\" ff on ao.failover_server=ff.server\n"
			// Allow its failover children
			+ "  left join linux.\"Server\" fs on ao.server=fs.failover_server\n"
			// Allow servers it replicates to
			+ "  left join backup.\"FileReplication\" ffr on ms.server=ffr.server\n"
			+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
			+ "  linux.\"Server\" ao2\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			// Allow direct access
			+ "    ms.server=ao2.server\n"
			// Allow its failover parent
			+ "    or ff.server=ao2.server\n"
			// Allow its failover children
			+ "    or fs.server=ao2.server\n"
			// Allow servers it replicates to
			+ "    or bp.ao_server=ao2.server\n"
			+ "  )",
			source.getUsername()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Server(),
			"select distinct\n"
			+ "  ao.server,\n"
			+ "  ao.hostname,\n"
			+ "  ao.daemon_bind,\n"
			+ "  ?,\n"
			+ "  ao.pool_size,\n"
			+ "  ao.distro_hour,\n"
			+ "  ao.last_distro_time,\n"
			+ "  ao.failover_server,\n"
			+ "  ao.\"daemonDeviceId\",\n"
			+ "  ao.daemon_connect_bind,\n"
			+ "  ao.time_zone,\n"
			+ "  ao.jilter_bind,\n"
			+ "  ao.restrict_outbound_email,\n"
			+ "  ao.daemon_connect_address,\n"
			+ "  ao.failover_batch_size,\n"
			+ "  ao.monitoring_load_low,\n"
			+ "  ao.monitoring_load_medium,\n"
			+ "  ao.monitoring_load_high,\n"
			+ "  ao.monitoring_load_critical,\n"
			+ "  ao.\"uidMin\",\n"
			+ "  ao.\"gidMin\",\n"
			+ "  ao.\"uidMax\",\n"
			+ "  ao.\"gidMax\",\n"
			+ "  ao.\"lastUid\",\n"
			+ "  ao.\"lastGid\",\n"
			+ "  ao.sftp_umask\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			// Allow servers it replicates to
			//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
			//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
			+ "  linux.\"Server\" ao\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    bs.server=ao.server\n"
			// Allow servers it replicates to
			//+ "    or bp.ao_server=ao.server\n"
			+ "  )",
			AoservProtocol.FILTERED,
			source.getUsername()
		);
	}
}
