/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.AppProtocol;
import com.aoindustries.aoserv.client.net.Bind;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import static com.aoindustries.aoserv.master.TableHandler.BU2_PARENTS_JOIN;
import static com.aoindustries.aoserv.master.TableHandler.PK3_BU2_PARENTS_WHERE;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
// TODO: Only do inner joins for open_firewall for clients 1.80.2 and older?
public class Bind_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.NET_BINDS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Bind(),
			"select\n"
			+ "  nb.*,\n"
			// Protocol conversion
			+ "  (\n"
			+ "    select\n"
			+ "      nbfz.id\n"
			+ "    from\n"
			+ "      net.\"BindFirewallZone\" nbfz\n"
			+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
			+ "    where\n"
			+ "      nb.id=nbfz.net_bind\n"
			+ "      and fz.\"name\"=?\n"
			+ "  ) is not null as open_firewall\n"
			+ "from\n"
			+ "  net.\"Bind\" nb",
			FirewallZone.PUBLIC
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
			new Bind(),
			"select\n"
			+ "  nb.id,\n"
			+ "  nb.package,\n"
			+ "  nb.server,\n"
			+ "  nb.\"ipAddress\",\n"
			+ "  nb.port,\n"
			+ "  nb.net_protocol,\n"
			+ "  nb.app_protocol,\n"
			+ "  nb.monitoring_enabled,\n"
			+ "  case when nb.monitoring_parameters is null then null::text else ? end as monitoring_parameters,\n"
			// Protocol conversion
			+ "  (\n"
			+ "    select\n"
			+ "      nbfz.id\n"
			+ "    from\n"
			+ "      net.\"BindFirewallZone\" nbfz\n"
			+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
			+ "    where\n"
			+ "      nb.id=nbfz.net_bind\n"
			+ "      and fz.\"name\"=?\n"
			+ "  ) is not null as open_firewall\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  net.\"Bind\" nb\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			+ "    ms.server=nb.server\n"
			+ "    or (\n"
			+ "      select\n"
			+ "        ffr.id\n"
			+ "      from\n"
			+ "        backup.\"FileReplication\" ffr\n"
			+ "        inner join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id\n"
			+ "      where\n"
			+ "        ms.server=ffr.server\n"
			+ "        and bp.ao_server=nb.server\n"
			+ "        and nb.app_protocol in (?,?)\n"
			+ "      limit 1\n"
			+ "    ) is not null\n"
			+ "  )",
			AoservProtocol.FILTERED,
			FirewallZone.PUBLIC,
			source.getCurrentAdministrator(),
			AppProtocol.AOSERV_DAEMON,
			AppProtocol.AOSERV_DAEMON_SSL
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
			new Bind(),
			"select\n"
			+ "  nb.id,\n"
			+ "  nb.package,\n"
			+ "  nb.server,\n"
			+ "  nb.\"ipAddress\",\n"
			+ "  nb.port,\n"
			+ "  nb.net_protocol,\n"
			+ "  nb.app_protocol,\n"
			+ "  nb.monitoring_enabled,\n"
			+ "  case when nb.monitoring_parameters is null then null::text else ? end as monitoring_parameters,\n"
			// Protocol conversion
			+ "  (\n"
			+ "    select\n"
			+ "      nbfz.id\n"
			+ "    from\n"
			+ "      net.\"BindFirewallZone\" nbfz\n"
			+ "      inner join net.\"FirewallZone\" fz on nbfz.firewalld_zone=fz.id\n"
			+ "    where\n"
			+ "      nb.id=nbfz.net_bind\n"
			+ "      and fz.\"name\"=?\n"
			+ "  ) is not null as open_firewall\n"
			+ "from\n"
			+ "  net.\"Bind\" nb\n"
			+ "where\n"
			+ "  nb.id in (\n"
			+ "    select\n"
			+ "      nb2.id\n"
			+ "    from\n"
			+ "      account.\"User\" un1,\n"
			+ "      billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "      billing.\"Package\" pk2,\n"
			+ "      net.\"Bind\" nb2\n"
			+ "    where\n"
			+ "      un1.username=?\n"
			+ "      and un1.package=pk1.name\n"
			+ "      and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu1.accounting=pk2.accounting\n"
			+ "      and pk2.name=nb2.package\n"
			+ "  )\n"
			+ "  or nb.id in (\n"
			+ "    select\n"
			+ "      nb3.id\n"
			+ "    from\n"
			+ "      account.\"User\" un3,\n"
			+ "      billing.\"Package\" pk3,\n"
			+ BU2_PARENTS_JOIN
			+ "      billing.\"Package\" pk4,\n"
			+ "      web.\"Site\" hs,\n"
			+ "      web.\"VirtualHost\" hsb,\n"
			+ "      net.\"Bind\" nb3\n"
			+ "    where\n"
			+ "      un3.username=?\n"
			+ "      and un3.package=pk3.name\n"
			+ "      and (\n"
			+ PK3_BU2_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
			+ "      and pk4.name=hs.package\n"
			+ "      and hs.id=hsb.httpd_site\n"
			+ "      and hsb.httpd_bind=nb3.id\n"
			+ "  ) or nb.id in (\n"
			+ "    select\n"
			+ "      ms4.bind\n"
			+ "    from\n"
			+ "      account.\"User\" un4,\n"
			+ "      billing.\"Package\" pk4,\n"
			+ "      account.\"AccountHost\" bs4,\n"
			+ "      mysql.\"Server\" ms4\n"
			+ "    where\n"
			+ "      un4.username=?\n"
			+ "      and un4.package=pk4.name\n"
			+ "      and pk4.accounting=bs4.accounting\n"
			+ "      and bs4.server=ms4.ao_server\n"
			+ "  ) or nb.id in (\n"
			+ "    select\n"
			+ "      ps5.bind\n"
			+ "    from\n"
			+ "      account.\"User\" un5,\n"
			+ "      billing.\"Package\" pk5,\n"
			+ "      account.\"AccountHost\" bs5,\n"
			+ "      postgresql.\"Server\" ps5\n"
			+ "    where\n"
			+ "      un5.username=?\n"
			+ "      and un5.package=pk5.name\n"
			+ "      and pk5.accounting=bs5.accounting\n"
			+ "      and bs5.server=ps5.ao_server\n"
			/*+ "  ) or nb.id in (\n"
			// Allow net.Bind of receiving backup.FileReplication (exact package match - no tree inheritence)
			+ "    select\n"
			+ "      nb6.id\n"
			+ "    from\n"
			+ "      account.\"User\" un6,\n"
			+ "      billing.\"Package\" pk6,\n"
			+ "      net.\"Host\" se6,\n"
			+ "      backup.\"FileReplication\" ffr6,\n"
			+ "      backup.\"BackupPartition\" bp6,\n"
			+ "      net.\"Bind\" nb6\n"
			+ "    where\n"
			+ "      un6.username=?\n"
			+ "      and un6.package=pk6.name\n"
			+ "      and pk6.id=se6.package\n"
			+ "      and se6.id=ffr6.server\n"
			+ "      and ffr6.backup_partition=bp6.id\n"
			+ "      and bp6.ao_server=nb6.ao_server\n"
			+ "      and nb6.app_protocol in (?,?)\n"*/
			+ "  )",
			AoservProtocol.FILTERED,
			FirewallZone.PUBLIC,
			currentAdministrator,
			currentAdministrator,
			currentAdministrator,
			currentAdministrator//,
			//username,
			//Protocol.AOSERV_DAEMON,
			//Protocol.AOSERV_DAEMON_SSL
		);
	}
}
