/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.billing.PackageDefinitionLimit;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.BusinessHandler;
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
public class PackageDefinitionLimit_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.PACKAGE_DEFINITION_LIMITS);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new PackageDefinitionLimit(),
			"select * from billing.\"PackageDefinitionLimit\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new PackageDefinitionLimit(),
			"select distinct\n"
			+ "  pdl.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  billing.\"PackageDefinitionLimit\" pdl\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=bs.server\n"
			+ "  and bs.accounting=pk.accounting\n"
			+ "  and pk.package_definition=pdl.package_definition",
			source.getUsername()
		);
	}

	@Override
	public void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		if(BusinessHandler.canSeePrices(conn, source)) {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				new PackageDefinitionLimit(),
				"select distinct\n"
				+ "  pdl.*\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  billing.\"Package\" pk2,\n"
				+ "  billing.\"PackageDefinition\" pd,\n"
				+ "  billing.\"PackageDefinitionLimit\" pdl\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and bu1.accounting=pk2.accounting\n"
				+ "  and (\n"
				+ "    pk2.package_definition=pd.id\n"
				+ "    or bu1.accounting=pd.accounting\n"
				+ "  ) and pd.id=pdl.package_definition",
				source.getUsername()
			);
		} else {
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				new PackageDefinitionLimit(),
				"select distinct\n"
				+ "  pdl.id,\n"
				+ "  pdl.package_definition,\n"
				+ "  pdl.resource,\n"
				+ "  pdl.soft_limit,\n"
				+ "  pdl.hard_limit,\n"
				+ "  null,\n"
				+ "  null\n"
				+ "from\n"
				+ "  account.\"Username\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  billing.\"Package\" pk2,\n"
				+ "  billing.\"PackageDefinition\" pd,\n"
				+ "  billing.\"PackageDefinitionLimit\" pdl\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and bu1.accounting=pk2.accounting\n"
				+ "  and (\n"
				+ "    pk2.package_definition=pd.id\n"
				+ "    or bu1.accounting=pd.accounting\n"
				+ "  ) and pd.id=pdl.package_definition",
				source.getUsername()
			);
		}
	}
}