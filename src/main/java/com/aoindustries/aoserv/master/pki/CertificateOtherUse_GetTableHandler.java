/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.pki;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.pki.CertificateOtherUse;
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
public class CertificateOtherUse_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SSL_CERTIFICATE_OTHER_USES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new CertificateOtherUse(),
			"select * from pki.\"CertificateOtherUse\""
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
			new CertificateOtherUse(),
			"select\n"
			+ "  scou.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join pki.\"Certificate\" sc on ms.server=sc.ao_server\n"
			+ "  inner join pki.\"CertificateOtherUse\" scou on sc.id=scou.ssl_certificate\n"
			+ "where\n"
			+ "  ms.username=?",
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
			new CertificateOtherUse(),
			"select\n"
			+ "  scou.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  pki.\"Certificate\" sc,\n"
			+ "  pki.\"CertificateOtherUse\" scou\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.id=sc.package\n"
			+ "  and sc.id=scou.ssl_certificate",
			source.getCurrentAdministrator()
		);
	}
}