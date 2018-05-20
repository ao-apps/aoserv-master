/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SslCertificate;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final public class SslCertificateHandler {

	private SslCertificateHandler() {
	}

	public static void checkAccessSslCertificate(DatabaseConnection conn, RequestSource source, String action, int sslCertificate) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu != null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length != 0) {
				int aoServer = getAOServerForSslCertificate(conn, sslCertificate);
				ServerHandler.checkAccessServer(conn, source, action, aoServer);
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForSslCertificate(conn, sslCertificate));
		}
	}

	public static AccountingCode getPackageForSslCertificate(DatabaseConnection conn, int sslCertificate) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from ssl_certificates where pkey=?",
			sslCertificate
		);
	}

	public static int getAOServerForSslCertificate(DatabaseConnection conn, int sslCertificate) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select ao_server from ssl_certificates where pkey=?",
			sslCertificate
		);
	}

	public static List<SslCertificate.Check> check(
		DatabaseConnection conn,
		RequestSource source,
		int sslCertificate
	) throws IOException, SQLException {
		// Check access
		checkAccessSslCertificate(conn, source, "check", sslCertificate);
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForSslCertificate(conn, sslCertificate)
		).checkSslCertificate(sslCertificate);
	}
}
