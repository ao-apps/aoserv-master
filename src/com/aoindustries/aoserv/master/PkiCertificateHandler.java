/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.pki.Certificate;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final public class PkiCertificateHandler {

	private PkiCertificateHandler() {
	}

	public static void checkAccessCertificate(DatabaseConnection conn, RequestSource source, String action, int certificate) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu != null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
				int linuxServer = getLinuxServerForCertificate(conn, certificate);
				NetHostHandler.checkAccessHost(conn, source, action, linuxServer);
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForCertificate(conn, certificate));
		}
	}

	public static Account.Name getPackageForCertificate(DatabaseConnection conn, int certificate) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select package from pki.\"Certificate\" where id=?",
			certificate
		);
	}

	public static int getLinuxServerForCertificate(DatabaseConnection conn, int certificate) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select ao_server from pki.\"Certificate\" where id=?",
			certificate
		);
	}

	public static List<Certificate.Check> check(
		DatabaseConnection conn,
		RequestSource source,
		int certificate
	) throws IOException, SQLException {
		// Check access
		checkAccessCertificate(conn, source, "check", certificate);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
			conn,
			getLinuxServerForCertificate(conn, certificate)
		);
		conn.releaseConnection();
		return daemonConnector.checkSslCertificate(certificate);
	}
}
