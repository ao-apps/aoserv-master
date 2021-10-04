/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.pki.Certificate;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
public final class PkiCertificateHandler {

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
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select package from pki.\"Certificate\" where id=?",
			certificate
		);
	}

	public static int getLinuxServerForCertificate(DatabaseConnection conn, int certificate) throws IOException, SQLException {
		return conn.queryInt(
			"select ao_server from pki.\"Certificate\" where id=?",
			certificate
		);
	}

	public static List<Certificate.Check> check(
		DatabaseConnection conn,
		RequestSource source,
		int certificate,
		boolean allowCached
	) throws IOException, SQLException {
		// Check access
		checkAccessCertificate(conn, source, "check", certificate);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
			conn,
			getLinuxServerForCertificate(conn, certificate)
		);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.checkSslCertificate(certificate, allowCached);
	}
}
