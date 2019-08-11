/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.pki;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.pki.CertificateName;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.LogFactory;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.billing.WhoisHistoryDomainLocator;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.net.DomainName;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author  AO Industries, Inc.
 */
public class CertificateNameService implements MasterService, WhoisHistoryDomainLocator {

	private static final Logger logger = LogFactory.getLogger(CertificateNameService.class);

	// <editor-fold desc="GetTableHandler" defaultstate="collapsed">
	@Override
	public TableHandler.GetTableHandler startGetTableHandler() {
		return new TableHandler.GetTableHandlerByRole() {
			@Override
			public Set<Table.TableID> getTableIds() {
				return EnumSet.of(Table.TableID.SSL_CERTIFICATE_NAMES);
			}

			@Override
			protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.AUTO,
					new CertificateName(),
					"select * from pki.\"CertificateName\""
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
					new CertificateName(),
					"select\n"
					+ "  scn.*\n"
					+ "from\n"
					+ "  master.\"UserHost\" ms\n"
					+ "  inner join pki.\"Certificate\" sc on ms.server=sc.ao_server\n"
					+ "  inner join pki.\"CertificateName\" scn on sc.id=scn.ssl_certificate\n"
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
					new CertificateName(),
					"select\n"
					+ "  scn.*\n"
					+ "from\n"
					+ "  account.\"User\" un,\n"
					+ "  billing.\"Package\" pk1,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"Package\" pk2,\n"
					+ "  pki.\"Certificate\" sc,\n"
					+ "  pki.\"CertificateName\" scn\n"
					+ "where\n"
					+ "  un.username=?\n"
					+ "  and un.package=pk1.name\n"
					+ "  and (\n"
					+ TableHandler.PK1_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  and bu1.accounting=pk2.accounting\n"
					+ "  and pk2.id=sc.package\n"
					+ "  and sc.id=scn.ssl_certificate",
					source.getCurrentAdministrator()
				);
			}
		};
	}
	// </editor-fold>

	// <editor-fold desc="WhoisHistoryDomainLocator" defaultstate="collapsed">
	@Override
	public Map<DomainName,Set<Account.Name>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException {
		List<DomainName> tlds = MasterServer.getService(DnsService.class).getDNSTLDs(conn);
		return conn.executeQuery(
			(ResultSet results) -> {
				try {
					Map<DomainName,Set<Account.Name>> map = new HashMap<>();
					while(results.next()) {
						DomainName domain = DomainName.valueOf(results.getString(1));
						Account.Name account = Account.Name.valueOf(results.getString(2));
						DomainName registrableDomain;
						try {
							registrableDomain = ZoneTable.getHostTLD(domain, tlds);
						} catch(IllegalArgumentException err) {
							logger.log(Level.WARNING, "Cannot find TLD, continuing verbatim", err);
							registrableDomain = domain;
						}
						Set<Account.Name> accounts = map.get(registrableDomain);
						if(accounts == null) map.put(registrableDomain, accounts = new LinkedHashSet<>());
						accounts.add(account);
					}
					return map;
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			},
			"SELECT DISTINCT\n"
			+ "  cn.domain,\n"
			+ "  pk.accounting AS accounting\n"
			+ "FROM\n"
			+ "             pki.\"CertificateName\" cn\n"
			+ "  INNER JOIN pki.\"Certificate\"     c  ON cn.ssl_certificate = c.id\n"
			+ "  INNER JOIN billing.\"Package\"     pk ON  c.package         =  pk.id\n"
			+ "WHERE\n"
			// Is not "localhost"
			+ "  cn.domain != 'localhost'"
		);
	}
	// </editor-fold>
}
