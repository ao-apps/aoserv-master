/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.email.Domain;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.LogFactory;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.billing.WhoisHistoryDomainLocator;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
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
public class DomainService implements MasterService, WhoisHistoryDomainLocator {

	private static final Logger logger = LogFactory.getLogger(DomainService.class);

	// <editor-fold desc="GetTableHandler" defaultstate="collapsed">
	@Override
	public TableHandler.GetTableHandler startGetTableHandler() {
		return new TableHandler.GetTableHandlerByRole() {
			@Override
			public Set<Table.TableID> getTableIds() {
				return EnumSet.of(Table.TableID.EMAIL_DOMAINS);
			}

			@Override
			protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.AUTO,
					new Domain(),
					"select * from email.\"Domain\""
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
					new Domain(),
					"select\n"
					+ "  ed.*\n"
					+ "from\n"
					+ "  master.\"UserHost\" ms,\n"
					+ "  email.\"Domain\" ed\n"
					+ "where\n"
					+ "  ms.username=?\n"
					+ "  and ms.server=ed.ao_server",
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
					new Domain(),
					"select\n"
					+ "  ed.*\n"
					+ "from\n"
					+ "  account.\"Username\" un,\n"
					+ "  billing.\"Package\" pk1,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"Package\" pk2,\n"
					+ "  email.\"Domain\" ed\n"
					+ "where\n"
					+ "  un.username=?\n"
					+ "  and un.package=pk1.name\n"
					+ "  and (\n"
					+ TableHandler.PK1_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  and bu1.accounting=pk2.accounting\n"
					+ "  and pk2.name=ed.package",
					source.getUsername()
				);
			}
		};
	}
	// </editor-fold>

	// <editor-fold desc="WhoisHistoryDomainLocator" defaultstate="collapsed">
	@Override
	public Map<DomainName,Set<AccountingCode>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException {
		List<DomainName> tlds = MasterServer.getService(DnsService.class).getDNSTLDs(conn);
		return conn.executeQuery(
			(ResultSet results) -> {
				try {
					Map<DomainName,Set<AccountingCode>> map = new HashMap<>();
					while(results.next()) {
						DomainName domain = DomainName.valueOf(results.getString(1));
						AccountingCode accounting = AccountingCode.valueOf(results.getString(2));
						DomainName registrableDomain;
						try {
							registrableDomain = ZoneTable.getHostTLD(domain, tlds);
						} catch(IllegalArgumentException err) {
							logger.log(Level.WARNING, "Cannot find TLD, continuing verbatim", err);
							registrableDomain = domain;
						}
						Set<AccountingCode> accounts = map.get(registrableDomain);
						if(accounts == null) map.put(registrableDomain, accounts = new LinkedHashSet<>());
						accounts.add(accounting);
					}
					return map;
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			},
			"SELECT DISTINCT\n"
			+ "  ed.\"domain\",\n"
			+ "  pk.accounting\n"
			+ "FROM\n"
			+ "  email.\"Domain\" ed\n"
			+ "  INNER JOIN billing.\"Package\" pk ON ed.package = pk.\"name\""
		);
	}
	// </editor-fold>
}
