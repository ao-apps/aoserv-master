/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.dns;

import com.aoindustries.aoserv.client.dns.Zone;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.billing.WhoisHistoryDomainLocator;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.DomainName;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class ZoneService implements MasterService, WhoisHistoryDomainLocator {

	// <editor-fold desc="GetTableHandler" defaultstate="collapsed">
	@Override
	public TableHandler.GetTableHandler startGetTableHandler() {
		return new TableHandler.GetTableHandlerByRole() {
			@Override
			public Set<Table.TableID> getTableIds() {
				return EnumSet.of(Table.TableID.DNS_ZONES);
			}

			private void getTableUnfiltered(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.AUTO,
					new Zone(),
					"select * from dns.\"Zone\""
				);
			}

			@Override
			protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
				getTableUnfiltered(conn, source, out, provideProgress, tableID);
			}

			@Override
			protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
				if(masterUser.isDNSAdmin()) {
					getTableUnfiltered(conn, source, out, provideProgress, tableID);
				} else {
					MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
				}
			}

			@Override
			protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.AUTO,
					new Zone(),
					"select\n"
					+ "  dz.*\n"
					+ "from\n"
					+ "  account.\"Username\" un,\n"
					+ "  billing.\"Package\" pk1,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"Package\" pk2,\n"
					+ "  dns.\"Zone\" dz\n"
					+ "where\n"
					+ "  un.username=?\n"
					+ "  and un.package=pk1.name\n"
					+ "  and (\n"
					+ TableHandler.PK1_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  and bu1.accounting=pk2.accounting\n"
					+ "  and pk2.name=dz.package",
					source.getUsername()
				);
			}
		};
	}
	// </editor-fold>

	// <editor-fold desc="WhoisHistoryDomainLocator" defaultstate="collapsed">
	@Override
	public Map<DomainName,Set<AccountingCode>> getWhoisHistoryDomains(List<DomainName> tlds, DatabaseConnection conn) throws IOException, SQLException {
		return conn.executeQuery(
			(ResultSet results) -> {
				try {
					Map<DomainName,Set<AccountingCode>> map = new HashMap<>();
					while(results.next()) {
						String zone = results.getString(1);
						// Strip any trailing period
						if(zone.endsWith(".")) zone = zone.substring(0, zone.length() - 1);
						DomainName domain = DomainName.valueOf(zone);
						AccountingCode accounting = AccountingCode.valueOf(results.getString(2));
						// We consider all in dns.Zone table as registrable and use them verbatim for whois lookups
						Set<AccountingCode> accounts = map.get(domain);
						if(accounts == null) map.put(domain, accounts = new LinkedHashSet<>());
						accounts.add(accounting);
					}
					return map;
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
			},
			"SELECT DISTINCT\n"
			+ "  dz.\"zone\",\n"
			+ "  pk.accounting\n"
			+ "FROM\n"
			+ "  dns.\"Zone\" dz\n"
			+ "  INNER JOIN billing.\"Package\" pk ON dz.package = pk.\"name\"\n"
			+ "WHERE\n"
			+ "  dz.\"zone\" NOT LIKE '%.in-addr.arpa'"
		);
	}
	// </editor-fold>
}
