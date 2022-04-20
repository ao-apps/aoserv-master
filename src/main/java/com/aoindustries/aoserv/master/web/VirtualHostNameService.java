/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master.web;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.VirtualHostName;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.aoserv.master.billing.WhoisHistoryDomainLocator;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
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
public class VirtualHostNameService implements MasterService, WhoisHistoryDomainLocator {

  private static final Logger logger = Logger.getLogger(VirtualHostNameService.class.getName());

  // <editor-fold desc="GetTableHandler" defaultstate="collapsed">
  @Override
  public TableHandler.GetTableHandler startGetTableHandler() {
    return new TableHandler.GetTableHandlerByRole() {
      @Override
      public Set<Table.TableID> getTableIds() {
        return EnumSet.of(Table.TableID.HTTPD_SITE_URLS);
      }

      @Override
      protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
        MasterServer.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.AUTO,
          new VirtualHostName(),
          "select * from web.\"VirtualHostName\""
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
          new VirtualHostName(),
          "select\n"
          + "  hsu.*\n"
          + "from\n"
          + "  master.\"UserHost\" ms,\n"
          + "  web.\"Site\" hs,\n"
          + "  web.\"VirtualHost\" hsb,\n"
          + "  web.\"VirtualHostName\" hsu\n"
          + "where\n"
          + "  ms.username=?\n"
          + "  and ms.server=hs.ao_server\n"
          + "  and hs.id=hsb.httpd_site\n"
          + "  and hsb.id=hsu.httpd_site_bind",
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
          new VirtualHostName(),
          "select\n"
          + "  hsu.*\n"
          + "from\n"
          + "  account.\"User\" un,\n"
          + "  billing.\"Package\" pk1,\n"
          + TableHandler.BU1_PARENTS_JOIN
          + "  billing.\"Package\" pk2,\n"
          + "  web.\"Site\" hs,\n"
          + "  web.\"VirtualHost\" hsb,\n"
          + "  web.\"VirtualHostName\" hsu\n"
          + "where\n"
          + "  un.username=?\n"
          + "  and un.package=pk1.name\n"
          + "  and (\n"
          + TableHandler.PK1_BU1_PARENTS_WHERE
          + "  )\n"
          + "  and bu1.accounting=pk2.accounting\n"
          + "  and pk2.name=hs.package\n"
          + "  and hs.id=hsb.httpd_site\n"
          + "  and hsb.id=hsu.httpd_site_bind",
          source.getCurrentAdministrator()
        );
      }
    };
  }
  // </editor-fold>

  // <editor-fold desc="WhoisHistoryDomainLocator" defaultstate="collapsed">
  @Override
  public Map<DomainName, Set<Account.Name>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException {
    List<DomainName> tlds = MasterServer.getService(DnsService.class).getDNSTLDs(conn);
    return conn.queryCall(
      results -> {
        try {
          Map<DomainName, Set<Account.Name>> map = new HashMap<>();
          while (results.next()) {
            DomainName hostname = DomainName.valueOf(results.getString(1));
            Account.Name account = Account.Name.valueOf(results.getString(2));
            DomainName registrableDomain;
            try {
              registrableDomain = ZoneTable.getHostTLD(hostname, tlds);
            } catch (IllegalArgumentException err) {
              logger.log(Level.WARNING, "Cannot find TLD, continuing verbatim", err);
              registrableDomain = hostname;
            }
            Set<Account.Name> accounts = map.get(registrableDomain);
            if (accounts == null) {
              map.put(registrableDomain, accounts = new LinkedHashSet<>());
            }
            accounts.add(account);
          }
          return map;
        } catch (ValidationException e) {
          throw new SQLException(e);
        }
      },
      "SELECT DISTINCT\n"
      + "  hsu.hostname,\n"
      + "  pk.accounting AS accounting\n"
      + "FROM\n"
      + "             web.\"VirtualHostName\" hsu\n"
      + "  INNER JOIN web.\"VirtualHost\"     hsb ON hsu.httpd_site_bind = hsb.id\n"
      + "  INNER JOIN web.\"Site\"            hs  ON hsb.httpd_site      =  hs.id\n"
      + "  INNER JOIN billing.\"Package\"     pk  ON  hs.package         =  pk.\"name\"\n"
      + "  INNER JOIN linux.\"Server\"        ao  ON  hs.ao_server       =  ao.server\n"
      + "WHERE\n"
      // Is not "localhost"
      + "  hsu.hostname != 'localhost'\n"
      // Is not the test URL
      + "  AND hsu.hostname != (hs.\"name\" || '.' || ao.hostname)"
    );
  }
  // </editor-fold>
}
