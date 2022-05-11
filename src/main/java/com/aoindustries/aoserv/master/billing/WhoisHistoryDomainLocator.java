/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2021, 2022  AO Industries, Inc.
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

package com.aoindustries.aoserv.master.billing;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.net.DomainName;
import com.aoapps.tlds.TopLevelDomain;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * Any {@link MasterService} that implements {@link WhoisHistoryDomainLocator}
 * contributes to the list of registrable domains that are monitored for billing purposes.
 *
 * @author  AO Industries, Inc.
 */
public interface WhoisHistoryDomainLocator {

  /**
   * Gets the set of all unique business accounting codes and registrable
   * domains that are subject to whois history logging.
   *
   * @see  DnsService#getTopLevelDomains(com.aoapps.dbc.DatabaseConnection)
   * @see  TopLevelDomain
   */
  Map<DomainName, Set<Account.Name>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException;
}
