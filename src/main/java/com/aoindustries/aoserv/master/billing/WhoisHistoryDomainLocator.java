/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.master.MasterService;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.DomainName;
import com.aoindustries.tlds.TopLevelDomain;
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
	 * @see  DnsService#getDNSTLDs(com.aoindustries.dbc.DatabaseConnection)
	 * @see  TopLevelDomain
	 */
	Map<DomainName,Set<Account.Name>> getWhoisHistoryDomains(DatabaseConnection conn) throws IOException, SQLException;
}
