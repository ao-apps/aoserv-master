/*
 * Copyright 2009-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>ResellerHandler</code> handles all the accesses to the reseller tables.
 *
 * @author  AO Industries, Inc.
 */
final public class ResellerHandler {

    private ResellerHandler() {
    }

    /**
     * Gets the lowest-level reseller that is at or above the provided business.
     * Will skip past resellers that are flagged as auto-escalate.
     */
    public static AccountingCode getResellerForBusinessAutoEscalate(
        DatabaseConnection conn,
        AccountingCode originalAccounting
    ) throws IOException, SQLException {
        AccountingCode accounting = originalAccounting;
        while(accounting!=null) {
            if(conn.executeBooleanQuery("select (select accounting from resellers where accounting=? and not ticket_auto_escalate) is not null", accounting)) return accounting;
            accounting = BusinessHandler.getParentBusiness(conn, accounting);
        }
        throw new SQLException("Unable to find Reseller for Business: "+originalAccounting);
    }
}