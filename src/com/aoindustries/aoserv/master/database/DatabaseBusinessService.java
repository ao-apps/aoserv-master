/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessService extends DatabaseAccountTypeService<AccountingCode,Business> implements BusinessService {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<Business> objectFactory = new AutoObjectFactory<Business>(Business.class, connector);

    DatabaseBusinessService(DatabaseConnector connector) {
        super(connector, AccountingCode.class, Business.class);
    }

    @Override
    protected List<Business> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Business>(),
            objectFactory,
            "select\n"
            + "  accounting,\n"
            + "  contract_version,\n"
            + "  (extract(epoch from created)*1000)::int8 as created,\n"
            + "  (extract(epoch from canceled)*1000)::int8 as canceled,\n"
            + "  cancel_reason,\n"
            + "  parent,\n"
            + "  can_add_backup_server,\n"
            + "  can_add_businesses,\n"
            + "  can_see_prices,\n"
            + "  disable_log,\n"
            + "  do_not_disable_reason,\n"
            + "  auto_enable,\n"
            + "  bill_parent,\n"
            + "  package_definition,\n"
            + "  created_by,\n"
            + "  email_in_burst,\n"
            + "  email_in_rate,\n"
            + "  email_out_burst,\n"
            + "  email_out_rate,\n"
            + "  email_relay_burst,\n"
            + "  email_relay_rate\n"
            + "from\n"
            + "  businesses"
        );
    }

    @Override
    protected List<Business> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Business>(),
            objectFactory,
            "select distinct\n"
            + "  bu.accounting,\n"
            + "  bu.contract_version,\n"
            + "  (extract(epoch from bu.created)*1000)::int8 as created,\n"
            + "  (extract(epoch from bu.canceled)*1000)::int8 as canceled,\n"
            + "  bu.cancel_reason,\n"
            + "  bu.parent,\n"
            + "  bu.can_add_backup_server,\n"
            + "  bu.can_add_businesses,\n"
            + "  bu.can_see_prices,\n"
            + "  bu.disable_log,\n"
            + "  bu.do_not_disable_reason,\n"
            + "  bu.auto_enable,\n"
            + "  bu.bill_parent,\n"
            + "  bu.package_definition,\n"
            + "  bu.created_by,\n"
            + "  bu.email_in_burst,\n"
            + "  bu.email_in_rate,\n"
            + "  bu.email_out_burst,\n"
            + "  bu.email_out_rate,\n"
            + "  bu.email_relay_burst,\n"
            + "  bu.email_relay_rate\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  businesses bu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=bu.accounting",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<Business> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Business>(),
            objectFactory,
            "select\n"
            + "  bu1.accounting,\n"
            + "  bu1.contract_version,\n"
            + "  (extract(epoch from bu1.created)*1000)::int8 as created,\n"
            + "  (extract(epoch from bu1.canceled)*1000)::int8 as canceled,\n"
            + "  bu1.cancel_reason,\n"
            + "  bu1.parent,\n"
            + "  bu1.can_add_backup_server,\n"
            + "  bu1.can_add_businesses,\n"
            + "  bu1.can_see_prices,\n"
            + "  bu1.disable_log,\n"
            + "  bu1.do_not_disable_reason,\n"
            + "  bu1.auto_enable,\n"
            + "  bu1.bill_parent,\n"
            + "  bu1.package_definition,\n"
            + "  bu1.created_by,\n"
            + "  bu1.email_in_burst,\n"
            + "  bu1.email_in_rate,\n"
            + "  bu1.email_out_burst,\n"
            + "  bu1.email_out_rate,\n"
            + "  bu1.email_relay_burst,\n"
            + "  bu1.email_relay_rate\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN_NO_COMMA
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )",
            connector.getSwitchUser()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void cancelBusiness(DatabaseConnection db, InvalidateSet invalidateSet, AccountingCode accountingCode, String reason) throws RemoteException, SQLException {
        throw new RemoteException("TODO: Implement command");
        /*
        // Cascade to specific account types
        Username un = connector.factory.rootConnector.getUsernames().get(command.getUsername());
        for(AOServObject<?,?> dependent : un.getDependentObjects()) {
            if(dependent instanceof PasswordProtected) {
                ((PasswordProtected)dependent).getSetPasswordCommand(command.getPlaintext()).execute(connector, isInteractive);
            }
        }

        // Update the database
        conn.executeUpdate(
            "update businesses set canceled=now(), cancel_reason=? where accounting=?",
            cancelReason,
            accounting
        );

        // Notify the clients
        invalidateList.addTable(conn, SchemaTable.TableID.BUSINESSES, accounting, getServersForBusiness(conn, accounting), false);
         */
    }
    // </editor-fold>
}
