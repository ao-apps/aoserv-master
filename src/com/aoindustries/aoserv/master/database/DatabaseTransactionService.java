/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.Transaction;
import com.aoindustries.aoserv.client.TransactionService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTransactionService extends DatabaseService<Integer,Transaction> implements TransactionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Transaction> objectFactory = new ObjectFactory<Transaction>() {
        @Override
        public Transaction createObject(ResultSet result) throws SQLException {
            try {
                return new Transaction(
                    DatabaseTransactionService.this,
                    result.getInt("transid"),
                    result.getTimestamp("time"),
                    AccountingCode.valueOf(result.getString("accounting")),
                    AccountingCode.valueOf(result.getString("source_accounting")),
                    UserId.valueOf(result.getString("username")),
                    result.getString("type"),
                    result.getBigDecimal("quantity"),
                    getMoney(result, "currency", "rate"),
                    result.getString("payment_type"),
                    result.getString("payment_info"),
                    result.getString("processor"),
                    (Integer)result.getObject("credit_card_transaction"),
                    Transaction.Status.valueOf(result.getString("status"))
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseTransactionService(DatabaseConnector connector) {
        super(connector, Integer.class, Transaction.class);
    }

    @Override
    protected Set<Transaction> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  transid,\n"
            + "  time,\n"
            + "  accounting,\n"
            + "  source_accounting,\n"
            + "  username,\n"
            + "  type,\n"
            + "  quantity,\n"
            + "  currency,\n"
            + "  rate,\n"
            + "  payment_type,\n"
            + "  payment_info,\n"
            + "  processor,\n"
            + "  credit_card_transaction,\n"
            + "  status\n"
            + "from\n"
            + "  transactions"
        );
    }

    @Override
    protected Set<Transaction> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<Transaction> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  tr.transid,\n"
            + "  tr.time,\n"
            + "  tr.accounting,\n"
            + "  tr.source_accounting,\n"
            + "  tr.username,\n"
            + "  tr.type,\n"
            + "  tr.quantity,\n"
            + "  tr.currency,\n"
            + "  tr.rate,\n"
            + "  tr.payment_type,\n"
            + "  tr.payment_info,\n"
            + "  tr.processor,\n"
            + "  tr.credit_card_transaction,\n"
            + "  tr.status\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  transactions tr\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=tr.accounting",
            connector.getConnectAs()
        );
    }
}
