/*
 * Copyright 2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.i18n.Money;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Currency;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBankTransactionService extends DatabaseBankAccountingService<Integer,BankTransaction> implements BankTransactionService {

    private final ObjectFactory<BankTransaction> objectFactory = new ObjectFactory<BankTransaction>() {
        @Override
        public BankTransaction createObject(ResultSet result) throws SQLException {
            try {
                return new BankTransaction(
                    connector,
                    result.getLong("date"),
                    result.getInt("transid"),
                    result.getString("bank_account"),
                    result.getString("processor"),
                    UserId.valueOf(result.getString("administrator")),
                    result.getString("type"),
                    result.getString("expense_code"),
                    result.getString("description"),
                    result.getString("check_no"),
                    new Money(Currency.getInstance("USD"), result.getBigDecimal("amount"))
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseBankTransactionService(DatabaseConnector connector) {
        super(connector, Integer.class, BankTransaction.class);
    }

    @Override
    protected ArrayList<BankTransaction> getListBankAccounting(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BankTransaction>(),
            objectFactory,
            "select\n"
            + "  (extract(epoch from \"date\")*1000)::int8 as \"date\",\n"
            + "  transid,\n"
            + "  bank_account,\n"
            + "  processor,\n"
            + "  administrator,\n"
            + "  type,\n"
            + "  expense_code,\n"
            + "  description,\n"
            + "  check_no,\n"
            + "  amount\n"
            + "from\n"
            + "  bank_transactions"
        );
    }
}
