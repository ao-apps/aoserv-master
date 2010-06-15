/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.CreditCardTransaction;
import com.aoindustries.aoserv.client.CreditCardTransactionService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCreditCardTransactionService extends DatabaseService<Integer,CreditCardTransaction> implements CreditCardTransactionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CreditCardTransaction> objectFactory = new ObjectFactory<CreditCardTransaction>() {
        @Override
        public CreditCardTransaction createObject(ResultSet result) throws SQLException {
            try {
                return new CreditCardTransaction(
                    DatabaseCreditCardTransactionService.this,
                    result.getInt("pkey"),
                    result.getString("processor_id"),
                    AccountingCode.valueOf(result.getString("accounting")),
                    result.getString("group_name"),
                    result.getBoolean("test_mode"),
                    result.getInt("duplicate_window"),
                    result.getString("order_number"),
                    getMoney(result, "currency", "amount"),
                    getMoney(result, "currency", "tax_amount"),
                    result.getBoolean("tax_exempt"),
                    getMoney(result, "currency", "shipping_amount"),
                    getMoney(result, "currency", "duty_amount"),
                    result.getString("shipping_first_name"),
                    result.getString("shipping_last_name"),
                    result.getString("shipping_company_name"),
                    result.getString("shipping_street_address1"),
                    result.getString("shipping_street_address2"),
                    result.getString("shipping_city"),
                    result.getString("shipping_state"),
                    result.getString("shipping_postal_code"),
                    result.getString("shipping_country_code"),
                    result.getBoolean("email_customer"),
                    getEmail(result.getString("merchant_email")),
                    result.getString("invoice_number"),
                    result.getString("purchase_order_number"),
                    result.getString("description"),
                    UserId.valueOf(result.getString("credit_card_created_by")),
                    result.getString("credit_card_principal_name"),
                    AccountingCode.valueOf(result.getString("credit_card_accounting")),
                    result.getString("credit_card_group_name"),
                    result.getString("credit_card_provider_unique_id"),
                    result.getString("credit_card_masked_card_number"),
                    result.getString("credit_card_first_name"),
                    result.getString("credit_card_last_name"),
                    result.getString("credit_card_company_name"),
                    getEmail(result.getString("credit_card_email")),
                    result.getString("credit_card_phone"),
                    result.getString("credit_card_fax"),
                    result.getString("credit_card_customer_tax_id"),
                    result.getString("credit_card_street_address1"),
                    result.getString("credit_card_street_address2"),
                    result.getString("credit_card_city"),
                    result.getString("credit_card_state"),
                    result.getString("credit_card_postal_code"),
                    result.getString("credit_card_country_code"),
                    result.getString("credit_card_comments"),
                    result.getTimestamp("authorization_time"),
                    UserId.valueOf(result.getString("authorization_username")),
                    result.getString("authorization_principal_name"),
                    result.getString("authorization_communication_result"),
                    result.getString("authorization_provider_error_code"),
                    result.getString("authorization_error_code"),
                    result.getString("authorization_provider_error_message"),
                    result.getString("authorization_provider_unique_id"),
                    result.getString("authorization_provider_approval_result"),
                    result.getString("authorization_approval_result"),
                    result.getString("authorization_provider_decline_reason"),
                    result.getString("authorization_decline_reason"),
                    result.getString("authorization_provider_review_reason"),
                    result.getString("authorization_review_reason"),
                    result.getString("authorization_provider_cvv_result"),
                    result.getString("authorization_cvv_result"),
                    result.getString("authorization_provider_avs_result"),
                    result.getString("authorization_avs_result"),
                    result.getString("authorization_approval_code"),
                    result.getTimestamp("capture_time"),
                    getUserId(result.getString("capture_username")),
                    result.getString("capture_principal_name"),
                    result.getString("capture_communication_result"),
                    result.getString("capture_provider_error_code"),
                    result.getString("capture_error_code"),
                    result.getString("capture_provider_error_message"),
                    result.getString("capture_provider_unique_id"),
                    result.getTimestamp("void_time"),
                    getUserId(result.getString("void_username")),
                    result.getString("void_principal_name"),
                    result.getString("void_communication_result"),
                    result.getString("void_provider_error_code"),
                    result.getString("void_error_code"),
                    result.getString("void_provider_error_message"),
                    result.getString("void_provider_unique_id"),
                    result.getString("status")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseCreditCardTransactionService(DatabaseConnector connector) {
        super(connector, Integer.class, CreditCardTransaction.class);
    }

    @Override
    protected Set<CreditCardTransaction> getSetMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).hasPermission(AOServPermission.Permission.get_credit_card_transactions)) {
            return db.executeObjectSetQuery(
                objectFactory,
                "select * from credit_card_transactions"
            );
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Set<CreditCardTransaction> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<CreditCardTransaction> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).hasPermission(AOServPermission.Permission.get_credit_card_transactions)) {
            return db.executeObjectSetQuery(
                objectFactory,
                "select\n"
                + "  cct.*\n"
                + "from\n"
                + "  usernames un,\n"
                + BU1_PARENTS_JOIN
                + "  credit_card_transactions cct\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and (\n"
                + UN_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=cct.accounting",
                connector.getConnectAs()
            );
        } else {
            return Collections.emptySet();
        }
    }
}
