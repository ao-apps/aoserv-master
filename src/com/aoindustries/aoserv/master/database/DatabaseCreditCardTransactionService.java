/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCreditCardTransactionService extends DatabaseAccountTypeService<Integer,CreditCardTransaction> implements CreditCardTransactionService {

    private final ObjectFactory<CreditCardTransaction> objectFactory = new ObjectFactory<CreditCardTransaction>() {
        @Override
        public CreditCardTransaction createObject(ResultSet result) throws SQLException {
            try {
                return new CreditCardTransaction(
                    connector,
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
                    Email.valueOf(result.getString("merchant_email")),
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
                    Email.valueOf(result.getString("credit_card_email")),
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
                    result.getLong("authorization_time"),
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
                    (Long)result.getObject("capture_time"),
                    UserId.valueOf(result.getString("capture_username")),
                    result.getString("capture_principal_name"),
                    result.getString("capture_communication_result"),
                    result.getString("capture_provider_error_code"),
                    result.getString("capture_error_code"),
                    result.getString("capture_provider_error_message"),
                    result.getString("capture_provider_unique_id"),
                    (Long)result.getObject("void_time"),
                    UserId.valueOf(result.getString("void_username")),
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
    protected ArrayList<CreditCardTransaction> getListMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).hasPermission(AOServPermission.Permission.get_credit_card_transactions)) {
            return db.executeObjectCollectionQuery(
                new ArrayList<CreditCardTransaction>(),
                objectFactory,
                "select\n"
                + "  pkey,\n"
                + "  processor_id,\n"
                + "  accounting,\n"
                + "  group_name,\n"
                + "  test_mode,\n"
                + "  duplicate_window,\n"
                + "  order_number,\n"
                + "  currency,\n"
                + "  amount,\n"
                + "  tax_amount,\n"
                + "  tax_exempt,\n"
                + "  shipping_amount,\n"
                + "  duty_amount,\n"
                + "  shipping_first_name,\n"
                + "  shipping_last_name,\n"
                + "  shipping_company_name,\n"
                + "  shipping_street_address1,\n"
                + "  shipping_street_address2,\n"
                + "  shipping_city,\n"
                + "  shipping_state,\n"
                + "  shipping_postal_code,\n"
                + "  shipping_country_code,\n"
                + "  email_customer,\n"
                + "  merchant_email,\n"
                + "  invoice_number,\n"
                + "  purchase_order_number,\n"
                + "  description,\n"
                + "  credit_card_created_by,\n"
                + "  credit_card_principal_name,\n"
                + "  credit_card_accounting,\n"
                + "  credit_card_group_name,\n"
                + "  credit_card_provider_unique_id,\n"
                + "  credit_card_masked_card_number,\n"
                + "  credit_card_first_name,\n"
                + "  credit_card_last_name,\n"
                + "  credit_card_company_name,\n"
                + "  credit_card_email,\n"
                + "  credit_card_phone,\n"
                + "  credit_card_fax,\n"
                + "  credit_card_customer_tax_id,\n"
                + "  credit_card_street_address1,\n"
                + "  credit_card_street_address2,\n"
                + "  credit_card_city,\n"
                + "  credit_card_state,\n"
                + "  credit_card_postal_code,\n"
                + "  credit_card_country_code,\n"
                + "  credit_card_comments,\n"
                + "  (extract(epoch from authorization_time)*1000)::int8 as authorization_time,\n"
                + "  authorization_username,\n"
                + "  authorization_principal_name,\n"
                + "  authorization_communication_result,\n"
                + "  authorization_provider_error_code,\n"
                + "  authorization_error_code,\n"
                + "  authorization_provider_error_message,\n"
                + "  authorization_provider_unique_id,\n"
                + "  authorization_provider_approval_result,\n"
                + "  authorization_approval_result,\n"
                + "  authorization_provider_decline_reason,\n"
                + "  authorization_decline_reason,\n"
                + "  authorization_provider_review_reason,\n"
                + "  authorization_review_reason,\n"
                + "  authorization_provider_cvv_result,\n"
                + "  authorization_cvv_result,\n"
                + "  authorization_provider_avs_result,\n"
                + "  authorization_avs_result,\n"
                + "  authorization_approval_code,\n"
                + "  (extract(epoch from capture_time)*1000)::int8 as capture_time,\n"
                + "  capture_username,\n"
                + "  capture_principal_name,\n"
                + "  capture_communication_result,\n"
                + "  capture_provider_error_code,\n"
                + "  capture_error_code,\n"
                + "  capture_provider_error_message,\n"
                + "  capture_provider_unique_id,\n"
                + "  (extract(epoch from void_time)*1000)::int8 as void_time,\n"
                + "  void_username,\n"
                + "  void_principal_name,\n"
                + "  void_communication_result,\n"
                + "  void_provider_error_code,\n"
                + "  void_error_code,\n"
                + "  void_provider_error_message,\n"
                + "  void_provider_unique_id,\n"
                + "  status\n"
                + "from\n"
                + "  credit_card_transactions"
            );
        } else {
            return new ArrayList<CreditCardTransaction>(0);
        }
    }

    @Override
    protected ArrayList<CreditCardTransaction> getListDaemon(DatabaseConnection db) {
        return new ArrayList<CreditCardTransaction>(0);
    }

    @Override
    protected ArrayList<CreditCardTransaction> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).hasPermission(AOServPermission.Permission.get_credit_card_transactions)) {
            return db.executeObjectCollectionQuery(
                new ArrayList<CreditCardTransaction>(),
                objectFactory,
                "select\n"
                + "  cct.pkey,\n"
                + "  cct.processor_id,\n"
                + "  cct.accounting,\n"
                + "  cct.group_name,\n"
                + "  cct.test_mode,\n"
                + "  cct.duplicate_window,\n"
                + "  cct.order_number,\n"
                + "  cct.currency,\n"
                + "  cct.amount,\n"
                + "  cct.tax_amount,\n"
                + "  cct.tax_exempt,\n"
                + "  cct.shipping_amount,\n"
                + "  cct.duty_amount,\n"
                + "  cct.shipping_first_name,\n"
                + "  cct.shipping_last_name,\n"
                + "  cct.shipping_company_name,\n"
                + "  cct.shipping_street_address1,\n"
                + "  cct.shipping_street_address2,\n"
                + "  cct.shipping_city,\n"
                + "  cct.shipping_state,\n"
                + "  cct.shipping_postal_code,\n"
                + "  cct.shipping_country_code,\n"
                + "  cct.email_customer,\n"
                + "  cct.merchant_email,\n"
                + "  cct.invoice_number,\n"
                + "  cct.purchase_order_number,\n"
                + "  cct.description,\n"
                + "  cct.credit_card_created_by,\n"
                + "  cct.credit_card_principal_name,\n"
                + "  cct.credit_card_accounting,\n"
                + "  cct.credit_card_group_name,\n"
                + "  cct.credit_card_provider_unique_id,\n"
                + "  cct.credit_card_masked_card_number,\n"
                + "  cct.credit_card_first_name,\n"
                + "  cct.credit_card_last_name,\n"
                + "  cct.credit_card_company_name,\n"
                + "  cct.credit_card_email,\n"
                + "  cct.credit_card_phone,\n"
                + "  cct.credit_card_fax,\n"
                + "  cct.credit_card_customer_tax_id,\n"
                + "  cct.credit_card_street_address1,\n"
                + "  cct.credit_card_street_address2,\n"
                + "  cct.credit_card_city,\n"
                + "  cct.credit_card_state,\n"
                + "  cct.credit_card_postal_code,\n"
                + "  cct.credit_card_country_code,\n"
                + "  cct.credit_card_comments,\n"
                + "  (extract(epoch from cct.authorization_time)*1000)::int8 as authorization_time,\n"
                + "  cct.authorization_username,\n"
                + "  cct.authorization_principal_name,\n"
                + "  cct.authorization_communication_result,\n"
                + "  cct.authorization_provider_error_code,\n"
                + "  cct.authorization_error_code,\n"
                + "  cct.authorization_provider_error_message,\n"
                + "  cct.authorization_provider_unique_id,\n"
                + "  cct.authorization_provider_approval_result,\n"
                + "  cct.authorization_approval_result,\n"
                + "  cct.authorization_provider_decline_reason,\n"
                + "  cct.authorization_decline_reason,\n"
                + "  cct.authorization_provider_review_reason,\n"
                + "  cct.authorization_review_reason,\n"
                + "  cct.authorization_provider_cvv_result,\n"
                + "  cct.authorization_cvv_result,\n"
                + "  cct.authorization_provider_avs_result,\n"
                + "  cct.authorization_avs_result,\n"
                + "  cct.authorization_approval_code,\n"
                + "  (extract(epoch from cct.capture_time)*1000)::int8 as capture_time,\n"
                + "  cct.capture_username,\n"
                + "  cct.capture_principal_name,\n"
                + "  cct.capture_communication_result,\n"
                + "  cct.capture_provider_error_code,\n"
                + "  cct.capture_error_code,\n"
                + "  cct.capture_provider_error_message,\n"
                + "  cct.capture_provider_unique_id,\n"
                + "  (extract(epoch from cct.void_time)*1000)::int8 as void_time,\n"
                + "  cct.void_username,\n"
                + "  cct.void_principal_name,\n"
                + "  cct.void_communication_result,\n"
                + "  cct.void_provider_error_code,\n"
                + "  cct.void_error_code,\n"
                + "  cct.void_provider_error_message,\n"
                + "  cct.void_provider_unique_id,\n"
                + "  cct.status\n"
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
            return new ArrayList<CreditCardTransaction>(0);
        }
    }
}
