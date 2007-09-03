package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>CreditCardHandler</code> handles all the accesses to the <code>credit_cards</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class CreditCardHandler {

    public static void checkAccessCreditCard(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, CreditCardHandler.class, "checkAccessCreditCard(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            BusinessHandler.checkPermission(conn, source, action, AOServPermission.Permission.get_credit_cards);
            BusinessHandler.checkAccessBusiness(
                conn,
                source,
                action,
                getBusinessForCreditCard(conn, pkey)
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessCreditCardProcessor(MasterDatabaseConnection conn, RequestSource source, String action, String processor) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, CreditCardHandler.class, "checkAccessCreditCardProcessor(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            BusinessHandler.checkAccessBusiness(
                conn,
                source,
                action,
                getBusinessForCreditCardProcessor(conn, processor)
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessCreditCardTransaction(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, CreditCardHandler.class, "checkAccessCreditCardTransaction(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            checkAccessCreditCardProcessor(
                conn,
                source,
                action,
                getCreditCardProcessorForCreditCardTransaction(conn, pkey)
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessEncryptionKey(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, CreditCardHandler.class, "checkAccessEncryptionKey(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            BusinessHandler.checkAccessBusiness(
                conn,
                source,
                action,
                getBusinessForEncryptionKey(conn, pkey)
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Creates a new <code>CreditCard</code>.
     */
    public static int addCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String processorName,
        String accounting,
        String groupName,
        String cardInfo,
        String providerUniqueId,
        String firstName,
        String lastName,
        String companyName,
        String email,
        String phone,
        String fax,
        String customerTaxId,
        String streetAddress1,
        String streetAddress2,
        String city,
        String state,
        String postalCode,
        String countryCode,
        String principalName,
        String description
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "addCreditCard", AOServPermission.Permission.add_credit_card);
        BusinessHandler.checkAccessBusiness(conn, source, "addCreditCard", accounting);

        int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('credit_cards_pkey_seq')");

        conn.executeUpdate(
            "insert into credit_cards values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,false,true,null,null,?)",
            pkey,
            processorName,
            accounting,
            groupName,
            cardInfo,
            providerUniqueId,
            firstName,
            lastName,
            companyName,
            email,
            phone,
            fax,
            customerTaxId,
            streetAddress1,
            streetAddress2,
            city,
            state,
            postalCode,
            countryCode,
            source.getUsername(),
            principalName,
            description
        );

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.CREDIT_CARDS, accounting, InvalidateList.allServers, false);
        return pkey;
    }

    public static void creditCardDeclined(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String reason
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "creditCardDeclined(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            BankAccountHandler.checkAccounting(conn, source, "creditCardDeclined");
            checkAccessCreditCard(conn, source, "creditCardDeclined", pkey);

            conn.executeUpdate(
                "update credit_cards set active=false, deactivated_on=now(), deactivate_reason=? where credit_cards.pkey=?",
                reason,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.CREDIT_CARDS,
                CreditCardHandler.getBusinessForCreditCard(conn, pkey),
                InvalidateList.allServers,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForCreditCard(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "getBusinessForCreditCard(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select accounting from credit_cards where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForCreditCardProcessor(MasterDatabaseConnection conn, String processor) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "getBusinessForCreditCardProcessor(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery("select accounting from credit_card_processors where provider_id=?", processor);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getCreditCardProcessorForCreditCardTransaction(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "getCreditCardProcessorForCreditCardTransaction(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select processor_id from credit_card_transactions where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEncryptionKey(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "getBusinessForEncryptionKey(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select accounting from encryption_keys where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, CreditCardHandler.class, "removeCreditCard(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            BusinessHandler.checkPermission(conn, source, "removeCreditCard", AOServPermission.Permission.delete_credit_card);
            checkAccessCreditCard(conn, source, "removeCreditCard", pkey);

            removeCreditCard(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void removeCreditCard(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "removeCreditCard(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Grab values for later use
            String business=getBusinessForCreditCard(conn, pkey);

            // Update the database
            conn.executeUpdate("delete from credit_cards where pkey=?", pkey);

            invalidateList.addTable(
                conn,
                SchemaTable.TableID.CREDIT_CARDS,
                business,
                BusinessHandler.getServersForBusiness(conn, business),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void updateCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String firstName,
        String lastName,
        String companyName,
        String email,
        String phone,
        String fax,
        String customerTaxId,
        String streetAddress1,
        String streetAddress2,
        String city,
        String state,
        String postalCode,
        String countryCode,
        String description
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "updateCreditCard", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "updateCreditCard", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  first_name=?,\n"
            + "  last_name=?,\n"
            + "  company_name=?,\n"
            + "  email=?,\n"
            + "  phone=?,\n"
            + "  fax=?,\n"
            + "  customer_tax_id=?,\n"
            + "  street_address1=?,\n"
            + "  street_address2=?,\n"
            + "  city=?,\n"
            + "  state=?,\n"
            + "  postal_code=?,\n"
            + "  country_code=?,\n"
            + "  description=?\n"
            + "where\n"
            + "  pkey=?",
            firstName,
            lastName,
            companyName,
            email,
            phone,
            fax,
            customerTaxId,
            streetAddress1,
            streetAddress2,
            city,
            state,
            postalCode,
            countryCode,
            description,
            pkey
        );
        
        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void updateCreditCardCardInfo(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String cardInfo
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "updateCreditCardCardInfo", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "updateCreditCardCardInfo", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  card_info=?\n"
            + "where\n"
            + "  pkey=?",
            cardInfo,
            pkey
        );
        
        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void reactivateCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "reactivateCreditCard", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "reactivateCreditCard", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  active=true,\n"
            + "  deactivated_on=null,\n"
            + "  deactivate_reason=null\n"
            + "where\n"
            + "  pkey=?",
            pkey
        );

        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void setCreditCardUseMonthly(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        int pkey
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "setCreditCardUseMonthly", AOServPermission.Permission.edit_credit_card);

        if(pkey==-1) {
            // Clear only
            conn.executeUpdate("update credit_cards set use_monthly=false where accounting=? and use_monthly", accounting);
        } else {
            checkAccessCreditCard(conn, source, "setCreditCardUseMonthly", pkey);

            // Make sure accounting codes match
            if(!accounting.equals(getBusinessForCreditCard(conn, pkey))) throw new SQLException("credit card and business accounting codes do not match");

            // Perform clear and set in one SQL statement - I thinks myself clever right now.
            conn.executeUpdate("update credit_cards set use_monthly=(pkey=?) where accounting=? and use_monthly!=(pkey=?)", pkey, accounting, pkey);
        }

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    /**
     * Creates a new <code>CreditCardTransaction</code>.
     */
    public static int addCreditCardTransaction(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String processor,
        String accounting,
        String groupName,
        boolean testMode,
        int duplicateWindow,
        String orderNumber,
        String currencyCode,
        String amount,
        String taxAmount,
        boolean taxExempt,
        String shippingAmount,
        String dutyAmount,
        String shippingFirstName,
        String shippingLastName,
        String shippingCompanyName,
        String shippingStreetAddress1,
        String shippingStreetAddress2,
        String shippingCity,
        String shippingState,
        String shippingPostalCode,
        String shippingCountryCode,
        boolean emailCustomer,
        String merchantEmail,
        String invoiceNumber,
        String purchaseOrderNumber,
        String description,
        String creditCardCreatedBy,
        String creditCardPrincipalName,
        String creditCardAccounting,
        String creditCardGroupName,
        String creditCardProviderUniqueId,
        String creditCardMaskedCardNumber,
        String creditCardFirstName,
        String creditCardLastName,
        String creditCardCompanyName,
        String creditCardEmail,
        String creditCardPhone,
        String creditCardFax,
        String creditCardCustomerTaxId,
        String creditCardStreetAddress1,
        String creditCardStreetAddress2,
        String creditCardCity,
        String creditCardState,
        String creditCardPostalCode,
        String creditCardCountryCode,
        String creditCardComments,
        long authorizationTime,
        String authorizationPrincipalName
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "addCreditCardTransaction", AOServPermission.Permission.add_credit_card_transaction);
        checkAccessCreditCardProcessor(conn, source, "addCreditCardTransaction", processor);
        BusinessHandler.checkAccessBusiness(conn, source, "addCreditCardTransaction", accounting);
        BusinessHandler.checkAccessBusiness(conn, source, "addCreditCardTransaction", creditCardAccounting);
        UsernameHandler.checkAccessUsername(conn, source, "addCreditCardTransaction", creditCardCreatedBy);

        int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('credit_card_transactions_pkey_seq')");

        conn.executeUpdate(
            "insert into credit_card_transactions (\n"
            + "  pkey,\n"
            + "  processor_id,\n"
            + "  accounting,\n"
            + "  group_name,\n"
            + "  test_mode,\n"
            + "  duplicate_window,\n"
            + "  order_number,\n"
            + "  currency_code,\n"
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
            + "  authorization_time,\n"
            + "  authorization_username,\n"
            + "  authorization_principal_name,\n"
            + "  status\n"
            + ") values (?,?,?,?,?,?,?,?,?::decimal(9,2),?::decimal(9,2),?,?::decimal(9,2),?::decimal(9,2),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PROCESSING')",
            pkey,
            processor,
            accounting,
            groupName,
            testMode,
            duplicateWindow,
            orderNumber,
            currencyCode,
            amount,
            taxAmount,
            taxExempt,
            shippingAmount,
            dutyAmount,
            shippingFirstName,
            shippingLastName,
            shippingCompanyName,
            shippingStreetAddress1,
            shippingStreetAddress2,
            shippingCity,
            shippingState,
            shippingPostalCode,
            shippingCountryCode,
            emailCustomer,
            merchantEmail,
            invoiceNumber,
            purchaseOrderNumber,
            description,
            creditCardCreatedBy,
            creditCardPrincipalName,
            creditCardAccounting,
            creditCardGroupName,
            creditCardProviderUniqueId,
            creditCardMaskedCardNumber,
            creditCardFirstName,
            creditCardLastName,
            creditCardCompanyName,
            creditCardEmail,
            creditCardPhone,
            creditCardFax,
            creditCardCustomerTaxId,
            creditCardStreetAddress1,
            creditCardStreetAddress2,
            creditCardCity,
            creditCardState,
            creditCardPostalCode,
            creditCardCountryCode,
            creditCardComments,
            new Timestamp(authorizationTime),
            source.getUsername(),
            authorizationPrincipalName
        );

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.CREDIT_CARD_TRANSACTIONS, accounting, InvalidateList.allServers, false);
        return pkey;
    }

    public static void creditCardTransactionSaleCompleted(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String authorizationCommunicationResult,
        String authorizationProviderErrorCode,
        String authorizationErrorCode,
        String authorizationProviderErrorMessage,
        String authorizationProviderUniqueId,
        String providerApprovalResult,
        String approvalResult,
        String providerDeclineReason,
        String declineReason,
        String providerReviewReason,
        String reviewReason,
        String providerCvvResult,
        String cvvResult,
        String providerAvsResult,
        String avsResult,
        String approvalCode,
        long captureTime,
        String capturePrincipalName,
        String captureCommunicationResult,
        String captureProviderErrorCode,
        String captureErrorCode,
        String captureProviderErrorMessage,
        String captureProviderUniqueId,
        String status
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "creditCardTransactionSaleCompleted", AOServPermission.Permission.credit_card_transaction_sale_completed);
        checkAccessCreditCardTransaction(conn, source, "creditCardTransactionSaleCompleted", pkey);
        if(capturePrincipalName!=null) UsernameHandler.checkAccessUsername(conn, source, "creditCardTransactionSaleCompleted", capturePrincipalName);

        String processor = getCreditCardProcessorForCreditCardTransaction(conn, pkey);
        String accounting = getBusinessForCreditCardProcessor(conn, processor);

        int updated = conn.executeUpdate(
            "update\n"
            + "  credit_card_transactions\n"
            + "set\n"
            + "  authorization_communication_result=?,\n"
            + "  authorization_provider_error_code=?,\n"
            + "  authorization_error_code=?,\n"
            + "  authorization_provider_error_message=?,\n"
            + "  authorization_provider_unique_id=?,\n"
            + "  authorization_provider_approval_result=?,\n"
            + "  authorization_approval_result=?,\n"
            + "  authorization_provider_decline_reason=?,\n"
            + "  authorization_decline_reason=?,\n"
            + "  authorization_provider_review_reason=?,\n"
            + "  authorization_review_reason=?,\n"
            + "  authorization_provider_cvv_result=?,\n"
            + "  authorization_cvv_result=?,\n"
            + "  authorization_provider_avs_result=?,\n"
            + "  authorization_avs_result=?,\n"
            + "  authorization_approval_code=?,\n"
            + "  capture_time=?::timestamp,\n"
            + "  capture_username=?,\n"
            + "  capture_principal_name=?,\n"
            + "  capture_communication_result=?,\n"
            + "  capture_provider_error_code=?,\n"
            + "  capture_error_code=?,\n"
            + "  capture_provider_error_message=?,\n"
            + "  capture_provider_unique_id=?,\n"
            + "  status=?\n"
            + "where\n"
            + "  pkey=?\n"
            + "  and status='PROCESSING'",
            authorizationCommunicationResult,
            authorizationProviderErrorCode,
            authorizationErrorCode,
            authorizationProviderErrorMessage,
            authorizationProviderUniqueId,
            providerApprovalResult,
            approvalResult,
            providerDeclineReason,
            declineReason,
            providerReviewReason,
            reviewReason,
            providerCvvResult,
            cvvResult,
            providerAvsResult,
            avsResult,
            approvalCode,
            captureTime==0 ? null : new Timestamp(captureTime),
            source.getUsername(),
            capturePrincipalName,
            captureCommunicationResult,
            captureProviderErrorCode,
            captureErrorCode,
            captureProviderErrorMessage,
            captureProviderUniqueId,
            status,
            pkey
        );
        if(updated!=1) throw new SQLException("Unable to find credit_card_transactions with pkey="+pkey+" and status='PROCESSING'");

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.CREDIT_CARD_TRANSACTIONS, accounting, InvalidateList.allServers, false);
    }
}
