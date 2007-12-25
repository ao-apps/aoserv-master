package com.aoindustries.aoserv.master;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.creditcards.AuthorizationResult;
import com.aoindustries.creditcards.CaptureResult;
import com.aoindustries.creditcards.CreditCard;
import com.aoindustries.creditcards.PersistenceMechanism;
import com.aoindustries.creditcards.Transaction;
import com.aoindustries.creditcards.TransactionRequest;
import com.aoindustries.creditcards.TransactionResult;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Stores the information directly in the master server.  Each instance should be used only within a single database transaction.
 * For another transaction, create a new instance.
 *
 * @author  AO Industries, Inc.
 */
public class MasterPersistenceMechanism implements PersistenceMechanism {

    final private MasterDatabaseConnection conn;
    final private InvalidateList invalidateList;

    public MasterPersistenceMechanism(MasterDatabaseConnection conn, InvalidateList invalidateList) {
        this.conn = conn;
        this.invalidateList = invalidateList;
    }

    public String storeCreditCard(Principal principal, CreditCard creditCard, Locale userLocale) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    public void updateMaskedCardNumber(Principal principal, CreditCard creditCard, String maskedCardNumber, Locale userLocale) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    public void deleteCreditCard(Principal principal, CreditCard creditCard, Locale userLocale) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    private static String toString(BigDecimal amount) {
        return amount==null ? null : amount.toString();
    }

    public String insertTransaction(Principal principal, Group group, Transaction transaction, Locale userLocale) throws SQLException {
        try {
            String providerId = transaction.getProviderId();
            TransactionRequest transactionRequest = transaction.getTransactionRequest();
            CreditCard creditCard = transaction.getCreditCard();
            // Get the createdBy from the credit card persistence mechanism
            String creditCardCreatedBy = conn.executeStringQuery("select created_by from credit_cards where pkey=?::integer", creditCard.getPersistenceUniqueId());
            String ccBusiness = conn.executeStringQuery("select accounting from credit_cards where pkey=?::integer", creditCard.getPersistenceUniqueId());
            int pkey = CreditCardHandler.addCreditCardTransaction(
                conn,
                invalidateList,
                transaction.getProviderId(),
                ccBusiness,
                group==null ? null : group.getName(),
                transactionRequest.getTestMode(),
                transactionRequest.getDuplicateWindow(),
                transactionRequest.getOrderNumber(),
                transactionRequest.getCurrencyCode().name(),
                toString(transactionRequest.getAmount()),
                toString(transactionRequest.getTaxAmount()),
                transactionRequest.getTaxExempt(),
                toString(transactionRequest.getShippingAmount()),
                toString(transactionRequest.getDutyAmount()),
                transactionRequest.getShippingFirstName(),
                transactionRequest.getShippingLastName(),
                transactionRequest.getShippingCompanyName(),
                transactionRequest.getShippingStreetAddress1(),
                transactionRequest.getShippingStreetAddress2(),
                transactionRequest.getShippingCity(),
                transactionRequest.getShippingState(),
                transactionRequest.getShippingPostalCode(),
                transactionRequest.getShippingCountryCode(),
                transactionRequest.getEmailCustomer(),
                transactionRequest.getMerchantEmail(),
                transactionRequest.getInvoiceNumber(),
                transactionRequest.getPurchaseOrderNumber(),
                transactionRequest.getDescription(),
                creditCardCreatedBy,
                creditCard.getPrincipalName(),
                ccBusiness,
                creditCard.getGroupName(),
                creditCard.getProviderUniqueId(),
                creditCard.getMaskedCardNumber(),
                creditCard.getFirstName(),
                creditCard.getLastName(),
                creditCard.getCompanyName(),
                creditCard.getEmail(),
                creditCard.getPhone(),
                creditCard.getFax(),
                creditCard.getCustomerTaxId(),
                creditCard.getStreetAddress1(),
                creditCard.getStreetAddress2(),
                creditCard.getCity(),
                creditCard.getState(),
                creditCard.getPostalCode(),
                creditCard.getCountryCode(),
                creditCard.getComments(),
                System.currentTimeMillis(),
                "aoserv",
                principal==null ? null : principal.getName()
            );
            conn.commit();
            return Integer.toString(pkey);
        } catch(IOException err) {
            SQLException sqlErr = new SQLException();
            sqlErr.initCause(err);
            throw sqlErr;
        }
    }

    /**
     * Stores the results of a sale transaction:
     * <ol>
     *   <li>authorizationResult</li>
     *   <li>captureTime</li>
     *   <li>capturePrincipalName</li>
     *   <li>captureResult</li>
     *   <li>status</li>
     * </ol>
     *
     * The current status must be PROCESSING.
     */
    public void saleCompleted(Principal principal, Transaction transaction, Locale userLocale) throws SQLException {
        try {
            long currentTime = System.currentTimeMillis();
            String providerId = transaction.getProviderId();

            AuthorizationResult authorizationResult = transaction.getAuthorizationResult();
            TransactionResult.CommunicationResult authorizationCommunicationResult = authorizationResult.getCommunicationResult();
            TransactionResult.ErrorCode authorizationErrorCode = authorizationResult.getErrorCode();
            AuthorizationResult.ApprovalResult approvalResult = authorizationResult.getApprovalResult();
            AuthorizationResult.DeclineReason declineReason = authorizationResult.getDeclineReason();
            AuthorizationResult.ReviewReason reviewReason = authorizationResult.getReviewReason();
            AuthorizationResult.CvvResult cvvResult = authorizationResult.getCvvResult();
            AuthorizationResult.AvsResult avsResult = authorizationResult.getAvsResult();

            CaptureResult captureResult = transaction.getCaptureResult();
            TransactionResult.CommunicationResult captureCommunicationResult = captureResult.getCommunicationResult();
            TransactionResult.ErrorCode captureErrorCode = captureResult.getErrorCode();

            CreditCardHandler.creditCardTransactionSaleCompleted(
                conn,
                invalidateList,
                Integer.parseInt(transaction.getPersistenceUniqueId()),
                authorizationCommunicationResult==null ? null : authorizationCommunicationResult.name(),
                authorizationResult.getProviderErrorCode(),
                authorizationErrorCode==null ? null : authorizationErrorCode.name(),
                authorizationResult.getProviderErrorMessage(),
                authorizationResult.getProviderUniqueId(),
                authorizationResult.getProviderApprovalResult(),
                approvalResult==null ? null : approvalResult.name(),
                authorizationResult.getProviderDeclineReason(),
                declineReason==null ? null : declineReason.name(),
                authorizationResult.getProviderReviewReason(),
                reviewReason==null ? null : reviewReason.name(),
                authorizationResult.getProviderCvvResult(),
                cvvResult==null ? null : cvvResult.name(),
                authorizationResult.getProviderAvsResult(),
                avsResult==null ? null : avsResult.name(),
                authorizationResult.getApprovalCode(),
                transaction.getCaptureTime(),
                "aoserv",
                transaction.getCapturePrincipalName(),
                captureCommunicationResult==null ? null : captureCommunicationResult.name(),
                captureResult.getProviderErrorCode(),
                captureErrorCode==null ? null : captureErrorCode.name(),
                captureResult.getProviderErrorMessage(),
                captureResult.getProviderUniqueId(),
                transaction.getStatus().name()
            );
            conn.commit();
        } catch(IOException err) {
            SQLException sqlErr = new SQLException();
            sqlErr.initCause(err);
            throw sqlErr;
        }
    }

    public void voidCompleted(Principal principal, Transaction transaction, Locale userLocale) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }
}