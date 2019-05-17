/*
 * Copyright 2007-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.Administrator;
import com.aoindustries.creditcards.AuthorizationResult;
import com.aoindustries.creditcards.CaptureResult;
import com.aoindustries.creditcards.CreditCard;
import com.aoindustries.creditcards.PersistenceMechanism;
import com.aoindustries.creditcards.Transaction;
import com.aoindustries.creditcards.TransactionRequest;
import com.aoindustries.creditcards.TransactionResult;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Stores the information directly in the master server.  Each instance should be used only within a single database transaction.
 * For another transaction, create a new instance.
 *
 * @author  AO Industries, Inc.
 */
public class MasterPersistenceMechanism implements PersistenceMechanism {

	/**
	 * The username used for actions taken directly by the AOServMaster
	 * but that must be associated with a {@link Administrator}.
	 */
	static final com.aoindustries.aoserv.client.account.User.Name MASTER_BUSINESS_ADMINISTRATOR;
	static {
		try {
			MASTER_BUSINESS_ADMINISTRATOR = com.aoindustries.aoserv.client.account.User.Name.valueOf("aoserv").intern();
		} catch(ValidationException e) {
			throw new AssertionError("These hard-coded values are valid", e);
		}
	}

	final private DatabaseConnection conn;
    final private InvalidateList invalidateList;

    public MasterPersistenceMechanism(DatabaseConnection conn, InvalidateList invalidateList) {
        this.conn = conn;
        this.invalidateList = invalidateList;
    }

    @Override
    public String storeCreditCard(Principal principal, CreditCard creditCard) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

	@Override
	public void updateCreditCard(Principal principal, CreditCard creditCard) throws SQLException {
		try {
            CreditCardHandler.updateCreditCard(
                conn,
                invalidateList,
				Integer.parseInt(creditCard.getPersistenceUniqueId()),
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
				creditCard.getComments()
            );
            conn.commit();
        } catch(IOException err) {
            throw new SQLException(err);
        }
	}

	@Override
    public void updateCardNumber(
		Principal principal,
		CreditCard creditCard,
		String cardNumber,
		byte expirationMonth,
		short expirationYear
	) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    @Override
    public void updateExpiration(
		Principal principal,
		CreditCard creditCard,
		byte expirationMonth,
		short expirationYear
	) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    @Override
    public void deleteCreditCard(Principal principal, CreditCard creditCard) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    @Override
    public String insertTransaction(Principal principal, Group group, Transaction transaction) throws SQLException {
        try {
            //String providerId = transaction.getProviderId();
            TransactionRequest transactionRequest = transaction.getTransactionRequest();
            CreditCard creditCard = transaction.getCreditCard();
            // Get the createdBy from the credit card persistence mechanism
            com.aoindustries.aoserv.client.account.User.Name creditCardCreatedBy = conn.executeObjectQuery(
				ObjectFactories.userNameFactory,
				"select created_by from payment.\"CreditCard\" where id=?::integer",
				creditCard.getPersistenceUniqueId()
			);
            Account.Name ccBusiness = conn.executeObjectQuery(ObjectFactories.accountNameFactory,
                "select accounting from payment.\"CreditCard\" where id=?::integer",
                creditCard.getPersistenceUniqueId()
            );
            int id = CreditCardHandler.addCreditCardTransaction(
                conn,
                invalidateList,
                transaction.getProviderId(),
                ccBusiness,
                group==null ? null : group.getName(),
                transactionRequest.getTestMode(),
                transactionRequest.getDuplicateWindow(),
                transactionRequest.getOrderNumber(),
                transactionRequest.getCurrency().getCurrencyCode(),
                Objects.toString(transactionRequest.getAmount(), null),
                Objects.toString(transactionRequest.getTaxAmount(), null),
                transactionRequest.getTaxExempt(),
                Objects.toString(transactionRequest.getShippingAmount(), null),
                Objects.toString(transactionRequest.getDutyAmount(), null),
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
                MASTER_BUSINESS_ADMINISTRATOR,
                principal==null ? null : principal.getName()
            );
            conn.commit();
            return Integer.toString(id);
        } catch(IOException err) {
            throw new SQLException(err);
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
     * The current status must be PROCESSING or AUTHORIZED.
     */
    @Override
    public void saleCompleted(Principal principal, Transaction transaction) throws SQLException {
        try {
            //long currentTime = System.currentTimeMillis();
            //String providerId = transaction.getProviderId();

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
                authorizationResult.getProviderReplacementMaskedCardNumber(),
                authorizationResult.getReplacementMaskedCardNumber(),
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
                MASTER_BUSINESS_ADMINISTRATOR,
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
            throw new SQLException(err);
        }
    }

    /**
     * Stores the results of an authorize transaction:
     * <ol>
     *   <li>authorizationResult</li>
     *   <li>status</li>
     * </ol>
     *
     * The current status must be PROCESSING.
     */
    @Override
    public void authorizeCompleted(Principal principal, Transaction transaction) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }

    @Override
    public void voidCompleted(Principal principal, Transaction transaction) throws SQLException {
        throw new SQLException("Method not implemented for direct master server persistence.");
    }
}
