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
import com.aoindustries.creditcards.TokenizedCreditCard;
import com.aoindustries.creditcards.Transaction;
import com.aoindustries.creditcards.TransactionRequest;
import com.aoindustries.creditcards.TransactionResult;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.dbc.ObjectFactory;
import com.aoindustries.util.i18n.Money;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Currency;
import java.util.LinkedHashMap;
import java.util.Map;

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

	private static final String COLUMNS =
		  "  cc.id                 AS \"persistenceUniqueId\",\n"
		+ "  cc.principal_name     AS \"principalName\",\n"
		+ "  cc.group_name         AS \"groupName\",\n"
		+ "  cc.processor_id       AS \"providerId\",\n"
		+ "  cc.provider_unique_id AS \"providerUniqueId\",\n"
		+ "  cc.card_info          AS \"maskedCardNumber\",\n"
		+ "  cc.\"expirationMonth\",\n"
		+ "  cc.\"expirationYear\",\n"
		+ "  cc.first_name         AS \"firstName\",\n"
		+ "  cc.last_name          AS \"lastName\",\n"
		+ "  cc.company_name       AS \"companyName\",\n"
		+ "  cc.email,\n"
		+ "  cc.phone,\n"
		+ "  cc.fax,\n"
		+ "  cc.\"customerId\",\n"
		+ "  cc.customer_tax_id    AS \"customerTaxId\",\n"
		+ "  cc.street_address1    AS \"streetAddress1\",\n"
		+ "  cc.street_address2    AS \"streetAddress2\",\n"
		+ "  cc.city,\n"
		+ "  cc.state,\n"
		+ "  cc.postal_code        AS \"postalCode\",\n"
		+ "  cc.country_code       AS \"countryCode\",\n"
		+ "  cc.description        AS \"comments\"";

	// TODO: 2.0: Nullable Byte
	private static byte getExpirationMonth(ResultSet result, String columnLabel) throws SQLException {
		byte expirationMonth = result.getByte(columnLabel);
		if(result.wasNull()) return CreditCard.UNKNOWN_EXPRIATION_MONTH;
		return expirationMonth;
	}

	// TODO: 2.0: Nullable Short
	private static short getExpirationYear(ResultSet result, String columnLabel) throws SQLException {
		short expirationYear = result.getShort(columnLabel);
		if(result.wasNull()) return CreditCard.UNKNOWN_EXPRIATION_YEAR;
		return expirationYear;
	}

	private static final ObjectFactory<CreditCard> creditCardObjectFactory = (ResultSet result) -> new CreditCard(
		Integer.toString(result.getInt("persistenceUniqueId")),
		result.getString("principalName"),
		result.getString("groupName"),
		result.getString("providerId"),
		result.getString("providerUniqueId"),
		null, // cardNumber
		result.getString("maskedCardNumber"),
		getExpirationMonth(result, "expirationMonth"),
		getExpirationYear(result, "expirationYear"),
		null, // cardCode
		result.getString("firstName"),
		result.getString("lastName"),
		result.getString("companyName"),
		result.getString("email"),
		result.getString("phone"),
		result.getString("fax"),
		result.getString("customerId"),
		result.getString("customerTaxId"),
		result.getString("streetAddress1"),
		result.getString("streetAddress2"),
		result.getString("city"),
		result.getString("state"),
		result.getString("postalCode"),
		result.getString("countryCode"),
		result.getString("comments")
	);

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
	public CreditCard getCreditCard(Principal principal, String persistenceUniqueId) throws SQLException {
		int creditCard;
		try {
			creditCard = Integer.parseInt(persistenceUniqueId);
		} catch(NumberFormatException e) {
			return null;
		}
		return conn.executeObjectQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			creditCardObjectFactory,
			"SELECT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "             payment.\"CreditCard\" cc\n"
			+ "  INNER JOIN payment.\"Processor\"  ccp ON cc.processor_id = ccp.provider_id\n"
			+ "WHERE\n"
			+ "  cc.id = ?\n"
			+ "  AND ccp.enabled\n"
			+ "ORDER BY\n"
			+ "  cc.accounting,\n"
			+ "  cc.created",
			creditCard
		);
	}

	@Override
	public Map<String, CreditCard> getCreditCards(Principal principal) throws SQLException {
		return conn.executeQuery(
			(ResultSet results) -> {
				Map<String, CreditCard> map = new LinkedHashMap<>();
				while(results.next()) {
					CreditCard copy = creditCardObjectFactory.createObject(results);
					String persistenceUniqueId = copy.getPersistenceUniqueId();
					if(map.put(persistenceUniqueId, copy) != null) throw new SQLException("Duplicate persistenceUniqueId: " + persistenceUniqueId);
				}
				return map;
			},
			"SELECT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "             payment.\"CreditCard\" cc\n"
			+ "  INNER JOIN payment.\"Processor\"  ccp ON cc.processor_id = ccp.provider_id\n"
			+ "WHERE\n"
			+ "  ccp.enabled\n"
			+ "ORDER BY\n"
			+ "  cc.accounting,\n"
			+ "  cc.created"
		);
	}

	@Override
	public Map<String, CreditCard> getCreditCards(Principal principal, String providerId) throws SQLException {
		return conn.executeQuery(
			(ResultSet results) -> {
				Map<String, CreditCard> map = new LinkedHashMap<>();
				while(results.next()) {
					CreditCard copy = creditCardObjectFactory.createObject(results);
					String providerUniqueId = copy.getProviderUniqueId();
					if(map.put(providerUniqueId, copy) != null) throw new SQLException("Duplicate providerUniqueId: " + providerUniqueId);
				}
				return map;
			},
			"SELECT\n"
			+ COLUMNS + "\n"
			+ "FROM\n"
			+ "             payment.\"CreditCard\" cc\n"
			+ "  INNER JOIN payment.\"Processor\"  ccp ON cc.processor_id = ccp.provider_id\n"
			+ "WHERE\n"
			+ "  cc.processor_id = ?\n"
			+ "  AND ccp.enabled\n"
			+ "ORDER BY\n"
			+ "  cc.accounting,\n"
			+ "  cc.created",
			providerId
		);
	}

	@Override
	public void updateCreditCard(Principal principal, CreditCard creditCard) throws SQLException {
		try {
            PaymentHandler.updateCreditCard(
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
				creditCard.getCustomerId(),
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
		try {
            PaymentHandler.updateCreditCardExpiration(
                conn,
                invalidateList,
				Integer.parseInt(creditCard.getPersistenceUniqueId()),
				expirationMonth,
				expirationYear
            );
            conn.commit();
        } catch(IOException err) {
            throw new SQLException(err);
        }
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
			Byte expirationMonth = creditCard.getExpirationMonth(); // TODO: 2.0: Nullable Byte
			if(expirationMonth == CreditCard.UNKNOWN_EXPRIATION_MONTH) expirationMonth = null;
			Short expirationYear = creditCard.getExpirationYear(); // TODO: 2.0: Nullable Short
			if(expirationYear == CreditCard.UNKNOWN_EXPRIATION_YEAR) expirationYear = null;
			Currency currency = transactionRequest.getCurrency();
            int payment = PaymentHandler.addPayment(
                conn,
                invalidateList,
                transaction.getProviderId(),
                ccBusiness,
                group==null ? null : group.getName(),
                transactionRequest.getTestMode(),
                transactionRequest.getDuplicateWindow(),
                transactionRequest.getOrderNumber(),
                new Money(currency, transactionRequest.getAmount()),
                transactionRequest.getTaxAmount() == null ? null : new Money(currency, transactionRequest.getTaxAmount()),
                transactionRequest.getTaxExempt(),
                transactionRequest.getShippingAmount() == null ? null : new Money(currency, transactionRequest.getShippingAmount()),
                transactionRequest.getDutyAmount() == null ? null : new Money(currency, transactionRequest.getDutyAmount()),
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
				expirationMonth,
				expirationYear,
                creditCard.getFirstName(),
                creditCard.getLastName(),
                creditCard.getCompanyName(),
                creditCard.getEmail(),
                creditCard.getPhone(),
                creditCard.getFax(),
                creditCard.getCustomerId(),
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
            return Integer.toString(payment);
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

			TokenizedCreditCard tokenizedCreditCard = authorizationResult.getTokenizedCreditCard();
            PaymentHandler.paymentSaleCompleted(
                conn,
                invalidateList,
                Integer.parseInt(transaction.getPersistenceUniqueId()),
                authorizationCommunicationResult==null ? null : authorizationCommunicationResult.name(),
                authorizationResult.getProviderErrorCode(),
                authorizationErrorCode==null ? null : authorizationErrorCode.name(),
                authorizationResult.getProviderErrorMessage(),
                authorizationResult.getProviderUniqueId(),
                tokenizedCreditCard == null ? null : tokenizedCreditCard.getProviderReplacementMaskedCardNumber(),
                tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber(),
                tokenizedCreditCard == null ? null : tokenizedCreditCard.getProviderReplacementExpiration(),
                tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementExpirationMonth(),
                tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementExpirationYear(),
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
