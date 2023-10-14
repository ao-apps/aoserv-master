/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2023  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.dbc.ObjectFactory;
import com.aoapps.lang.i18n.Money;
import com.aoapps.lang.security.acl.Group;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.payments.AuthorizationResult;
import com.aoapps.payments.CaptureResult;
import com.aoapps.payments.CreditCard;
import com.aoapps.payments.PersistenceMechanism;
import com.aoapps.payments.TokenizedCreditCard;
import com.aoapps.payments.Transaction;
import com.aoapps.payments.TransactionRequest;
import com.aoapps.payments.TransactionResult;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.Administrator;
import java.io.IOException;
import java.security.Principal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
    } catch (ValidationException e) {
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
    if (result.wasNull()) {
      return CreditCard.UNKNOWN_EXPIRATION_MONTH;
    }
    return expirationMonth;
  }

  // TODO: 2.0: Nullable Short
  private static short getExpirationYear(ResultSet result, String columnLabel) throws SQLException {
    short expirationYear = result.getShort(columnLabel);
    if (result.wasNull()) {
      return CreditCard.UNKNOWN_EXPIRATION_YEAR;
    }
    return expirationYear;
  }

  private static final ObjectFactory<CreditCard> creditCardObjectFactory = result -> new CreditCard(
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

  private final DatabaseConnection conn;
  private final InvalidateList invalidateList;

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
    } catch (NumberFormatException e) {
      return null;
    }
    return conn.queryObjectOptional(
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
    ).orElse(null);
  }

  @Override
  public Map<String, CreditCard> getCreditCards(Principal principal) throws SQLException {
    return conn.queryCall(
        results -> {
          Map<String, CreditCard> map = new LinkedHashMap<>();
          while (results.next()) {
            CreditCard copy = creditCardObjectFactory.createObject(results);
            String persistenceUniqueId = copy.getPersistenceUniqueId();
            if (map.put(persistenceUniqueId, copy) != null) {
              throw new SQLException("Duplicate persistenceUniqueId: " + persistenceUniqueId);
            }
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
    return conn.queryCall(
        results -> {
          Map<String, CreditCard> map = new LinkedHashMap<>();
          while (results.next()) {
            CreditCard copy = creditCardObjectFactory.createObject(results);
            String providerUniqueId = copy.getProviderUniqueId();
            if (map.put(providerUniqueId, copy) != null) {
              throw new SQLException("Duplicate providerUniqueId: " + providerUniqueId);
            }
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
    } catch (IOException err) {
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
    } catch (IOException err) {
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
      com.aoindustries.aoserv.client.account.User.Name creditCardCreatedBy = conn.queryObject(
          ObjectFactories.userNameFactory,
          "select created_by from payment.\"CreditCard\" where id=?::integer",
          creditCard.getPersistenceUniqueId()
      );
      Account.Name ccBusiness = conn.queryObject(
          ObjectFactories.accountNameFactory,
          "select accounting from payment.\"CreditCard\" where id=?::integer",
          creditCard.getPersistenceUniqueId()
      );
      Byte expirationMonth = creditCard.getExpirationMonth(); // TODO: 2.0: Nullable Byte
      if (expirationMonth == CreditCard.UNKNOWN_EXPIRATION_MONTH) {
        expirationMonth = null;
      }
      Short expirationYear = creditCard.getExpirationYear(); // TODO: 2.0: Nullable Short
      if (expirationYear == CreditCard.UNKNOWN_EXPIRATION_YEAR) {
        expirationYear = null;
      }
      Currency currency = transactionRequest.getCurrency();
      int payment = PaymentHandler.addPayment(
          conn,
          invalidateList,
          transaction.getProviderId(),
          ccBusiness,
          group == null ? null : group.getName(),
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
          System.currentTimeMillis(), // TODO: Timestamp nanosecond precision
          MASTER_BUSINESS_ADMINISTRATOR,
          principal == null ? null : principal.getName()
      );
      conn.commit();
      return Integer.toString(payment);
    } catch (IOException err) {
      throw new SQLException(err);
    }
  }

  /**
   * Stores the results of a sale transaction.
   * <ol>
   *   <li>authorizationResult</li>
   *   <li>captureTime</li>
   *   <li>capturePrincipalName</li>
   *   <li>captureResult</li>
   *   <li>status</li>
   * </ol>
   * <p>
   * The current status must be PROCESSING or AUTHORIZED.
   * </p>
   */
  @Override
  public void saleCompleted(Principal principal, Transaction transaction) throws SQLException {
    try {
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

      long captureTime = transaction.getCaptureTime();

      TokenizedCreditCard tokenizedCreditCard = authorizationResult.getTokenizedCreditCard();
      PaymentHandler.paymentSaleCompleted(
          conn,
          invalidateList,
          Integer.parseInt(transaction.getPersistenceUniqueId()),
          authorizationCommunicationResult == null ? null : authorizationCommunicationResult.name(),
          authorizationResult.getProviderErrorCode(),
          authorizationErrorCode == null ? null : authorizationErrorCode.name(),
          authorizationResult.getProviderErrorMessage(),
          authorizationResult.getProviderUniqueId(),
          tokenizedCreditCard == null ? null : tokenizedCreditCard.getProviderReplacementMaskedCardNumber(),
          tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber(),
          tokenizedCreditCard == null ? null : tokenizedCreditCard.getProviderReplacementExpiration(),
          tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementExpirationMonth(),
          tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementExpirationYear(),
          authorizationResult.getProviderApprovalResult(),
          approvalResult == null ? null : approvalResult.name(),
          authorizationResult.getProviderDeclineReason(),
          declineReason == null ? null : declineReason.name(),
          authorizationResult.getProviderReviewReason(),
          reviewReason == null ? null : reviewReason.name(),
          authorizationResult.getProviderCvvResult(),
          cvvResult == null ? null : cvvResult.name(),
          authorizationResult.getProviderAvsResult(),
          avsResult == null ? null : avsResult.name(),
          authorizationResult.getApprovalCode(),
          captureTime == 0 ? null : new Timestamp(captureTime),
          MASTER_BUSINESS_ADMINISTRATOR,
          transaction.getCapturePrincipalName(),
          captureCommunicationResult == null ? null : captureCommunicationResult.name(),
          captureResult.getProviderErrorCode(),
          captureErrorCode == null ? null : captureErrorCode.name(),
          captureResult.getProviderErrorMessage(),
          captureResult.getProviderUniqueId(),
          transaction.getStatus().name()
      );
      conn.commit();
    } catch (IOException err) {
      throw new SQLException(err);
    }
  }

  /**
   * Stores the results of an authorize transaction.
   * <ol>
   *   <li>authorizationResult</li>
   *   <li>status</li>
   * </ol>
   * <p>
   * The current status must be PROCESSING.
   * </p>
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
