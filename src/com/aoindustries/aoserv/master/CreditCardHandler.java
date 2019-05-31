/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.billing.TransactionType;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.payment.PaymentType;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.creditcards.AuthorizationResult;
import com.aoindustries.creditcards.CreditCard;
import com.aoindustries.creditcards.CreditCardProcessor;
import com.aoindustries.creditcards.MerchantServicesProvider;
import com.aoindustries.creditcards.MerchantServicesProviderFactory;
import com.aoindustries.creditcards.TokenizedCreditCard;
import com.aoindustries.creditcards.Transaction;
import com.aoindustries.creditcards.TransactionRequest;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.lang.SysExits;
import com.aoindustries.math.SafeMath;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Currency;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>CreditCardHandler</code> handles all the accesses to the <code>payment.CreditCard</code> table.
 *
 * TODO: Deactivate immediately on expired card
 * TODO: Retry failed cards on the 7th and 14th, then deactivate?  See newly documented account billing policy.
 *
 * @author  AO Industries, Inc.
 */
final public class CreditCardHandler /*implements CronJob*/ {

	private static final Logger logger = LogFactory.getLogger(ServerHandler.class);

	private CreditCardHandler() {
	}

	/**
	 * The maximum time for a processing pass.
	 */
	private static final long TIMER_MAX_TIME=60L*60*1000;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL=2L*60*60*1000;

	//private static boolean started=false;

	public static void start() {
		/*
		synchronized(System.out) {
			if(!started) {
				System.out.print("Starting " + CreditCardHandler.class.getSimpleName() + ": ");
				CronDaemon.addCronJob(new CreditCardHandler(), logger);
				started=true;
				System.out.println("Done");
			}
		}
		*/
	}

	public static void checkAccessCreditCard(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, action, Permission.Name.get_credit_cards);
		BusinessHandler.checkAccessBusiness(
			conn,
			source,
			action,
			getBusinessForCreditCard(conn, id)
		);
	}

	public static void checkAccessCreditCardProcessor(DatabaseConnection conn, RequestSource source, String action, String processor) throws IOException, SQLException {
		BusinessHandler.checkAccessBusiness(
			conn,
			source,
			action,
			getBusinessForCreditCardProcessor(conn, processor)
		);
	}

	public static void checkAccessCreditCardTransaction(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		checkAccessCreditCardProcessor(
			conn,
			source,
			action,
			getCreditCardProcessorForCreditCardTransaction(conn, id)
		);
	}

	public static void checkAccessEncryptionKey(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		BusinessHandler.checkAccessBusiness(
			conn,
			source,
			action,
			getBusinessForEncryptionKey(conn, id)
		);
	}

	/**
	 * Creates a new <code>CreditCard</code>.
	 */
	public static int addCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String processorName,
		Account.Name accounting,
		String groupName,
		String cardInfo, // TODO: Rename maskedCardNumber
		Byte expirationMonth,
		Short expirationYear,
		String providerUniqueId,
		String firstName,
		String lastName,
		String companyName,
		String email,
		String phone,
		String fax,
		String customerId,
		String customerTaxId,
		String streetAddress1,
		String streetAddress2,
		String city,
		String state,
		String postalCode,
		String countryCode,
		String principalName,
		String description,
		String encryptedCardNumber,
		int encryptionFrom,
		int encryptionRecipient
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "addCreditCard", Permission.Name.add_credit_card);
		BusinessHandler.checkAccessBusiness(conn, source, "addCreditCard", accounting);
		if(encryptionFrom!=-1) checkAccessEncryptionKey(conn, source, "addCreditCard", encryptionFrom);
		if(encryptionRecipient!=-1) checkAccessEncryptionKey(conn, source, "addCreditCard", encryptionRecipient);

		int id;
		if(encryptedCardNumber==null && encryptionFrom==-1 && encryptionRecipient==-1) {
			id = conn.executeIntUpdate(
				"INSERT INTO payment.\"CreditCard\" (\n"
				+ "  processor_id,\n"
				+ "  accounting,\n"
				+ "  group_name,\n"
				+ "  card_info,\n"
				+ "  \"expirationMonth\",\n"
				+ "  \"expirationYear\",\n"
				+ "  provider_unique_id,\n"
				+ "  first_name,\n"
				+ "  last_name,\n"
				+ "  company_name,\n"
				+ "  email,\n"
				+ "  phone,\n"
				+ "  fax,\n"
				+ "  \"customerId\",\n"
				+ "  customer_tax_id,\n"
				+ "  street_address1,\n"
				+ "  street_address2,\n"
				+ "  city,\n"
				+ "  \"state\",\n"
				+ "  postal_code,\n"
				+ "  country_code,\n"
				+ "  created_by,\n"
				+ "  principal_name,\n"
				+ "  description\n"
				+ ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
				processorName,
				accounting,
				groupName,
				cardInfo,
				expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
				expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
				providerUniqueId,
				firstName,
				lastName,
				companyName,
				email,
				phone,
				fax,
				customerId,
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
		} else if(encryptedCardNumber!=null && encryptionFrom!=-1 && encryptionRecipient!=-1) {
			id = conn.executeIntUpdate(
				"INSERT INTO payment.\"CreditCard\" (\n"
				+ "  processor_id,\n"
				+ "  accounting,\n"
				+ "  group_name,\n"
				+ "  card_info,\n"
				+ "  \"expirationMonth\",\n"
				+ "  \"expirationYear\",\n"
				+ "  provider_unique_id,\n"
				+ "  first_name,\n"
				+ "  last_name,\n"
				+ "  company_name,\n"
				+ "  email,\n"
				+ "  phone,\n"
				+ "  fax,\n"
				+ "  \"customerId\",\n"
				+ "  customer_tax_id,\n"
				+ "  street_address1,\n"
				+ "  street_address2,\n"
				+ "  city,\n"
				+ "  \"state\",\n"
				+ "  postal_code,\n"
				+ "  country_code,\n"
				+ "  created_by,\n"
				+ "  created_by,\n"
				+ "  principal_name,\n"
				+ "  description,\n"
				+ "  encrypted_card_number,\n"
				+ "  encryption_card_number_from,\n"
				+ "  encryption_card_number_recipient\n"
				+ ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
				processorName,
				accounting,
				groupName,
				cardInfo,
				expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
				expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
				providerUniqueId,
				firstName,
				lastName,
				companyName,
				email,
				phone,
				fax,
				customerId,
				customerTaxId,
				streetAddress1,
				streetAddress2,
				city,
				state,
				postalCode,
				countryCode,
				source.getUsername(),
				principalName,
				description,
				encryptedCardNumber,
				encryptionFrom,
				encryptionRecipient
			);
		} else throw new SQLException("encryptedCardNumber, encryptionFrom, and encryptionRecipient must either all be null or none null");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARDS, accounting, InvalidateList.allServers, false);
		return id;
	}

	public static void creditCardDeclined(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String reason
	) throws IOException, SQLException {
		BankAccountHandler.checkAccounting(conn, source, "creditCardDeclined");
		checkAccessCreditCard(conn, source, "creditCardDeclined", id);

		conn.executeUpdate(
			"update payment.\"CreditCard\" set active=false, deactivated_on=now(), deactivate_reason=? where id=?",
			reason,
			id
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			CreditCardHandler.getBusinessForCreditCard(conn, id),
			InvalidateList.allServers,
			false
		);
	}

	public static Account.Name getBusinessForCreditCard(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select accounting from payment.\"CreditCard\" where id=?",
			id
		);
	}

	public static Account.Name getBusinessForCreditCardProcessor(DatabaseConnection conn, String processor) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select accounting from payment.\"Processor\" where provider_id=?",
			processor
		);
	}

	public static String getCreditCardProcessorForCreditCardTransaction(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeStringQuery("select processor_id from payment.\"Payment\" where id=?", id);
	}

	public static Account.Name getBusinessForEncryptionKey(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select accounting from pki.\"EncryptionKey\" where id=?",
			id);
	}

	public static void removeCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "removeCreditCard", Permission.Name.delete_credit_card);
		checkAccessCreditCard(conn, source, "removeCreditCard", id);

		removeCreditCard(conn, invalidateList, id);
	}

	public static void removeCreditCard(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		// Grab values for later use
		Account.Name business=getBusinessForCreditCard(conn, id);

		// Update the database
		conn.executeUpdate("delete from payment.\"CreditCard\" where id=?", id);

		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			business,
			BusinessHandler.getServersForBusiness(conn, business),
			false
		);
	}

	public static void updateCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String cardInfo,
		String firstName,
		String lastName,
		String companyName,
		String email,
		String phone,
		String fax,
		String customerId,
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
		BusinessHandler.checkPermission(conn, source, "updateCreditCard", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCard", id);
		assert cardInfo != null || source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) < 0 : "Compatibility with older clients (before 1.82.0) that don't send any cardInfo";

		updateCreditCard(
			conn,
			invalidateList,
			id,
			cardInfo,
			firstName,
			lastName,
			companyName,
			email,
			phone,
			fax,
			customerId,
			customerTaxId,
			streetAddress1,
			streetAddress2,
			city,
			state,
			postalCode,
			countryCode,
			description
		);
	}

	public static void updateCreditCard(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id,
		String cardInfo,
		String firstName,
		String lastName,
		String companyName,
		String email,
		String phone,
		String fax,
		String customerId,
		String customerTaxId,
		String streetAddress1,
		String streetAddress2,
		String city,
		String state,
		String postalCode,
		String countryCode,
		String description
	) throws IOException, SQLException {
		// Update row
		if(cardInfo == null) {
			conn.executeUpdate(
				"update\n"
				+ "  payment.\"CreditCard\"\n"
				+ "set\n"
				+ "  first_name=?,\n"
				+ "  last_name=?,\n"
				+ "  company_name=?,\n"
				+ "  email=?,\n"
				+ "  phone=?,\n"
				+ "  fax=?,\n"
				+ "  \"customerId\"=?,\n"
				+ "  customer_tax_id=?,\n"
				+ "  street_address1=?,\n"
				+ "  street_address2=?,\n"
				+ "  city=?,\n"
				+ "  state=?,\n"
				+ "  postal_code=?,\n"
				+ "  country_code=?,\n"
				+ "  description=?\n"
				+ "where\n"
				+ "  id=?",
				firstName,
				lastName,
				companyName,
				email,
				phone,
				fax,
				customerId,
				customerTaxId,
				streetAddress1,
				streetAddress2,
				city,
				state,
				postalCode,
				countryCode,
				description,
				id
			);
		} else {
			conn.executeUpdate(
				"update\n"
				+ "  payment.\"CreditCard\"\n"
				+ "set\n"
				+ "  card_info=?,\n"
				+ "  first_name=?,\n"
				+ "  last_name=?,\n"
				+ "  company_name=?,\n"
				+ "  email=?,\n"
				+ "  phone=?,\n"
				+ "  fax=?,\n"
				+ "  \"customerId\"=?,\n"
				+ "  customer_tax_id=?,\n"
				+ "  street_address1=?,\n"
				+ "  street_address2=?,\n"
				+ "  city=?,\n"
				+ "  state=?,\n"
				+ "  postal_code=?,\n"
				+ "  country_code=?,\n"
				+ "  description=?\n"
				+ "where\n"
				+ "  id=?",
				cardInfo,
				firstName,
				lastName,
				companyName,
				email,
				phone,
				fax,
				customerId,
				customerTaxId,
				streetAddress1,
				streetAddress2,
				city,
				state,
				postalCode,
				countryCode,
				description,
				id
			);
		}

		Account.Name accounting = getBusinessForCreditCard(conn, id);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	public static void updateCreditCardNumberAndExpiration(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String maskedCardNumber,
		Byte expirationMonth,
		Short expirationYear,
		String encryptedCardNumber,
		int encryptionFrom,
		int encryptionRecipient
	) throws IOException, SQLException {
		// Permission checks
		BusinessHandler.checkPermission(conn, source, "updateCreditCardNumberAndExpiration", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCardNumberAndExpiration", id);

		if(encryptionFrom!=-1) checkAccessEncryptionKey(conn, source, "updateCreditCardNumberAndExpiration", encryptionFrom);
		if(encryptionRecipient!=-1) checkAccessEncryptionKey(conn, source, "updateCreditCardNumberAndExpiration", encryptionRecipient);

		if(encryptedCardNumber==null && encryptionFrom==-1 && encryptionRecipient==-1) {
			// Update row
			conn.executeUpdate(
				"update\n"
				+ "  payment.\"CreditCard\"\n"
				+ "set\n"
				+ "  card_info=?,\n"
				+ "  \"expirationMonth\"=?,\n"
				+ "  \"expirationYear\"=?,\n"
				+ "  encrypted_card_number=null,\n"
				+ "  encryption_card_number_from=null,\n"
				+ "  encryption_card_number_recipient=null\n"
				+ "where\n"
				+ "  id=?",
				maskedCardNumber,
				expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
				expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
				id
			);
		} else if(encryptedCardNumber!=null && encryptionFrom!=-1 && encryptionRecipient!=-1) {
			// Update row
			conn.executeUpdate(
				"update\n"
				+ "  payment.\"CreditCard\"\n"
				+ "set\n"
				+ "  card_info=?,\n"
				+ "  \"expirationMonth\"=?,\n"
				+ "  \"expirationYear\"=?,\n"
				+ "  encrypted_card_number=?,\n"
				+ "  encryption_card_number_from=?,\n"
				+ "  encryption_card_number_recipient=?\n"
				+ "where\n"
				+ "  id=?",
				maskedCardNumber,
				expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
				expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
				encryptedCardNumber,
				encryptionFrom,
				encryptionRecipient,
				id
			);
		} else throw new SQLException("encryptedCardNumber, encryptionFrom, and encryptionRecipient must either all be null or none null");

		Account.Name accounting = getBusinessForCreditCard(conn, id);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	public static void updateCreditCardExpiration(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		Byte expirationMonth,
		Short expirationYear
	) throws IOException, SQLException {
		// Permission checks
		BusinessHandler.checkPermission(conn, source, "updateCreditCardExpiration", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCardExpiration", id);

		updateCreditCardExpiration(conn, invalidateList, id, expirationMonth, expirationYear);
	}

	public static void updateCreditCardExpiration(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id,
		Byte expirationMonth,
		Short expirationYear
	) throws IOException, SQLException {
		// Update row
		conn.executeUpdate(
			"update\n"
			+ "  payment.\"CreditCard\"\n"
			+ "set\n"
			+ "  \"expirationMonth\"=?,\n"
			+ "  \"expirationYear\"=?\n"
			+ "where\n"
			+ "  id=?",
			expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
			expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
			id
		);

		Account.Name accounting = getBusinessForCreditCard(conn, id);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	public static void reactivateCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		// Permission checks
		BusinessHandler.checkPermission(conn, source, "reactivateCreditCard", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "reactivateCreditCard", id);

		// Update row
		conn.executeUpdate(
			"update\n"
			+ "  payment.\"CreditCard\"\n"
			+ "set\n"
			+ "  active=true,\n"
			+ "  deactivated_on=null,\n"
			+ "  deactivate_reason=null\n"
			+ "where\n"
			+ "  id=?",
			id
		);

		Account.Name accounting = getBusinessForCreditCard(conn, id);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	public static void setCreditCardUseMonthly(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name accounting,
		int id
	) throws IOException, SQLException {
		// Permission checks
		BusinessHandler.checkPermission(conn, source, "setCreditCardUseMonthly", Permission.Name.edit_credit_card);

		if(id==-1) {
			// Clear only
			conn.executeUpdate("update payment.\"CreditCard\" set use_monthly=false where accounting=? and use_monthly", accounting);
		} else {
			checkAccessCreditCard(conn, source, "setCreditCardUseMonthly", id);

			// Make sure accounting codes match
			if(!accounting.equals(getBusinessForCreditCard(conn, id))) throw new SQLException("credit card and business accounting codes do not match");

			// Perform clear and set in one SQL statement - I thinks myself clever right now.
			conn.executeUpdate("update payment.\"CreditCard\" set use_monthly=(id=?) where accounting=? and use_monthly!=(id=?)", id, accounting, id);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			accounting,
			BusinessHandler.getServersForBusiness(conn, accounting),
			false
		);
	}

	/**
	 * Creates a new <code>Payment</code>.
	 */
	public static int addCreditCardTransaction(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String processor,
		Account.Name accounting,
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
		User.Name creditCardCreatedBy,
		String creditCardPrincipalName,
		Account.Name creditCardAccounting,
		String creditCardGroupName,
		String creditCardProviderUniqueId,
		String creditCardMaskedCardNumber,
		Byte creditCard_expirationMonth,
		Short creditCard_expirationYear,
		String creditCardFirstName,
		String creditCardLastName,
		String creditCardCompanyName,
		String creditCardEmail,
		String creditCardPhone,
		String creditCardFax,
		String creditCardCustomerId,
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
		BusinessHandler.checkPermission(conn, source, "addCreditCardTransaction", Permission.Name.add_credit_card_transaction);
		checkAccessCreditCardProcessor(conn, source, "addCreditCardTransaction", processor);
		BusinessHandler.checkAccessBusiness(conn, source, "addCreditCardTransaction", accounting);
		BusinessHandler.checkAccessBusiness(conn, source, "addCreditCardTransaction", creditCardAccounting);
		UsernameHandler.checkAccessUsername(conn, source, "addCreditCardTransaction", creditCardCreatedBy);

		return addCreditCardTransaction(
			conn,
			invalidateList,
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
			creditCard_expirationMonth,
			creditCard_expirationYear,
			creditCardFirstName,
			creditCardLastName,
			creditCardCompanyName,
			creditCardEmail,
			creditCardPhone,
			creditCardFax,
			creditCardCustomerId,
			creditCardCustomerTaxId,
			creditCardStreetAddress1,
			creditCardStreetAddress2,
			creditCardCity,
			creditCardState,
			creditCardPostalCode,
			creditCardCountryCode,
			creditCardComments,
			authorizationTime,
			source.getUsername(),
			authorizationPrincipalName
		);
	}

	/**
	 * Creates a new <code>Payment</code>.
	 */
	public static int addCreditCardTransaction(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String processor,
		Account.Name accounting,
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
		User.Name creditCardCreatedBy,
		String creditCardPrincipalName,
		Account.Name creditCardAccounting,
		String creditCardGroupName,
		String creditCardProviderUniqueId,
		String creditCardMaskedCardNumber,
		Byte creditCard_expirationMonth,
		Short creditCard_expirationYear,
		String creditCardFirstName,
		String creditCardLastName,
		String creditCardCompanyName,
		String creditCardEmail,
		String creditCardPhone,
		String creditCardFax,
		String creditCardCustomerId,
		String creditCardCustomerTaxId,
		String creditCardStreetAddress1,
		String creditCardStreetAddress2,
		String creditCardCity,
		String creditCardState,
		String creditCardPostalCode,
		String creditCardCountryCode,
		String creditCardComments,
		long authorizationTime,
		User.Name authorizationUsername,
		String authorizationPrincipalName
	) throws IOException, SQLException {
		int id = conn.executeIntUpdate(
			"INSERT INTO payment.\"Payment\" (\n"
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
			+ "  \"creditCard.expirationMonth\",\n"
			+ "  \"creditCard.expirationYear\",\n"
			+ "  credit_card_first_name,\n"
			+ "  credit_card_last_name,\n"
			+ "  credit_card_company_name,\n"
			+ "  credit_card_email,\n"
			+ "  credit_card_phone,\n"
			+ "  credit_card_fax,\n"
			+ "  \"creditCard.customerId\",\n"
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
			+ ") VALUES (?,?,?,?,?,?,?,?::numeric(9,2),?::numeric(9,2),?,?::numeric(9,2),?::numeric(9,2),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PROCESSING') RETURNING id", // TODO: Use enum for PROCESSING
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
			creditCard_expirationMonth == null ? DatabaseAccess.Null.SMALLINT : creditCard_expirationMonth.shortValue(),
			creditCard_expirationYear == null ? DatabaseAccess.Null.SMALLINT : creditCard_expirationYear,
			creditCardFirstName,
			creditCardLastName,
			creditCardCompanyName,
			creditCardEmail,
			creditCardPhone,
			creditCardFax,
			creditCardCustomerId,
			creditCardCustomerTaxId,
			creditCardStreetAddress1,
			creditCardStreetAddress2,
			creditCardCity,
			creditCardState,
			creditCardPostalCode,
			creditCardCountryCode,
			creditCardComments,
			new Timestamp(authorizationTime),
			authorizationUsername,
			authorizationPrincipalName
		);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, accounting, InvalidateList.allServers, false);
		return id;
	}

	public static void creditCardTransactionSaleCompleted(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String authorizationCommunicationResult,
		String authorizationProviderErrorCode,
		String authorizationErrorCode,
		String authorizationProviderErrorMessage,
		String authorizationProviderUniqueId,
		String authorizationResult_providerReplacementMaskedCardNumber,
		String authorizationResult_replacementMaskedCardNumber,
		String authorizationResult_providerReplacementExpiration,
		Byte authorizationResult_replacementExpirationMonth,
		Short authorizationResult_replacementExpirationYear,
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
		BusinessHandler.checkPermission(conn, source, "creditCardTransactionSaleCompleted", Permission.Name.credit_card_transaction_sale_completed);
		checkAccessCreditCardTransaction(conn, source, "creditCardTransactionSaleCompleted", id);
		// TODO: Are the principal names required to be AOServ objects, or are they arbitrary application-specific values?
		if(capturePrincipalName!=null) {
			try {
				UsernameHandler.checkAccessUsername(conn, source, "creditCardTransactionSaleCompleted", User.Name.valueOf(capturePrincipalName));
			} catch(ValidationException e) {
				throw new SQLException(e.getLocalizedMessage(), e);
			}
		}

		creditCardTransactionSaleCompleted(
			conn,
			invalidateList,
			id,
			authorizationCommunicationResult,
			authorizationProviderErrorCode,
			authorizationErrorCode,
			authorizationProviderErrorMessage,
			authorizationProviderUniqueId,
			authorizationResult_providerReplacementMaskedCardNumber,
			authorizationResult_replacementMaskedCardNumber,
			authorizationResult_providerReplacementExpiration,
			authorizationResult_replacementExpirationMonth,
			authorizationResult_replacementExpirationYear,
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
			captureTime,
			source.getUsername(),
			capturePrincipalName,
			captureCommunicationResult,
			captureProviderErrorCode,
			captureErrorCode,
			captureProviderErrorMessage,
			captureProviderUniqueId,
			status
		);
	}

	public static void creditCardTransactionSaleCompleted(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id,
		String authorizationCommunicationResult,
		String authorizationProviderErrorCode,
		String authorizationErrorCode,
		String authorizationProviderErrorMessage,
		String authorizationProviderUniqueId,
		String authorizationResult_providerReplacementMaskedCardNumber,
		String authorizationResult_replacementMaskedCardNumber,
		String authorizationResult_providerReplacementExpiration,
		Byte authorizationResult_replacementExpirationMonth,
		Short authorizationResult_replacementExpirationYear,
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
		User.Name captureUsername,
		String capturePrincipalName,
		String captureCommunicationResult,
		String captureProviderErrorCode,
		String captureErrorCode,
		String captureProviderErrorMessage,
		String captureProviderUniqueId,
		String status
	) throws IOException, SQLException {
		String processor = getCreditCardProcessorForCreditCardTransaction(conn, id);
		Account.Name accounting = getBusinessForCreditCardProcessor(conn, processor);

		int updated = conn.executeUpdate(
			"update\n"
			+ "  payment.\"Payment\"\n"
			+ "set\n"
			+ "  authorization_communication_result=?::\"com.aoindustries.creditcards\".\"TransactionResult.CommunicationResult\",\n"
			+ "  authorization_provider_error_code=?,\n"
			+ "  authorization_error_code=?::\"com.aoindustries.creditcards\".\"TransactionResult.ErrorCode\",\n"
			+ "  authorization_provider_error_message=?,\n"
			+ "  authorization_provider_unique_id=?,\n"
			+ "  \"authorizationResult.providerReplacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.replacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.providerReplacementExpiration\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationMonth\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationYear\"=?,\n"
			+ "  authorization_provider_approval_result=?,\n"
			+ "  authorization_approval_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.ApprovalResult\",\n"
			+ "  authorization_provider_decline_reason=?,\n"
			+ "  authorization_decline_reason=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.DeclineReason\",\n"
			+ "  authorization_provider_review_reason=?,\n"
			+ "  authorization_review_reason=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.ReviewReason\",\n"
			+ "  authorization_provider_cvv_result=?,\n"
			+ "  authorization_cvv_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.CvvResult\",\n"
			+ "  authorization_provider_avs_result=?,\n"
			+ "  authorization_avs_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.AvsResult\",\n"
			+ "  authorization_approval_code=?,\n"
			+ "  capture_time=?::timestamp,\n"
			+ "  capture_username=?,\n"
			+ "  capture_principal_name=?,\n"
			+ "  capture_communication_result=?::\"com.aoindustries.creditcards\".\"TransactionResult.CommunicationResult\",\n"
			+ "  capture_provider_error_code=?,\n"
			+ "  capture_error_code=?::\"com.aoindustries.creditcards\".\"TransactionResult.ErrorCode\",\n"
			+ "  capture_provider_error_message=?,\n"
			+ "  capture_provider_unique_id=?,\n"
			+ "  status=?::\"com.aoindustries.creditcards\".\"Transaction.Status\"\n"
			+ "where\n"
			+ "  id=?\n"
			+ "  and status in ('PROCESSING', 'AUTHORIZED')",
			authorizationCommunicationResult,
			authorizationProviderErrorCode,
			authorizationErrorCode,
			authorizationProviderErrorMessage,
			authorizationProviderUniqueId,
			authorizationResult_providerReplacementMaskedCardNumber,
			authorizationResult_replacementMaskedCardNumber,
			authorizationResult_providerReplacementExpiration,
			authorizationResult_replacementExpirationMonth == null ? DatabaseAccess.Null.SMALLINT : authorizationResult_replacementExpirationMonth.shortValue(),
			authorizationResult_replacementExpirationYear == null ? DatabaseAccess.Null.SMALLINT : authorizationResult_replacementExpirationYear,
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
			captureUsername,
			capturePrincipalName,
			captureCommunicationResult,
			captureProviderErrorCode,
			captureErrorCode,
			captureProviderErrorMessage,
			captureProviderUniqueId,
			status,
			id
		);
		if(updated!=1) throw new SQLException("Unable to find payment.Payment with id="+id+" and status in ('PROCESSING', 'AUTHORIZED')");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, accounting, InvalidateList.allServers, false);
	}

	public static void creditCardTransactionAuthorizeCompleted(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String authorizationCommunicationResult,
		String authorizationProviderErrorCode,
		String authorizationErrorCode,
		String authorizationProviderErrorMessage,
		String authorizationProviderUniqueId,
		String authorizationResult_providerReplacementMaskedCardNumber,
		String authorizationResult_replacementMaskedCardNumber,
		String authorizationResult_providerReplacementExpiration,
		Byte authorizationResult_replacementExpirationMonth,
		Short authorizationResult_replacementExpirationYear,
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
		String status
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "creditCardTransactionAuthorizeCompleted", Permission.Name.credit_card_transaction_authorize_completed);
		checkAccessCreditCardTransaction(conn, source, "creditCardTransactionAuthorizeCompleted", id);

		String processor = getCreditCardProcessorForCreditCardTransaction(conn, id);
		Account.Name accounting = getBusinessForCreditCardProcessor(conn, processor);

		creditCardTransactionAuthorizeCompleted(
			conn,
			invalidateList,
			id,
			authorizationCommunicationResult,
			authorizationProviderErrorCode,
			authorizationErrorCode,
			authorizationProviderErrorMessage,
			authorizationProviderUniqueId,
			authorizationResult_providerReplacementMaskedCardNumber,
			authorizationResult_replacementMaskedCardNumber,
			authorizationResult_providerReplacementExpiration,
			authorizationResult_replacementExpirationMonth,
			authorizationResult_replacementExpirationYear,
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
			status
		);
	}

	public static void creditCardTransactionAuthorizeCompleted(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int id,
		String authorizationCommunicationResult,
		String authorizationProviderErrorCode,
		String authorizationErrorCode,
		String authorizationProviderErrorMessage,
		String authorizationProviderUniqueId,
		String authorizationResult_providerReplacementMaskedCardNumber,
		String authorizationResult_replacementMaskedCardNumber,
		String authorizationResult_providerReplacementExpiration,
		Byte authorizationResult_replacementExpirationMonth,
		Short authorizationResult_replacementExpirationYear,
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
		String status
	) throws IOException, SQLException {
		String processor = getCreditCardProcessorForCreditCardTransaction(conn, id);
		Account.Name accounting = getBusinessForCreditCardProcessor(conn, processor);

		int updated = conn.executeUpdate(
			"update\n"
			+ "  payment.\"Payment\"\n"
			+ "set\n"
			+ "  authorization_communication_result=?::\"com.aoindustries.creditcards\".\"TransactionResult.CommunicationResult\",\n"
			+ "  authorization_provider_error_code=?,\n"
			+ "  authorization_error_code=?::\"com.aoindustries.creditcards\".\"TransactionResult.ErrorCode\",\n"
			+ "  authorization_provider_error_message=?,\n"
			+ "  authorization_provider_unique_id=?,\n"
			+ "  \"authorizationResult.providerReplacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.replacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.providerReplacementExpiration\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationMonth\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationYear\"=?,\n"
			+ "  authorization_provider_approval_result=?,\n"
			+ "  authorization_approval_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.ApprovalResult\",\n"
			+ "  authorization_provider_decline_reason=?,\n"
			+ "  authorization_decline_reason=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.DeclineReason\",\n"
			+ "  authorization_provider_review_reason=?,\n"
			+ "  authorization_review_reason=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.ReviewReason\",\n"
			+ "  authorization_provider_cvv_result=?,\n"
			+ "  authorization_cvv_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.CvvResult\",\n"
			+ "  authorization_provider_avs_result=?,\n"
			+ "  authorization_avs_result=?::\"com.aoindustries.creditcards\".\"AuthorizationResult.AvsResult\",\n"
			+ "  authorization_approval_code=?,\n"
			+ "  status=?::\"com.aoindustries.creditcards\".\"Transaction.Status\"\n"
			+ "where\n"
			+ "  id=?\n"
			+ "  and status='PROCESSING'", // TODO: Use enum here instead of literal
			authorizationCommunicationResult,
			authorizationProviderErrorCode,
			authorizationErrorCode,
			authorizationProviderErrorMessage,
			authorizationProviderUniqueId,
			authorizationResult_providerReplacementMaskedCardNumber,
			authorizationResult_replacementMaskedCardNumber,
			authorizationResult_providerReplacementExpiration,
			authorizationResult_replacementExpirationMonth == null ? DatabaseAccess.Null.SMALLINT : authorizationResult_replacementExpirationMonth.shortValue(),
			authorizationResult_replacementExpirationYear == null ? DatabaseAccess.Null.SMALLINT : authorizationResult_replacementExpirationYear,
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
			status,
			id
		);
		if(updated!=1) throw new SQLException("Unable to find payment.Payment with id="+id+" and status='PROCESSING'");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, accounting, InvalidateList.allServers, false);
	}

	private static class AutomaticPayment {
		final private Account.Name accounting;
		final private BigDecimal amount;
		final private int ccPkey;
		final private String ccCardInfo;
		final private Byte ccExpirationMonth;
		final private Short ccExpirationYear;
		final private String ccPrincipalName;
		final private String ccGroupName;
		final private String ccProviderUniqueId;
		final private String ccFirstName;
		final private String ccLastName;
		final private String ccCompanyName;
		final private String ccEmail;
		final private String ccPhone;
		final private String ccFax;
		final private String ccCustomerId;
		final private String ccCustomerTaxId;
		final private String ccStreetAddress1;
		final private String ccStreetAddress2;
		final private String ccCity;
		final private String ccState;
		final private String ccPostalCode;
		final private String ccCountryCode;
		final private String ccComments;
		final private String ccpProviderId;
		final private String ccpClassName;
		final private String ccpParam1;
		final private String ccpParam2;
		final private String ccpParam3;
		final private String ccpParam4;

		private AutomaticPayment(
			Account.Name accounting,
			BigDecimal amount,
			int ccPkey,
			String ccCardInfo,
			Byte ccExpirationMonth,
			Short ccExpirationYear,
			String ccPrincipalName,
			String ccGroupName,
			String ccProviderUniqueId,
			String ccFirstName,
			String ccLastName,
			String ccCompanyName,
			String ccEmail,
			String ccPhone,
			String ccFax,
			String ccCustomerId,
			String ccCustomerTaxId,
			String ccStreetAddress1,
			String ccStreetAddress2,
			String ccCity,
			String ccState,
			String ccPostalCode,
			String ccCountryCode,
			String ccComments,
			String ccpProviderId,
			String ccpClassName,
			String ccpParam1,
			String ccpParam2,
			String ccpParam3,
			String ccpParam4
		) {
			this.accounting = accounting;
			this.amount = amount;
			this.ccPkey = ccPkey;
			this.ccCardInfo = ccCardInfo;
			this.ccExpirationMonth = ccExpirationMonth;
			this.ccExpirationYear = ccExpirationYear;
			this.ccPrincipalName = ccPrincipalName;
			this.ccGroupName = ccGroupName;
			this.ccProviderUniqueId = ccProviderUniqueId;
			this.ccFirstName = ccFirstName;
			this.ccLastName = ccLastName;
			this.ccCompanyName = ccCompanyName;
			this.ccEmail = ccEmail;
			this.ccPhone = ccPhone;
			this.ccFax = ccFax;
			this.ccCustomerId = ccCustomerId;
			this.ccCustomerTaxId = ccCustomerTaxId;
			this.ccStreetAddress1 = ccStreetAddress1;
			this.ccStreetAddress2 = ccStreetAddress2;
			this.ccCity = ccCity;
			this.ccState = ccState;
			this.ccPostalCode = ccPostalCode;
			this.ccCountryCode = ccCountryCode;
			this.ccComments = ccComments;
			this.ccpProviderId = ccpProviderId;
			this.ccpClassName = ccpClassName;
			this.ccpParam1 = ccpParam1;
			this.ccpParam2 = ccpParam2;
			this.ccpParam3 = ccpParam3;
			this.ccpParam4 = ccpParam4;
		}
	}

	// TODO: infoOut, warningOut, verboseOut here, too
	private static void processAutomaticPayments(int month, int year) {
		System.err.println("DEBUG: month="+year+"-"+month);
		try {
			try (
				ProcessTimer timer=new ProcessTimer(
					logger,
					CreditCardHandler.class.getName(),
					"processAutomaticPayments",
					"CreditCardHandler - Process Automatic Payments",
					"Processes the automatic payments for the month",
					TIMER_MAX_TIME,
					TIMER_REMINDER_INTERVAL
				);
			) {
				MasterServer.executorService.submit(timer);

				// Find the beginning of the next month (for transaction search)
				Calendar beginningOfNextMonth = Calendar.getInstance();
				beginningOfNextMonth.set(Calendar.YEAR, year);
				beginningOfNextMonth.set(Calendar.MONTH, month-1);
				beginningOfNextMonth.set(Calendar.DAY_OF_MONTH, 1);
				beginningOfNextMonth.set(Calendar.HOUR_OF_DAY, 0);
				beginningOfNextMonth.set(Calendar.MINUTE, 0);
				beginningOfNextMonth.set(Calendar.SECOND, 0);
				beginningOfNextMonth.set(Calendar.MILLISECOND, 0);
				beginningOfNextMonth.add(Calendar.MONTH, 1);

				// Find the last minute of the current month
				Calendar lastMinuteOfTheMonth = (Calendar)beginningOfNextMonth.clone();
				lastMinuteOfTheMonth.add(Calendar.MINUTE, -1);

				// Start the transaction
				InvalidateList invalidateList=new InvalidateList();
				DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
				try {
					boolean connRolledBack=false;
					try {
						// Find the accounting code, credit_card id, and account balances of all account.Account that have a credit card set for automatic payments (and is active)
						List<AutomaticPayment> automaticPayments = conn.executeQuery(
							(ResultSet results) -> {
								try {
									List<AutomaticPayment> list = new ArrayList<>();
									BigDecimal total = BigDecimal.ZERO;
									// Look for duplicate accounting codes and report a warning
									Account.Name lastAccounting = null;
									while(results.next()) {
										Account.Name accounting = Account.Name.valueOf(results.getString(1));
										if(accounting.equals(lastAccounting)) {
											logger.log(Level.WARNING, "More than one credit card marked as automatic for accounting={0}, using the first one found", accounting);
										} else {
											lastAccounting = accounting;
											BigDecimal endofmonth = results.getBigDecimal(2);
											BigDecimal current = results.getBigDecimal(3);
											if(
												endofmonth.compareTo(BigDecimal.ZERO)>0
												&& current.compareTo(BigDecimal.ZERO)>0
											) {
												BigDecimal amount = endofmonth.compareTo(current)<=0 ? endofmonth : current;
												total = total.add(amount);
												Byte expirationMonth = SafeMath.castByte(results.getShort(6));
												if(results.wasNull()) expirationMonth = null;
												Short expirationYear = results.getShort(7);
												if(results.wasNull()) expirationYear = null;
												list.add(
													new AutomaticPayment(
														accounting,
														amount,
														results.getInt(4),
														results.getString(5),
														expirationMonth,
														expirationYear,
														results.getString(8),
														results.getString(9),
														results.getString(10),
														results.getString(11),
														results.getString(12),
														results.getString(13),
														results.getString(14),
														results.getString(15),
														results.getString(16),
														results.getString(17),
														results.getString(18),
														results.getString(19),
														results.getString(20),
														results.getString(21),
														results.getString(22),
														results.getString(23),
														results.getString(24),
														results.getString(25),
														results.getString(26),
														results.getString(27),
														results.getString(28),
														results.getString(29),
														results.getString(30),
														results.getString(31)
													)
												);
											}
										}
									}
									System.out.println("Processing a total of $" + total);
									return list;
								} catch(ValidationException e) {
									throw new SQLException(e.getLocalizedMessage(), e);
								}
							},
							"select\n"
							+ "  bu.accounting,\n"
							+ "  endofmonth.balance as endofmonth,\n"
							+ "  current.balance as current,\n"
							+ "  cc.id,\n"
							+ "  cc.card_info,\n"
							+ "  cc.\"expirationMonth\",\n"
							+ "  cc.\"expirationYear\",\n"
							+ "  cc.principal_name,\n"
							+ "  cc.group_name,\n"
							+ "  cc.provider_unique_id,\n"
							+ "  cc.first_name,\n"
							+ "  cc.last_name,\n"
							+ "  cc.company_name,\n"
							+ "  cc.email,\n"
							+ "  cc.phone,\n"
							+ "  cc.fax,\n"
							+ "  cc.\"customerId\",\n"
							+ "  cc.customer_tax_id,\n"
							+ "  cc.street_address1,\n"
							+ "  cc.street_address2,\n"
							+ "  cc.city,\n"
							+ "  cc.state,\n"
							+ "  cc.postal_code,\n"
							+ "  cc.country_code,\n"
							+ "  cc.description,\n"
							+ "  ccp.provider_id,\n"
							+ "  ccp.class_name,\n"
							+ "  ccp.param1,\n"
							+ "  ccp.param2,\n"
							+ "  ccp.param3,\n"
							+ "  ccp.param4\n"
							+ "from\n"
							+ "  account.\"Account\" bu,\n"
							+ "  (\n"
							+ "    select\n"
							+ "      bu.accounting,\n"
							+ "      coalesce(sum(cast((tr.rate*tr.quantity) as numeric(9,2))), 0) as balance\n"
							+ "    from\n"
							+ "                account.\"Account\"     bu\n"
							+ "      left join billing.\"Transaction\" tr on bu.accounting = tr.accounting\n"
							+ "    where\n"
							+ "      tr.payment_confirmed!='N'\n"
							+ "      and tr.time<?\n"
							+ "    group by\n"
							+ "      bu.accounting\n"
							+ "  ) as endofmonth,\n"
							+ "  (\n"
							+ "    select\n"
							+ "      bu.accounting,\n"
							+ "      coalesce(sum(cast((tr.rate*tr.quantity) as numeric(9,2))), 0) as balance\n"
							+ "    from\n"
							+ "                account.\"Account\"     bu\n"
							+ "      left join billing.\"Transaction\" tr on bu.accounting = tr.accounting\n"
							+ "    where\n"
							+ "      tr.payment_confirmed!='N'\n"
							+ "    group by\n"
							+ "      bu.accounting\n"
							+ "  ) as current,\n"
							+ "  payment.\"CreditCard\" cc,\n"
							+ "  payment.\"Processor\" ccp\n"
							+ "where\n"
							+ "  bu.accounting=cc.accounting\n"
							+ "  and bu.accounting=endofmonth.accounting\n"
							+ "  and bu.accounting=current.accounting\n"
							+ "  and cc.use_monthly\n"
							+ "  and cc.active\n"
							+ "  and cc.processor_id=ccp.provider_id\n"
							+ "  and ccp.enabled\n"
							+ "order by\n"
							+ "  bu.accounting",
							new Timestamp(beginningOfNextMonth.getTimeInMillis())
						);
						// Only need to create the persistence once per DB transaction
						MasterPersistenceMechanism masterPersistenceMechanism = new MasterPersistenceMechanism(conn, invalidateList);
						for(AutomaticPayment automaticPayment : automaticPayments) {
							System.out.println("accounting="+automaticPayment.accounting);
							System.out.println("    amount="+automaticPayment.amount);
							// Find the processor
							CreditCardProcessor processor = new CreditCardProcessor(
								MerchantServicesProviderFactory.getMerchantServicesProvider(
									automaticPayment.ccpProviderId,
									automaticPayment.ccpClassName,
									automaticPayment.ccpParam1,
									automaticPayment.ccpParam2,
									automaticPayment.ccpParam3,
									automaticPayment.ccpParam4
								),
								masterPersistenceMechanism
							);
							System.out.println("    processor="+processor.getProviderId());

							// Add as pending transaction
							String paymentTypeName;
							String cardInfo = automaticPayment.ccCardInfo;
							// TODO: Use some sort of shared API for this
							if(
								cardInfo.startsWith("34")
								|| cardInfo.startsWith("37")
								|| cardInfo.startsWith("3" + CreditCard.UNKNOWN_DIGIT)
							) {
								paymentTypeName = PaymentType.AMEX;
							} else if(cardInfo.startsWith("60")) {
								paymentTypeName = PaymentType.DISCOVER;
							} else if(
								cardInfo.startsWith("51")
								|| cardInfo.startsWith("52")
								|| cardInfo.startsWith("53")
								|| cardInfo.startsWith("54")
								|| cardInfo.startsWith("55")
								|| cardInfo.startsWith("5" + CreditCard.UNKNOWN_DIGIT)
							) {
								paymentTypeName = PaymentType.MASTERCARD;
							} else if(cardInfo.startsWith("4")) {
								paymentTypeName = PaymentType.VISA;
							} else {
								paymentTypeName = null;
							}
							int transID = TransactionHandler.addTransaction(
								conn,
								invalidateList,
								new Timestamp(lastMinuteOfTheMonth.getTimeInMillis()),
								automaticPayment.accounting,
								automaticPayment.accounting,
								MasterPersistenceMechanism.MASTER_BUSINESS_ADMINISTRATOR,
								TransactionType.PAYMENT,
								"Monthly automatic billing",
								new BigDecimal("1.000"),
								automaticPayment.amount.negate(),
								paymentTypeName,
								cardInfo,
								automaticPayment.ccpProviderId,
								com.aoindustries.aoserv.client.billing.Transaction.WAITING_CONFIRMATION
							);
							conn.commit();

							// Process payment
							Transaction transaction = processor.sale(
								null,
								null,
								new TransactionRequest(
									false, // testMode
									InetAddress.getLocalHost().getHostAddress(),
									120, // duplicateWindow
									Integer.toString(transID), // orderNumber
									Currency.getInstance("USD"),
									automaticPayment.amount,
									null, // taxAmount
									false, // taxExempt
									null, // shippingAmount
									null, // dutyAmount
									null, // shippingFirstName
									null, // shippingLastName
									null, // shippingCompanyName
									null, // shippingStreetAddress1
									null, // shippingStreetAddress2
									null, // shippingCity
									null, // shippingState
									null, // shippingPostalCode
									null, // shippingCountryCode
									false, // emailCustomer
									null, // merchantEmail
									null, // invoiceNumber
									null, // purchaseOrderNumber
									"Monthly automatic billing"
								),
								new CreditCard(
									Integer.toString(automaticPayment.ccPkey),
									automaticPayment.ccPrincipalName,
									automaticPayment.ccGroupName,
									automaticPayment.ccpProviderId,
									automaticPayment.ccProviderUniqueId,
									null, // cardNumber
									automaticPayment.ccCardInfo,
									automaticPayment.ccExpirationMonth == null ? CreditCard.UNKNOWN_EXPRIATION_MONTH : automaticPayment.ccExpirationMonth,
									automaticPayment.ccExpirationYear == null ? CreditCard.UNKNOWN_EXPRIATION_YEAR : automaticPayment.ccExpirationYear,
									null, // cardCode
									automaticPayment.ccFirstName,
									automaticPayment.ccLastName,
									automaticPayment.ccCompanyName,
									automaticPayment.ccEmail,
									automaticPayment.ccPhone,
									automaticPayment.ccFax,
									automaticPayment.ccCustomerId,
									automaticPayment.ccCustomerTaxId,
									automaticPayment.ccStreetAddress1,
									automaticPayment.ccStreetAddress2,
									automaticPayment.ccCity,
									automaticPayment.ccState,
									automaticPayment.ccPostalCode,
									automaticPayment.ccCountryCode,
									automaticPayment.ccComments
								)
							);

							AuthorizationResult authorizationResult = transaction.getAuthorizationResult();
							TokenizedCreditCard tokenizedCreditCard = authorizationResult.getTokenizedCreditCard();
							switch(authorizationResult.getCommunicationResult()) {
								case LOCAL_ERROR :
								case IO_ERROR :
								case GATEWAY_ERROR :
								{
									// Update transaction as failed
									//     TODO: Deactivate the card if this is the 3rd consecutive failure
									//     TODO: Notify customer
									TransactionHandler.transactionDeclined(
										conn,
										invalidateList,
										transID,
										Integer.parseInt(transaction.getPersistenceUniqueId()),
										tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber()
									);
									conn.commit();
									System.out.println("    Result: Error");
									break;
								}
								case SUCCESS :
									// Check approval result
									switch(authorizationResult.getApprovalResult()) {
										case HOLD : {
											// Update transaction
											TransactionHandler.transactionHeld(
												conn,
												invalidateList,
												transID,
												Integer.parseInt(transaction.getPersistenceUniqueId()),
												tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber()
											);
											conn.commit();
											System.out.println("    Result: Hold");
											System.out.println("    Review Reason: "+authorizationResult.getReviewReason());
											break;
										}
										case DECLINED : {
											// Update transaction as declined
											//     TODO: Deactivate the card if this is the 3rd consecutive failure
											//     TODO: Notify customer
											TransactionHandler.transactionDeclined(
												conn,
												invalidateList,
												transID,
												Integer.parseInt(transaction.getPersistenceUniqueId()),
												tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber()
											);
											conn.commit();
											System.out.println("    Result: Declined");
											System.out.println("    Decline Reason: "+authorizationResult.getDeclineReason());
											break;
										}
										case APPROVED : {
											// Update transaction as successful
											TransactionHandler.transactionApproved(
												conn,
												invalidateList,
												transID,
												Integer.parseInt(transaction.getPersistenceUniqueId()),
												tokenizedCreditCard == null ? null : tokenizedCreditCard.getReplacementMaskedCardNumber()
											);
											System.out.println("    Result: Approved");
											break;
										}
										default: {
											throw new RuntimeException("Unexpected value for authorization approval result: "+authorizationResult.getApprovalResult());
										}
									}
									break;
								default:
									throw new RuntimeException("Unexpected value for authorization communication result: "+authorizationResult.getCommunicationResult());
							}
						}
					} catch(RuntimeException | IOException err) {
						if(conn.rollback()) {
							connRolledBack=true;
							// invalidateList=null; Not cleared because some commits happen during processing
						}
						throw err;
					} catch(SQLException err) {
						if(conn.rollbackAndClose()) {
							connRolledBack=true;
							// invalidateList=null; Not cleared because some commits happen during processing
						}
						throw err;
					} finally {
						if(!connRolledBack && !conn.isClosed()) conn.commit();
					}
				} finally {
					conn.releaseConnection();
				}
				/*if(invalidateList!=null)*/ MasterServer.invalidateTables(invalidateList, null);
			}
		} catch(ThreadDeath TD) {
			throw TD;
		} catch(Throwable T) {
			logger.log(Level.SEVERE, null, T);
		}
	}

	// TODO: Call from a CronJob, too.  Log any warnings as a warning ticket.  Once at system start-up, too?
	private static void synchronizeStoredCards(PrintWriter verboseOut, PrintWriter infoOut, PrintWriter warningOut, boolean dryRun) {
		try {
			try (
				ProcessTimer timer = new ProcessTimer(
					logger,
					CreditCardHandler.class.getName(),
					"synchronizeStoredCards",
					"CreditCardHandler - Synchronize Stored Cards",
					"Synchronizes any updated masked card numbers or expiration dates from the payment providers back to local persistence",
					TIMER_MAX_TIME,
					TIMER_REMINDER_INTERVAL
				);
			) {
				MasterServer.executorService.submit(timer);

				// Start the transaction
				InvalidateList invalidateList = new InvalidateList();
				DatabaseConnection conn = MasterDatabase.getDatabase().createDatabaseConnection();
				try {
					boolean connRolledBack = false;
					try {
						if(infoOut != null) infoOut.println(CreditCardHandler.class.getSimpleName() + ".synchronizeStoredCards: Synchronizing stored cards");

						// Find the accounting code, credit_card id, and account balances of all account.Account that have a credit card set for automatic payments (and is active)
						List<MerchantServicesProvider> providers = conn.executeObjectListQuery(
							(ResultSet result) -> {
								try {
									return MerchantServicesProviderFactory.getMerchantServicesProvider(
										result.getString("providerId"),
										result.getString("className"),
										result.getString("param1"),
										result.getString("param2"),
										result.getString("param3"),
										result.getString("param4")
									);
								} catch(ReflectiveOperationException e) {
									throw new SQLException(e.getLocalizedMessage(), e);
								}
							},
							"SELECT\n"
							+ "  provider_id AS \"providerId\",\n"
							+ "  class_name  AS \"className\",\n"
							+ "  param1,\n"
							+ "  param2,\n"
							+ "  param3,\n"
							+ "  param4\n"
							+ "FROM\n"
							+ "  payment.\"Processor\"\n"
							+ "WHERE\n"
							+ "  enabled\n"
							+ "ORDER BY\n"
							+ "  provider_id"
						);
						if(infoOut != null) infoOut.println(CreditCardHandler.class.getSimpleName() + ".synchronizeStoredCards: Found " + providers.size() + " enabled " + (providers.size() == 1 ? "payment processor" : "payment processors"));

						// Find all the stored cards
						MasterPersistenceMechanism masterPersistenceMechanism = new MasterPersistenceMechanism(conn, invalidateList);

						// Only need to create the persistence once per DB transaction
						for(MerchantServicesProvider provider : providers) {
							CreditCardProcessor processor = new CreditCardProcessor(provider, masterPersistenceMechanism);
							processor.synchronizeStoredCards(null, verboseOut, infoOut, warningOut, dryRun);
						}
					} catch(RuntimeException err) {
						if(conn.rollback()) {
							connRolledBack=true;
							// invalidateList=null; Not cleared because some commits happen during processing
						}
						throw err;
					} catch(SQLException err) {
						if(conn.rollbackAndClose()) {
							connRolledBack=true;
							// invalidateList=null; Not cleared because some commits happen during processing
						}
						throw err;
					} finally {
						if(!connRolledBack && !conn.isClosed()) conn.commit();
					}
				} finally {
					conn.releaseConnection();
				}
				/*if(invalidateList!=null)*/ MasterServer.invalidateTables(invalidateList, null);
			}
		} catch(ThreadDeath TD) {
			throw TD;
		} catch(Throwable T) {
			logger.log(Level.SEVERE, null, T);
		}
	}

	public static void main(String[] args) {
		int exitStatus;
		boolean verbose = false;
		boolean quiet = false;
		boolean dryRun = false;
		String command = null;
		for(String arg : args) {
			if("--verbose".equals(arg)) {
				verbose = true;
			} else if("--quiet".equals(arg)) {
				quiet = true;
			} else if("--dry-run".equals(arg)) {
				dryRun = true;
			} else if(command != null) {
				// Too many commands
				command = null;
				break;
			} else {
				command = arg;
			}
		}
		if(command != null) {
			if("synchronize".equals(command)) {
				PrintWriter out = new PrintWriter(System.out, true);
				synchronizeStoredCards(
					verbose ? out : null,
					quiet ? null : out,
					new PrintWriter(System.err, true),
					dryRun
				);
				exitStatus = 0;
			} else {
				// Not y10k compliant
				if(command.length()==7) {
					if(command.charAt(4)=='-') {
						try {
							int year = Integer.parseInt(command.substring(0,4));
							try {
								int month = Integer.parseInt(command.substring(5,7));
								if(dryRun) {
									System.err.println("Dry run not implemented for batch processing");
									exitStatus = SysExits.EX_SOFTWARE;
								} else {
									processAutomaticPayments(month, year);
									exitStatus = 0;
								}
							} catch(NumberFormatException err) {
								System.err.println("Unable to parse month");
								exitStatus = SysExits.EX_USAGE;
							}
						} catch(NumberFormatException err) {
							System.err.println("Unable to parse year");
							exitStatus = SysExits.EX_USAGE;
						}
					} else {
						System.err.println("Unable to find - in month");
						exitStatus = SysExits.EX_USAGE;
					}
				} else {
					System.err.println("Invalid month");
					exitStatus = SysExits.EX_USAGE;
				}
			}
		} else {
			System.err.println("usage: " + CreditCardHandler.class.getName() + " [--verbose] [--quiet] [--dry-run] {synchronize|YYYY-MM}");
			exitStatus = SysExits.EX_USAGE;
		}
		System.exit(exitStatus);
	}

	/**
	 * Runs at 12:12 am on the first day of the month.
	 */
	/*public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		return
			minute==12
			&& hour==0
			&& dayOfMonth==1
		;
	}*/

	/*public int getCronJobScheduleMode() {
		return CRON_JOB_SCHEDULE_SKIP;
	}*/

	/*public String getCronJobName() {
		return "CreditCardHandler";
	}*/

	/*public int getCronJobThreadPriority() {
		return Thread.NORM_PRIORITY+1;
	}*/

	/*
	public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
		// Find last month
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month-1);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MONTH, -1);
		// Process for last month
		processAutomaticPayments(cal.get(Calendar.MONTH)+1, cal.get(Calendar.YEAR));
	}
	*/
}
