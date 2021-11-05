/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.lang.SysExits;
import com.aoapps.lang.i18n.CurrencyComparator;
import com.aoapps.lang.i18n.Money;
import com.aoapps.lang.math.SafeMath;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.payments.AuthorizationResult;
import com.aoapps.payments.CreditCard;
import com.aoapps.payments.CreditCardProcessor;
import com.aoapps.payments.MerchantServicesProvider;
import com.aoapps.payments.MerchantServicesProviderFactory;
import com.aoapps.payments.TokenizedCreditCard;
import com.aoapps.payments.Transaction;
import com.aoapps.payments.TransactionRequest;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.billing.TransactionType;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.payment.Payment;
import com.aoindustries.aoserv.client.payment.PaymentType;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.schema.Type;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Currency;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The {@link PaymentHandler} handles all the accesses to the <code>payment.CreditCard</code> table.
 *
 * TODO: Deactivate immediately on expired card
 * TODO: Retry failed cards on the 7th and 14th, then deactivate?  See newly documented account billing policy.
 *
 * @author  AO Industries, Inc.
 */
public abstract class PaymentHandler /*implements CronJob*/ {

	/** Make no instances. */
	private PaymentHandler() {throw new AssertionError();}

	private static final Logger logger = Logger.getLogger(NetHostHandler.class.getName());

	/**
	 * The maximum time for a processing pass.
	 */
	private static final long TIMER_MAX_TIME = 60L * 60 * 1000;

	/**
	 * The interval in which the administrators will be reminded.
	 */
	private static final long TIMER_REMINDER_INTERVAL = 2L * 60 * 60 * 1000;

	private static boolean started=false;

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	public static void start() {
		synchronized(System.out) {
			if(!started) {
				System.out.print("Starting " + PaymentHandler.class.getSimpleName() + ": ");
				CronDaemon.addCronJob(synchronizeStoredCardsCronJob, logger);
				CronDaemon.runImmediately(synchronizeStoredCardsCronJob);
				started=true;
				System.out.println("Done");
			}
		}
	}

	public static void checkAccessCreditCard(DatabaseConnection conn, RequestSource source, String action, int creditCard) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, action, Permission.Name.get_credit_cards);
		AccountHandler.checkAccessAccount(
			conn,
			source,
			action,
			getAccountForCreditCard(conn, creditCard)
		);
	}

	public static void checkAccessProcessor(DatabaseConnection conn, RequestSource source, String action, String processor) throws IOException, SQLException {
		AccountHandler.checkAccessAccount(
			conn,
			source,
			action,
			getAccountForProcessor(conn, processor)
		);
	}

	public static void checkAccessPayment(DatabaseConnection conn, RequestSource source, String action, int payment) throws IOException, SQLException {
		checkAccessProcessor(
			conn,
			source,
			action,
			getProcessorForPayment(conn, payment)
		);
	}

	public static void checkAccessEncryptionKey(DatabaseConnection conn, RequestSource source, String action, int encryptionKey) throws IOException, SQLException {
		AccountHandler.checkAccessAccount(
			conn,
			source,
			action,
			getAccountForEncryptionKey(conn, encryptionKey)
		);
	}

	/**
	 * Creates a new <code>CreditCard</code>.
	 */
	public static int addCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String processor,
		Account.Name account,
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
		AccountHandler.checkPermission(conn, source, "addCreditCard", Permission.Name.add_credit_card);
		AccountHandler.checkAccessAccount(conn, source, "addCreditCard", account);
		if(encryptionFrom!=-1) checkAccessEncryptionKey(conn, source, "addCreditCard", encryptionFrom);
		if(encryptionRecipient!=-1) checkAccessEncryptionKey(conn, source, "addCreditCard", encryptionRecipient);

		int creditCard;
		if(encryptedCardNumber==null && encryptionFrom==-1 && encryptionRecipient==-1) {
			creditCard = conn.updateInt(
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
				processor,
				account,
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
				source.getCurrentAdministrator(),
				principalName,
				description
			);
		} else if(encryptedCardNumber!=null && encryptionFrom!=-1 && encryptionRecipient!=-1) {
			creditCard = conn.updateInt(
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
				processor,
				account,
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
				source.getCurrentAdministrator(),
				principalName,
				description,
				encryptedCardNumber,
				encryptionFrom,
				encryptionRecipient
			);
		} else throw new SQLException("encryptedCardNumber, encryptionFrom, and encryptionRecipient must either all be null or none null");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARDS, account, InvalidateList.allHosts, false);
		return creditCard;
	}

	public static void creditCardDeclined(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard,
		String reason
	) throws IOException, SQLException {
		BankAccountHandler.checkIsAccounting(conn, source, "creditCardDeclined");
		checkAccessCreditCard(conn, source, "creditCardDeclined", creditCard);

		conn.update(
			"update payment.\"CreditCard\" set active=false, deactivated_on=now(), deactivate_reason=? where id=?",
			reason,
			creditCard
		);

		// Notify all clients of the update
		invalidateList.addTable(conn,
			Table.TableID.CREDIT_CARDS,
			PaymentHandler.getAccountForCreditCard(conn, creditCard),
			InvalidateList.allHosts,
			false
		);
	}

	public static Account.Name getAccountForCreditCard(DatabaseConnection conn, int creditCard) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select accounting from payment.\"CreditCard\" where id=?",
			creditCard
		);
	}

	public static Account.Name getAccountForProcessor(DatabaseConnection conn, String processor) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select accounting from payment.\"Processor\" where provider_id=?",
			processor
		);
	}

	public static String getProcessorForPayment(DatabaseConnection conn, int payment) throws IOException, SQLException {
		return conn.queryString("select processor_id from payment.\"Payment\" where id=?", payment);
	}

	public static Account.Name getAccountForEncryptionKey(DatabaseConnection conn, int encryptionKey) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select accounting from pki.\"EncryptionKey\" where id=?",
			encryptionKey
		);
	}

	public static void removeCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "removeCreditCard", Permission.Name.delete_credit_card);
		checkAccessCreditCard(conn, source, "removeCreditCard", creditCard);

		removeCreditCard(conn, invalidateList, creditCard);
	}

	public static void removeCreditCard(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int creditCard
	) throws IOException, SQLException {
		// Grab values for later use
		Account.Name business=getAccountForCreditCard(conn, creditCard);

		// Update the database
		conn.update("delete from payment.\"CreditCard\" where id=?", creditCard);

		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			business,
			AccountHandler.getHostsForAccount(conn, business),
			false
		);
	}

	public static void updateCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard,
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
		AccountHandler.checkPermission(conn, source, "updateCreditCard", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCard", creditCard);
		assert cardInfo != null || source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) < 0 : "Compatibility with older clients (before 1.82.0) that don't send any cardInfo";

		updateCreditCard(
			conn,
			invalidateList,
			creditCard,
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
		int creditCard,
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
			conn.update(
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
				creditCard
			);
		} else {
			conn.update(
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
				creditCard
			);
		}

		Account.Name account = getAccountForCreditCard(conn, creditCard);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void updateCreditCardNumberAndExpiration(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard,
		String maskedCardNumber,
		Byte expirationMonth,
		Short expirationYear,
		String encryptedCardNumber,
		int encryptionFrom,
		int encryptionRecipient
	) throws IOException, SQLException {
		// Permission checks
		AccountHandler.checkPermission(conn, source, "updateCreditCardNumberAndExpiration", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCardNumberAndExpiration", creditCard);

		if(encryptionFrom!=-1) checkAccessEncryptionKey(conn, source, "updateCreditCardNumberAndExpiration", encryptionFrom);
		if(encryptionRecipient!=-1) checkAccessEncryptionKey(conn, source, "updateCreditCardNumberAndExpiration", encryptionRecipient);

		if(encryptedCardNumber==null && encryptionFrom==-1 && encryptionRecipient==-1) {
			// Update row
			conn.update(
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
				creditCard
			);
		} else if(encryptedCardNumber!=null && encryptionFrom!=-1 && encryptionRecipient!=-1) {
			// Update row
			conn.update(
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
				creditCard
			);
		} else throw new SQLException("encryptedCardNumber, encryptionFrom, and encryptionRecipient must either all be null or none null");

		Account.Name account = getAccountForCreditCard(conn, creditCard);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void updateCreditCardExpiration(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard,
		Byte expirationMonth,
		Short expirationYear
	) throws IOException, SQLException {
		// Permission checks
		AccountHandler.checkPermission(conn, source, "updateCreditCardExpiration", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "updateCreditCardExpiration", creditCard);

		updateCreditCardExpiration(conn, invalidateList, creditCard, expirationMonth, expirationYear);
	}

	public static void updateCreditCardExpiration(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int creditCard,
		Byte expirationMonth,
		Short expirationYear
	) throws IOException, SQLException {
		// Update row
		conn.update(
			"update\n"
			+ "  payment.\"CreditCard\"\n"
			+ "set\n"
			+ "  \"expirationMonth\"=?,\n"
			+ "  \"expirationYear\"=?\n"
			+ "where\n"
			+ "  id=?",
			expirationMonth == null ? DatabaseAccess.Null.SMALLINT : expirationMonth.shortValue(),
			expirationYear == null ? DatabaseAccess.Null.SMALLINT : expirationYear,
			creditCard
		);

		Account.Name account = getAccountForCreditCard(conn, creditCard);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void reactivateCreditCard(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int creditCard
	) throws IOException, SQLException {
		// Permission checks
		AccountHandler.checkPermission(conn, source, "reactivateCreditCard", Permission.Name.edit_credit_card);
		checkAccessCreditCard(conn, source, "reactivateCreditCard", creditCard);

		// Update row
		conn.update(
			"update\n"
			+ "  payment.\"CreditCard\"\n"
			+ "set\n"
			+ "  active=true,\n"
			+ "  deactivated_on=null,\n"
			+ "  deactivate_reason=null\n"
			+ "where\n"
			+ "  id=?",
			creditCard
		);

		Account.Name account = getAccountForCreditCard(conn, creditCard);
		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void setCreditCardUseMonthly(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name account,
		int creditCard
	) throws IOException, SQLException {
		// Permission checks
		AccountHandler.checkPermission(conn, source, "setCreditCardUseMonthly", Permission.Name.edit_credit_card);
		AccountHandler.checkAccessAccount(conn, source, "setCreditCardUseMonthly", account);

		if(creditCard == -1) {
			// Clear only
			conn.update("update payment.\"CreditCard\" set use_monthly=false where accounting=? and use_monthly", account);
		} else {
			checkAccessCreditCard(conn, source, "setCreditCardUseMonthly", creditCard);

			// Make sure accounting codes match
			if(!account.equals(getAccountForCreditCard(conn, creditCard))) throw new SQLException("credit card and business accounting codes do not match");

			// Perform clear and set in one SQL statement - I thinks myself clever right now.
			conn.update("update payment.\"CreditCard\" set use_monthly=(id=?) where accounting=? and use_monthly!=(id=?)", creditCard, account, creditCard);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.CREDIT_CARDS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	/**
	 * Creates a new {@link Payment}.
	 */
	public static int addPayment(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String processor,
		Account.Name account,
		String groupName,
		boolean testMode,
		int duplicateWindow,
		String orderNumber,
		Money amount,
		Money taxAmount,
		boolean taxExempt,
		Money shippingAmount,
		Money dutyAmount,
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
		AccountHandler.checkPermission(conn, source, "addPayment", Permission.Name.add_credit_card_transaction);
		checkAccessProcessor(conn, source, "addPayment", processor);
		AccountHandler.checkAccessAccount(conn, source, "addPayment", account);
		AccountHandler.checkAccessAccount(conn, source, "addPayment", creditCardAccounting);
		AccountUserHandler.checkAccessUser(conn, source, "addPayment", creditCardCreatedBy);

		return addPayment(
			conn,
			invalidateList,
			processor,
			account,
			groupName,
			testMode,
			duplicateWindow,
			orderNumber,
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
			source.getCurrentAdministrator(),
			authorizationPrincipalName
		);
	}

	/**
	 * Creates a new {@link Payment}.
	 */
	public static int addPayment(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String processor,
		Account.Name account,
		String groupName,
		boolean testMode,
		int duplicateWindow,
		String orderNumber,
		Money amount,
		Money taxAmount,
		boolean taxExempt,
		Money shippingAmount,
		Money dutyAmount,
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
		Currency currency = amount.getCurrency();
		if(taxAmount != null && taxAmount.getCurrency() != currency) {
			throw new SQLException("Currency mismatch: amount.currency = " + currency + ", taxAmount.currency = " + taxAmount.getCurrency());
		}
		if(shippingAmount != null && shippingAmount.getCurrency() != currency) {
			throw new SQLException("Currency mismatch: amount.currency = " + currency + ", shippingAmount.currency = " + shippingAmount.getCurrency());
		}
		if(dutyAmount != null && dutyAmount.getCurrency() != currency) {
			throw new SQLException("Currency mismatch: amount.currency = " + currency + ", dutyAmount.currency = " + dutyAmount.getCurrency());
		}
		int payment = conn.updateInt(
			"INSERT INTO payment.\"Payment\" (\n"
			+ "  processor_id,\n"
			+ "  accounting,\n"
			+ "  group_name,\n"
			+ "  test_mode,\n"
			+ "  duplicate_window,\n"
			+ "  order_number,\n"
			+ "  currency,\n"
			+ "  amount,\n"
			+ "  \"taxAmount\",\n"
			+ "  tax_exempt,\n"
			+ "  \"shippingAmount\",\n"
			+ "  \"dutyAmount\",\n"
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
			+ ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PROCESSING') RETURNING id", // TODO: Use enum for PROCESSING
			processor,
			account,
			groupName,
			testMode,
			duplicateWindow,
			orderNumber,
			currency.getCurrencyCode(),
			amount.getValue(),
			taxAmount == null ? DatabaseAccess.Null.NUMERIC : taxAmount.getValue(),
			taxExempt,
			shippingAmount == null ? DatabaseAccess.Null.NUMERIC : shippingAmount.getValue(),
			dutyAmount == null ? DatabaseAccess.Null.NUMERIC : dutyAmount.getValue(),
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
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, account, InvalidateList.allHosts, false);
		return payment;
	}

	public static void paymentSaleCompleted(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int payment,
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
		Timestamp captureTime,
		String capturePrincipalName,
		String captureCommunicationResult,
		String captureProviderErrorCode,
		String captureErrorCode,
		String captureProviderErrorMessage,
		String captureProviderUniqueId,
		String status
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "paymentSaleCompleted", Permission.Name.credit_card_transaction_sale_completed);
		checkAccessPayment(conn, source, "paymentSaleCompleted", payment);
		// TODO: Are the principal names required to be AOServ objects, or are they arbitrary application-specific values?
		if(capturePrincipalName!=null) {
			try {
				AccountUserHandler.checkAccessUser(conn, source, "paymentSaleCompleted", User.Name.valueOf(capturePrincipalName));
			} catch(ValidationException e) {
				throw new SQLException(e.getLocalizedMessage(), e);
			}
		}

		paymentSaleCompleted(
			conn,
			invalidateList,
			payment,
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
			source.getCurrentAdministrator(),
			capturePrincipalName,
			captureCommunicationResult,
			captureProviderErrorCode,
			captureErrorCode,
			captureProviderErrorMessage,
			captureProviderUniqueId,
			status
		);
	}

	public static void paymentSaleCompleted(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int payment,
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
		Timestamp captureTime,
		User.Name captureUsername,
		String capturePrincipalName,
		String captureCommunicationResult,
		String captureProviderErrorCode,
		String captureErrorCode,
		String captureProviderErrorMessage,
		String captureProviderUniqueId,
		String status
	) throws IOException, SQLException {
		String processor = getProcessorForPayment(conn, payment);
		Account.Name account = getAccountForProcessor(conn, processor);

		int updated = conn.update(
			"update\n"
			+ "  payment.\"Payment\"\n"
			+ "set\n"
			+ "  authorization_communication_result=?::\"com.aoapps.payments\".\"TransactionResult.CommunicationResult\",\n"
			+ "  authorization_provider_error_code=?,\n"
			+ "  authorization_error_code=?::\"com.aoapps.payments\".\"TransactionResult.ErrorCode\",\n"
			+ "  authorization_provider_error_message=?,\n"
			+ "  authorization_provider_unique_id=?,\n"
			+ "  \"authorizationResult.providerReplacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.replacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.providerReplacementExpiration\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationMonth\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationYear\"=?,\n"
			+ "  authorization_provider_approval_result=?,\n"
			+ "  authorization_approval_result=?::\"com.aoapps.payments\".\"AuthorizationResult.ApprovalResult\",\n"
			+ "  authorization_provider_decline_reason=?,\n"
			+ "  authorization_decline_reason=?::\"com.aoapps.payments\".\"AuthorizationResult.DeclineReason\",\n"
			+ "  authorization_provider_review_reason=?,\n"
			+ "  authorization_review_reason=?::\"com.aoapps.payments\".\"AuthorizationResult.ReviewReason\",\n"
			+ "  authorization_provider_cvv_result=?,\n"
			+ "  authorization_cvv_result=?::\"com.aoapps.payments\".\"AuthorizationResult.CvvResult\",\n"
			+ "  authorization_provider_avs_result=?,\n"
			+ "  authorization_avs_result=?::\"com.aoapps.payments\".\"AuthorizationResult.AvsResult\",\n"
			+ "  authorization_approval_code=?,\n"
			+ "  capture_time=?::timestamp,\n"
			+ "  capture_username=?,\n"
			+ "  capture_principal_name=?,\n"
			+ "  capture_communication_result=?::\"com.aoapps.payments\".\"TransactionResult.CommunicationResult\",\n"
			+ "  capture_provider_error_code=?,\n"
			+ "  capture_error_code=?::\"com.aoapps.payments\".\"TransactionResult.ErrorCode\",\n"
			+ "  capture_provider_error_message=?,\n"
			+ "  capture_provider_unique_id=?,\n"
			+ "  status=?::\"com.aoapps.payments\".\"Transaction.Status\"\n"
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
			captureTime == null ? DatabaseAccess.Null.TIMESTAMP : captureTime,
			captureUsername,
			capturePrincipalName,
			captureCommunicationResult,
			captureProviderErrorCode,
			captureErrorCode,
			captureProviderErrorMessage,
			captureProviderUniqueId,
			status,
			payment
		);
		if(updated!=1) throw new SQLException("Unable to find payment.Payment with id="+payment+" and status in ('PROCESSING', 'AUTHORIZED')");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, account, InvalidateList.allHosts, false);
	}

	public static void paymentAuthorizeCompleted(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int payment,
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
		AccountHandler.checkPermission(conn, source, "paymentAuthorizeCompleted", Permission.Name.credit_card_transaction_authorize_completed);
		checkAccessPayment(conn, source, "paymentAuthorizeCompleted", payment);

		paymentAuthorizeCompleted(
			conn,
			invalidateList,
			payment,
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

	public static void paymentAuthorizeCompleted(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int payment,
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
		String processor = getProcessorForPayment(conn, payment);
		Account.Name account = getAccountForProcessor(conn, processor);

		int updated = conn.update(
			"update\n"
			+ "  payment.\"Payment\"\n"
			+ "set\n"
			+ "  authorization_communication_result=?::\"com.aoapps.payments\".\"TransactionResult.CommunicationResult\",\n"
			+ "  authorization_provider_error_code=?,\n"
			+ "  authorization_error_code=?::\"com.aoapps.payments\".\"TransactionResult.ErrorCode\",\n"
			+ "  authorization_provider_error_message=?,\n"
			+ "  authorization_provider_unique_id=?,\n"
			+ "  \"authorizationResult.providerReplacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.replacementMaskedCardNumber\"=?,\n"
			+ "  \"authorizationResult.providerReplacementExpiration\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationMonth\"=?,\n"
			+ "  \"authorizationResult.replacementExpirationYear\"=?,\n"
			+ "  authorization_provider_approval_result=?,\n"
			+ "  authorization_approval_result=?::\"com.aoapps.payments\".\"AuthorizationResult.ApprovalResult\",\n"
			+ "  authorization_provider_decline_reason=?,\n"
			+ "  authorization_decline_reason=?::\"com.aoapps.payments\".\"AuthorizationResult.DeclineReason\",\n"
			+ "  authorization_provider_review_reason=?,\n"
			+ "  authorization_review_reason=?::\"com.aoapps.payments\".\"AuthorizationResult.ReviewReason\",\n"
			+ "  authorization_provider_cvv_result=?,\n"
			+ "  authorization_cvv_result=?::\"com.aoapps.payments\".\"AuthorizationResult.CvvResult\",\n"
			+ "  authorization_provider_avs_result=?,\n"
			+ "  authorization_avs_result=?::\"com.aoapps.payments\".\"AuthorizationResult.AvsResult\",\n"
			+ "  authorization_approval_code=?,\n"
			+ "  status=?::\"com.aoapps.payments\".\"Transaction.Status\"\n"
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
			payment
		);
		if(updated!=1) throw new SQLException("Unable to find payment.Payment with id="+payment+" and status='PROCESSING'");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.CREDIT_CARD_TRANSACTIONS, account, InvalidateList.allHosts, false);
	}

	private static class AutomaticPayment {
		private final Account.Name account;
		private final Money amount;
		private final int cc_id;
		private final String cardInfo;
		private final Byte expirationMonth;
		private final Short expirationYear;
		private final String principalName;
		private final String groupName;
		private final String cc_providerUniqueId;
		private final String firstName;
		private final String lastName;
		private final String companyName;
		private final String email;
		private final String phone;
		private final String fax;
		private final String customerId;
		private final String customerTaxId;
		private final String streetAddress1;
		private final String streetAddress2;
		private final String city;
		private final String state;
		private final String postalCode;
		private final String countryCode;
		private final String description;
		private final String ccp_providerId;
		private final String ccp_className;
		private final String ccp_param1;
		private final String ccp_param2;
		private final String ccp_param3;
		private final String ccp_param4;

		private AutomaticPayment(
			Account.Name account,
			Money amount,
			int cc_id,
			String cardInfo,
			Byte expirationMonth,
			Short expirationYear,
			String principalName,
			String groupName,
			String cc_providerUniqueId,
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
			String description,
			String ccp_providerId,
			String ccp_className,
			String ccp_param1,
			String ccp_param2,
			String ccp_param3,
			String ccp_param4
		) {
			this.account = account;
			this.amount = amount;
			this.cc_id = cc_id;
			this.cardInfo = cardInfo;
			this.expirationMonth = expirationMonth;
			this.expirationYear = expirationYear;
			this.principalName = principalName;
			this.groupName = groupName;
			this.cc_providerUniqueId = cc_providerUniqueId;
			this.firstName = firstName;
			this.lastName = lastName;
			this.companyName = companyName;
			this.email = email;
			this.phone = phone;
			this.fax = fax;
			this.customerId = customerId;
			this.customerTaxId = customerTaxId;
			this.streetAddress1 = streetAddress1;
			this.streetAddress2 = streetAddress2;
			this.city = city;
			this.state = state;
			this.postalCode = postalCode;
			this.countryCode = countryCode;
			this.description = description;
			this.ccp_providerId = ccp_providerId;
			this.ccp_className = ccp_className;
			this.ccp_param1 = ccp_param1;
			this.ccp_param2 = ccp_param2;
			this.ccp_param3 = ccp_param3;
			this.ccp_param4 = ccp_param4;
		}
	}

	// TODO: infoOut, warningOut, verboseOut here, too
	private static void processAutomaticPayments(int month, int year) {
		System.err.println("DEBUG: month=" + year + "-" + month);
		try {
			try (
				ProcessTimer timer=new ProcessTimer(
					logger,
					PaymentHandler.class.getName(),
					"processAutomaticPayments",
					"CreditCardHandler - Process Automatic Payments",
					"Processes the automatic payments for the month",
					TIMER_MAX_TIME,
					TIMER_REMINDER_INTERVAL
				);
			) {
				MasterServer.executorService.submit(timer);

				// Find the beginning of the next month (for transaction search)
				GregorianCalendar beginningOfNextMonth = new GregorianCalendar(Type.DATE_TIME_ZONE);
				beginningOfNextMonth.set(Calendar.YEAR, year);
				beginningOfNextMonth.set(Calendar.MONTH, month-1);
				beginningOfNextMonth.set(Calendar.DAY_OF_MONTH, 1);
				beginningOfNextMonth.set(Calendar.HOUR_OF_DAY, 0);
				beginningOfNextMonth.set(Calendar.MINUTE, 0);
				beginningOfNextMonth.set(Calendar.SECOND, 0);
				beginningOfNextMonth.set(Calendar.MILLISECOND, 0);
				beginningOfNextMonth.add(Calendar.MONTH, 1);

				// Find the last microsecond of the current month - PostgreSQL has microsecond precision
				Timestamp lastMicrosecondOfMonth;
				{
					Calendar lastSecondOfTheMonth = (Calendar)beginningOfNextMonth.clone();
					lastSecondOfTheMonth.add(Calendar.SECOND, -1);
					lastMicrosecondOfMonth = new Timestamp(lastSecondOfTheMonth.getTimeInMillis());
					lastMicrosecondOfMonth.setNanos(999999000);
				}

				// Start the transaction
				try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
					InvalidateList invalidateList = new InvalidateList();
					// Find the accounting code, credit_card id, and account balances of all account.Account that have a credit card set for automatic payments (and is active)
					List<AutomaticPayment> automaticPayments = conn.queryCall(
						results -> {
							try {
								List<AutomaticPayment> list = new ArrayList<>();
								SortedMap<Currency, BigDecimal> totals = new TreeMap<>(CurrencyComparator.getInstance());
								// Look for duplicate accounting codes and report a warning
								Account.Name lastAccounting = null;
								while(results.next()) {
									Account.Name account = Account.Name.valueOf(results.getString("accounting"));
									if(account.equals(lastAccounting)) {
										logger.log(Level.WARNING, "More than one credit card marked as automatic for accounting={0}, using the first one found", account);
									} else {
										lastAccounting = account;
										Currency currency = Currency.getInstance(results.getString("currency"));
										BigDecimal endofmonth = results.getBigDecimal("endofmonth");
										if(endofmonth == null) endofmonth = BigDecimal.ZERO;
										BigDecimal current = results.getBigDecimal("current");
										if(current == null) current = BigDecimal.ZERO;
										if(
											endofmonth.compareTo(BigDecimal.ZERO) > 0
											&& current.compareTo(BigDecimal.ZERO) > 0
										) {
											BigDecimal amount = endofmonth.compareTo(current)<=0 ? endofmonth : current;
											BigDecimal total = totals.get(currency);
											total = (total == null) ? amount : total.add(amount);
											totals.put(currency, total);

											Byte expirationMonth = SafeMath.castByte(results.getShort("expirationMonth"));
											if(results.wasNull()) expirationMonth = null;
											Short expirationYear = results.getShort("expirationYear");
											if(results.wasNull()) expirationYear = null;
											list.add(
												new AutomaticPayment(
													account,
													new Money(currency, amount),
													results.getInt("cc_id"),
													results.getString("cardInfo"),
													expirationMonth,
													expirationYear,
													results.getString("principalName"),
													results.getString("groupName"),
													results.getString("cc_providerUniqueId"),
													results.getString("firstName"),
													results.getString("lastName"),
													results.getString("companyName"),
													results.getString("email"),
													results.getString("phone"),
													results.getString("fax"),
													results.getString("customerId"),
													results.getString("customerTaxId"),
													results.getString("streetAddress1"),
													results.getString("streetAddress2"),
													results.getString("city"),
													results.getString("state"),
													results.getString("postalCode"),
													results.getString("countryCode"),
													results.getString("description"),
													results.getString("ccp_providerId"),
													results.getString("ccp_className"),
													results.getString("ccp_param1"),
													results.getString("ccp_param2"),
													results.getString("ccp_param3"),
													results.getString("ccp_param4")
												)
											);
										}
									}
								}
								if(totals.isEmpty()) {
									assert list.isEmpty();
									System.out.println("Nothing to process");
								} else {
									for(Map.Entry<Currency, BigDecimal> entry : totals.entrySet()) {
										System.out.println("Processing a total of " + new Money(entry.getKey(), entry.getValue()));
									}
								}
								return list;
							} catch(ValidationException e) {
								throw new SQLException(e.getLocalizedMessage(), e);
							}
						},
						"select\n"
						+ "  bu.accounting,\n"
						+ "  c.\"currencyCode\" AS currency,\n"
						+ "  endofmonth.balance AS endofmonth,\n"
						+ "  current.balance AS current,\n"
						+ "  cc.id as cc_id,\n"
						+ "  cc.card_info as cardInfo,\n"
						+ "  cc.\"expirationMonth\",\n"
						+ "  cc.\"expirationYear\",\n"
						+ "  cc.principal_name as \"principalName\",\n"
						+ "  cc.group_name as \"groupName\",\n"
						+ "  cc.provider_unique_id as \"cc_providerUniqueId\",\n"
						+ "  cc.first_name as \"firstName\",\n"
						+ "  cc.last_name as \"lastName\",\n"
						+ "  cc.company_name as \"companyName\",\n"
						+ "  cc.email,\n"
						+ "  cc.phone,\n"
						+ "  cc.fax,\n"
						+ "  cc.\"customerId\",\n"
						+ "  cc.customer_tax_id as \"customerTaxId\",\n"
						+ "  cc.street_address1 as \"streetAddress1\",\n"
						+ "  cc.street_address2 as \"streetAddress2\",\n"
						+ "  cc.city,\n"
						+ "  cc.state,\n"
						+ "  cc.postal_code as \"postalCode\",\n"
						+ "  cc.country_code as \"countryCode\",\n"
						+ "  cc.description,\n"
						+ "  ccp.provider_id as \"ccp_providerId\",\n"
						+ "  ccp.class_name as \"ccp_className\",\n"
						+ "  ccp.param1 as \"ccp_param1\",\n"
						+ "  ccp.param2 as \"ccp_param2\",\n"
						+ "  ccp.param3 as \"ccp_param3\",\n"
						+ "  ccp.param4 as \"ccp_param4\"\n"
						+ "from\n"
						+ "  account.\"Account\" bu\n"
						+ "  CROSS JOIN billing.\"Currency\" c\n"
						+ "  INNER JOIN payment.\"CreditCard\" cc ON bu.accounting=cc.accounting\n"
						+ "  INNER JOIN payment.\"Processor\" ccp ON cc.processor_id=ccp.provider_id\n"
						+ "  LEFT JOIN (\n"
						+ "    SELECT\n"
						+ "      bu.accounting,\n"
						+ "      tr.\"rate.currency\" AS currency,\n"
						+ "      sum(\n"
						+ "        round(\n"
						+ "          tr.quantity * tr.\"rate.value\",\n"
						+ "          c2.\"fractionDigits\"\n"
						+ "        )\n"
						+ "      ) AS balance\n"
						+ "    FROM\n"
						+ "                account.\"Account\"     bu\n"
						+ "      LEFT JOIN billing.\"Transaction\" tr ON bu.accounting = tr.accounting\n"
						+ "      LEFT JOIN billing.\"Currency\"    c2 ON tr.\"rate.currency\" = c2.\"currencyCode\"\n"
						+ "    WHERE\n"
						+ "      tr.payment_confirmed != 'N'\n"
						+ "      AND tr.time < ?\n"
						+ "    GROUP BY\n"
						+ "      bu.accounting,\n"
						+ "      tr.\"rate.currency\"\n"
						+ "  ) AS endofmonth ON (bu.accounting, c.\"currencyCode\")=(endofmonth.accounting, endofmonth.currency)\n"
						+ "  LEFT JOIN (\n"
						+ "    SELECT\n"
						+ "      bu.accounting,\n"
						+ "      tr.\"rate.currency\" AS currency,\n"
						+ "      sum(\n"
						+ "        round(\n"
						+ "          tr.quantity * tr.\"rate.value\",\n"
						+ "          c2.\"fractionDigits\"\n"
						+ "        )\n"
						+ "      ) AS balance\n"
						+ "    FROM\n"
						+ "                account.\"Account\"     bu\n"
						+ "      LEFT JOIN billing.\"Transaction\" tr ON bu.accounting = tr.accounting\n"
						+ "      LEFT JOIN billing.\"Currency\"    c2 ON tr.\"rate.currency\" = c2.\"currencyCode\"\n"
						+ "    WHERE\n"
						+ "      tr.payment_confirmed != 'N'\n"
						+ "    GROUP BY\n"
						+ "      bu.accounting,\n"
						+ "      tr.\"rate.currency\"\n"
						+ "  ) AS current ON (bu.accounting, c.\"currencyCode\")=(current.accounting, current.currency)\n"
						+ "WHERE\n"
						+ "  cc.use_monthly\n"
						+ "  AND cc.active\n"
						+ "  AND ccp.enabled\n"
						+ "  AND (\n"
						+ "    endofmonth.balance IS NOT NULL\n"
						+ "    OR current.balance IS NOT NULL\n"
						+ "  )\n"
						+ "ORDER BY\n"
						+ "  bu.accounting,\n"
						+ "  c.\"currencyCode\"",
						new Timestamp(beginningOfNextMonth.getTimeInMillis())
					);
					// Only need to create the persistence once per DB transaction
					MasterPersistenceMechanism masterPersistenceMechanism = new MasterPersistenceMechanism(conn, invalidateList);
					for(AutomaticPayment automaticPayment : automaticPayments) {
						System.out.println("accounting="+automaticPayment.account);
						System.out.println("    amount="+automaticPayment.amount);
						// Find the processor
						CreditCardProcessor processor = new CreditCardProcessor(
							MerchantServicesProviderFactory.getMerchantServicesProvider(
								automaticPayment.ccp_providerId,
								automaticPayment.ccp_className,
								automaticPayment.ccp_param1,
								automaticPayment.ccp_param2,
								automaticPayment.ccp_param3,
								automaticPayment.ccp_param4
							),
							masterPersistenceMechanism
						);
						System.out.println("    processor="+processor.getProviderId());

						// Add as pending transaction
						String paymentTypeName;
						String cardInfo = automaticPayment.cardInfo;
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
						int transID = BillingTransactionHandler.addTransaction(
							conn,
							invalidateList,
							'T',
							lastMicrosecondOfMonth,
							automaticPayment.account,
							automaticPayment.account,
							MasterPersistenceMechanism.MASTER_BUSINESS_ADMINISTRATOR,
							TransactionType.PAYMENT,
							"Monthly automatic billing",
							new BigDecimal("1.000"),
							automaticPayment.amount.negate(),
							paymentTypeName,
							CreditCard.getCardNumberDisplay(cardInfo),
							automaticPayment.ccp_providerId,
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
								automaticPayment.amount.getCurrency(),
								automaticPayment.amount.getValue(),
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
								Integer.toString(automaticPayment.cc_id),
								automaticPayment.principalName,
								automaticPayment.groupName,
								automaticPayment.ccp_providerId,
								automaticPayment.cc_providerUniqueId,
								null, // cardNumber
								automaticPayment.cardInfo,
								automaticPayment.expirationMonth == null ? CreditCard.UNKNOWN_EXPIRATION_MONTH : automaticPayment.expirationMonth,
								automaticPayment.expirationYear == null ? CreditCard.UNKNOWN_EXPIRATION_YEAR : automaticPayment.expirationYear,
								null, // cardCode
								automaticPayment.firstName,
								automaticPayment.lastName,
								automaticPayment.companyName,
								automaticPayment.email,
								automaticPayment.phone,
								automaticPayment.fax,
								automaticPayment.customerId,
								automaticPayment.customerTaxId,
								automaticPayment.streetAddress1,
								automaticPayment.streetAddress2,
								automaticPayment.city,
								automaticPayment.state,
								automaticPayment.postalCode,
								automaticPayment.countryCode,
								automaticPayment.description
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
								BillingTransactionHandler.transactionDeclined(
									conn,
									invalidateList,
									transID,
									Integer.parseInt(transaction.getPersistenceUniqueId()),
									tokenizedCreditCard == null ? null : CreditCard.getCardNumberDisplay(tokenizedCreditCard.getReplacementMaskedCardNumber())
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
										BillingTransactionHandler.transactionHeld(
											conn,
											invalidateList,
											transID,
											Integer.parseInt(transaction.getPersistenceUniqueId()),
											tokenizedCreditCard == null ? null : CreditCard.getCardNumberDisplay(tokenizedCreditCard.getReplacementMaskedCardNumber())
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
										BillingTransactionHandler.transactionDeclined(
											conn,
											invalidateList,
											transID,
											Integer.parseInt(transaction.getPersistenceUniqueId()),
											tokenizedCreditCard == null ? null : CreditCard.getCardNumberDisplay(tokenizedCreditCard.getReplacementMaskedCardNumber())
										);
										conn.commit();
										System.out.println("    Result: Declined");
										System.out.println("    Decline Reason: "+authorizationResult.getDeclineReason());
										break;
									}
									case APPROVED : {
										// Update transaction as successful
										BillingTransactionHandler.transactionApproved(
											conn,
											invalidateList,
											transID,
											Integer.parseInt(transaction.getPersistenceUniqueId()),
											tokenizedCreditCard == null ? null : CreditCard.getCardNumberDisplay(tokenizedCreditCard.getReplacementMaskedCardNumber())
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
					conn.commit();
					MasterServer.invalidateTables(conn, invalidateList, null);
				}
			}
		} catch(ThreadDeath td) {
			throw td;
		} catch(Throwable t) {
			logger.log(Level.SEVERE, null, t);
		}
	}

	// TODO: Synchronize should become its own service, once we've moved this handler into the new service architecture
	private static void synchronizeStoredCards(PrintWriter verboseOut, PrintWriter infoOut, PrintWriter warningOut, boolean dryRun) throws IOException, SQLException {
		try (
			ProcessTimer timer = new ProcessTimer(
				logger,
				PaymentHandler.class.getName(),
				"synchronizeStoredCards",
				"CreditCardHandler - Synchronize Stored Cards",
				"Synchronizes any updated masked card numbers or expiration dates from the payment providers back to local persistence",
				TIMER_MAX_TIME,
				TIMER_REMINDER_INTERVAL
			);
		) {
			MasterServer.executorService.submit(timer);

			// Start the transaction
			try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
				InvalidateList invalidateList = new InvalidateList();
				if(infoOut != null) infoOut.println(PaymentHandler.class.getSimpleName() + ".synchronizeStoredCards: Synchronizing stored cards");

				// Find the accounting code, credit_card id, and account balances of all account.Account that have a credit card set for automatic payments (and is active)
				List<MerchantServicesProvider> providers = conn.queryList(
					result -> {
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
				if(infoOut != null) infoOut.println(PaymentHandler.class.getSimpleName() + ".synchronizeStoredCards: Found " + providers.size() + " enabled " + (providers.size() == 1 ? "payment processor" : "payment processors"));

				// Find all the stored cards
				MasterPersistenceMechanism masterPersistenceMechanism = new MasterPersistenceMechanism(conn, invalidateList);

				// Only need to create the persistence once per DB transaction
				for(MerchantServicesProvider provider : providers) {
					CreditCardProcessor processor = new CreditCardProcessor(provider, masterPersistenceMechanism);
					processor.synchronizeStoredCards(null, verboseOut, infoOut, warningOut, dryRun);
				}
				conn.commit();
				MasterServer.invalidateTables(conn, invalidateList, null);
			}
		}
	}

	/**
	 * Runs at 11:34 pm daily.
	 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) ->
		hour == 23 && minute == 34;

	private static final CronJob synchronizeStoredCardsCronJob = new CronJob() {
		@Override
		public Schedule getSchedule() {
			return schedule;
		}

		@Override
		public String getName() {
			return PaymentHandler.class.getSimpleName() + ".synchronizeStoredCards";
		}

		@Override
		public int getThreadPriority() {
			return Thread.NORM_PRIORITY - 2;
		}

		@Override
		public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
			StringWriter warningBuffer = new StringWriter();
			try (PrintWriter warningOut = new PrintWriter(warningBuffer)) {
				synchronizeStoredCards(null, null, warningOut, false);
				StringBuffer buff = warningBuffer.getBuffer();
				if(buff.length() != 0) {
					// Generate warning ticket
					logger.log(Level.WARNING, buff.toString());
				}
			} catch(ThreadDeath td) {
				throw td;
			} catch(Throwable t) {
				// Log failure in ticket
				logger.log(Level.SEVERE, null, t);
			}
		}
	};

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
				try {
					PrintWriter out = new PrintWriter(System.out, true);
					synchronizeStoredCards(
						verbose ? out : null,
						quiet ? null : out,
						new PrintWriter(System.err, true),
						dryRun
					);
					exitStatus = 0;
				} catch(IOException e) {
					e.printStackTrace(System.err);
					exitStatus = SysExits.EX_IOERR;
				} catch(SQLException e) {
					e.printStackTrace(System.err);
					exitStatus = SysExits.EX_DATAERR;
				}
			} else {
				// Not y10k compliant
				if(command.length()==7) {
					if(command.charAt(4)=='-') {
						try {
							int year = Integer.parseInt(command.substring(0, 4));
							try {
								int month = Integer.parseInt(command.substring(5, 7));
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
			System.err.println("usage: " + PaymentHandler.class.getName() + " [--verbose] [--quiet] [--dry-run] {synchronize|YYYY-MM}");
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
		GregorianCalendar gcal = new GregorianCalendar(Type.DATE_TIME_ZONE);
		gcal.set(Calendar.YEAR, year);
		gcal.set(Calendar.MONTH, month-1);
		gcal.set(Calendar.DAY_OF_MONTH, 1);
		gcal.set(Calendar.HOUR_OF_DAY, 0);
		gcal.set(Calendar.MINUTE, 0);
		gcal.set(Calendar.SECOND, 0);
		gcal.set(Calendar.MILLISECOND, 0);
		gcal.add(Calendar.MONTH, -1);
		// Process for last month
		processAutomaticPayments(gcal.get(Calendar.MONTH) + 1, gcal.get(Calendar.YEAR));
	}
	*/
}
