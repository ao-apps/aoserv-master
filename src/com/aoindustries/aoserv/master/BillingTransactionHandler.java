/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.billing.Currency;
import com.aoindustries.aoserv.client.billing.Transaction;
import com.aoindustries.aoserv.client.billing.TransactionSearchCriteria;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.lang.Strings;
import com.aoindustries.util.i18n.Money;
import com.aoindustries.util.i18n.Monies;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The <code>TransactionHandler</code> handles all the accesses to the transaction tables.
 *
 * @author  AO Industries, Inc.
 */
final public class BillingTransactionHandler {

	private BillingTransactionHandler() {
	}

	public static boolean canAccessTransaction(DatabaseConnection conn, RequestSource source, int transaction) throws IOException, SQLException {
		return AccountHandler.canAccessAccount(conn, source, getAccountForTransaction(conn, transaction));
	}

	public static void checkAccessTransaction(DatabaseConnection conn, RequestSource source, String action, int transaction) throws IOException, SQLException {
		AccountHandler.checkAccessAccount(conn, source, action, getAccountForTransaction(conn, transaction));
	}

	/**
	 * Adds a transaction.
	 */
	public static int addTransaction(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		char timeType,
		Timestamp time,
		Account.Name account,
		Account.Name sourceAccount,
		com.aoindustries.aoserv.client.account.User.Name administrator,
		String type,
		String description,
		int quantity,
		Money rate,
		String paymentType,
		String paymentInfo,
		String processor,
		byte payment_confirmed
	) throws IOException, SQLException {
		BankAccountHandler.checkIsAccounting(conn, source, "addTransaction");
		AccountHandler.checkAccessAccount(conn, source, "addTransaction", account);
		AccountHandler.checkAccessAccount(conn, source, "addTransaction", sourceAccount);
		AccountUserHandler.checkAccessUser(conn, source, "addTransaction", administrator);
		if(administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add Transaction for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		return addTransaction(
			conn,
			invalidateList,
			timeType,
			time,
			account,
			sourceAccount,
			administrator,
			type,
			description,
			BigDecimal.valueOf(quantity, 3),
			rate,
			paymentType,
			paymentInfo,
			processor,
			payment_confirmed
		);
	}

	/**
	 * Adds a transaction.
	 */
	public static int addTransaction(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		char timeType,
		Timestamp time,
		Account.Name account,
		Account.Name sourceAccount,
		com.aoindustries.aoserv.client.account.User.Name administrator,
		String type,
		String description,
		BigDecimal quantity,
		Money rate,
		String paymentType,
		String paymentInfo,
		String processor,
		byte payment_confirmed
	) throws IOException, SQLException {
		if(administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add Transaction for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

		int transaction;
		if(time == null) {
			String function;
			if(timeType == 'D') {
				function = "CURRENT_DATE";
			} else if(timeType == 'T') {
				function = "NOW()";
			} else {
				throw new IllegalArgumentException("Unexpected value for timeType: " + timeType);
			}
			transaction = conn.executeIntUpdate(
				"INSERT INTO billing.\"Transaction\" VALUES (" + function + ",default,?,?,?,?,?,?,?,?,?,?,?,null,?) RETURNING transid",
				account,
				sourceAccount,
				administrator,
				type,
				description,
				quantity,
				rate.getCurrency().getCurrencyCode(),
				rate.getValue(),
				paymentType,
				paymentInfo,
				processor,
				payment_confirmed==Transaction.CONFIRMED?"Y":payment_confirmed==Transaction.NOT_CONFIRMED?"N":"W"
			);
		} else {
			String cast;
			if(timeType == 'D') {
				cast = "::date";
			} else if(timeType == 'T') {
				cast = "";
			} else {
				throw new IllegalArgumentException("Unexpected value for timeType: " + timeType);
			}
			transaction = conn.executeIntUpdate(
				"INSERT INTO billing.\"Transaction\" VALUES (?" + cast + ",default,?,?,?,?,?,?,?,?,?,?,?,null,?) RETURNING transid",
				time,
				account,
				sourceAccount,
				administrator,
				type,
				description,
				quantity,
				rate.getCurrency().getCurrencyCode(),
				rate.getValue(),
				paymentType,
				paymentInfo,
				processor,
				payment_confirmed==Transaction.CONFIRMED?"Y":payment_confirmed==Transaction.NOT_CONFIRMED?"N":"W"
			);
		}

		// Notify all clients of the updates
		invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, account, AccountHandler.getHostsForAccount(conn, account), false);
		return transaction;
	}

	/**
	 * Gets the balance for one account.
	 */
	public static void getAccountBalance(
		DatabaseConnection conn,
		RequestSource source, 
		StreamableOutput out,
		Account.Name account
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			// TODO: release conn before writing to out
			MasterServer.writePenniesCheckBusiness(
				conn,
				source,
				"getAccountBalance",
				account,
				out,
				"SELECT coalesce(sum(cast((quantity * \"rate.value\") as numeric(9,2))), 0)\n"
				+ "FROM billing.\"Transaction\"\n"
				+ "WHERE accounting=? AND \"rate.currency\"=? AND payment_confirmed!='N'",
				account.toString(),
				Currency.USD.getCurrencyCode()
			);
		} else {
			throw new IOException("getAccountBalance only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	/**
	 * Gets the balance for one account.
	 */
	public static void getAccountBalanceBefore(
		DatabaseConnection conn,
		RequestSource source, 
		StreamableOutput out, 
		Account.Name account,
		long before
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			// TODO: release conn before writing to out
			MasterServer.writePenniesCheckBusiness(
				conn,
				source,
				"getAccountBalanceBefore",
				account,
				out,
				"SELECT coalesce(sum(cast((quantity * \"rate.value\") as numeric(9,2))), 0)\n"
				+ "FROM billing.\"Transaction\"\n"
				+ "WHERE accounting=? AND \"rate.currency\"=? and \"time\"<? AND payment_confirmed!='N'",
				account.toString(),
				Currency.USD.getCurrencyCode(),
				new Timestamp(before)
			);
		} else {
			throw new IOException("getAccountBalanceBefore only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	/**
	 * Gets the confirmed balance for one account.
	 */
	public static void getConfirmedAccountBalance(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		Account.Name account
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			// TODO: release conn before writing to out
			MasterServer.writePenniesCheckBusiness(
				conn,
				source,
				"getConfirmedAccountBalance",
				account,
				out,
				"SELECT coalesce(sum(cast((quantity * \"rate.value\") as numeric(9,2))), 0)\n"
				+ "FROM billing.\"Transaction\"\n"
				+ "WHERE accounting=? AND \"rate.currency\"=? AND payment_confirmed='Y'",
				account.toString(),
				Currency.USD.getCurrencyCode()
			);
		} else {
			throw new IOException("getConfirmedAccountBalance only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	/**
	 * Gets the confirmed balance for one account.
	 */
	public static Monies getConfirmedAccountBalance(
		DatabaseConnection conn,
		Account.Name account
	) throws IOException, SQLException {
		return Monies.of(
			conn.executeObjectCollectionQuery(
				new ArrayList<>(),
				ObjectFactories.moneyFactory,
				"SELECT\n"
				+ "  t.\"rate.currency\"\n"
				+ "  sum(\n"
				+ "    round(\n"
				+ "      t.quantity * t.\"rate.value\",\n"
				+ "      c.\"fractionDigits\"\n"
				+ "    )\n"
				+ "  )\n"
				+ "FROM\n"
				+ "  billing.\"Transaction\" t\n"
				+ "  INNER JOIN billing.\"Currency\" c ON t.\"rate.currency\" = c.\"currencyCode\"\n"
				+ "WHERE\n"
				+ "  t.accounting=?\n"
				+ "  AND t.payment_confirmed='Y'\n"
				+ "GROUP BY t.\"rate.currency\"",
				account.toString()
			)
		);
	}

	/**
	 * Gets the confirmed balance for one account.
	 */
	public static void getConfirmedAccountBalanceBefore(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		Account.Name account,
		long before
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			// TODO: release conn before writing to out
			MasterServer.writePenniesCheckBusiness(
				conn,
				source,
				"getConfirmedAccountBalanceBefore",
				account,
				out,
				"SELECT coalesce(sum(cast((quantity * \"rate.value\") as numeric(9,2))), 0)\n"
				+ "FROM billing.\"Transaction\"\n"
				+ "WHERE accounting=? AND \"rate.currency\"=? and \"time\"<? AND payment_confirmed='Y'",
				account.toString(),
				Currency.USD.getCurrencyCode(),
				new Timestamp(before)
			);
		} else {
			throw new IOException("getConfirmedAccountBalanceBefore only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	/**
	 * Gets all billing.Transaction for one business.
	 */
	public static void getTransactionsForAccount(
		DatabaseConnection conn,
		RequestSource source, 
		StreamableOutput out,
		boolean provideProgress,
		Account.Name account
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
			User masterUser=MasterServer.getUser(conn, currentAdministrator);
			UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, currentAdministrator);
			if(masterUser!=null) {
				if(masterServers.length==0) {
					// TODO: release conn before writing to out
					MasterServer.writeObjects(
						conn,
						source,
						out,
						provideProgress,
						CursorMode.AUTO,
						new Transaction(),
						"SELECT * FROM billing.\"Transaction\" WHERE accounting=? AND \"rate.currency\"=?",
						account,
						Currency.USD.getCurrencyCode()
					);
				}else {
					conn.releaseConnection();
					MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
				}
			} else {
				// TODO: release conn before writing to out
				MasterServer.writeObjects(
					conn,
					source,
					out,
					provideProgress,
					CursorMode.AUTO,
					new Transaction(),
					"SELECT\n"
					+ "  tr.*\n"
					+ "FROM\n"
					+ "  account.\"User\" un1,\n"
					+ "  billing.\"Package\" pk1,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"Transaction\" tr\n"
					+ "WHERE\n"
					+ "  un1.username=?\n"
					+ "  AND un1.package=pk1.name\n"
					+ "  AND (\n"
					+ TableHandler.PK1_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  AND bu1.accounting=tr.accounting\n"
					+ "  AND tr.accounting=?\n"
					+ "  AND tr.\"rate.currency\"=?",
					currentAdministrator,
					account,
					Currency.USD.getCurrencyCode()
				);
			}
		} else {
			throw new IOException("getTransactionsForAccount only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	/**
	 * Gets all billing.Transaction for one business administrator.
	 */
	public static void getTransactionsForAdministrator(
		DatabaseConnection conn,
		RequestSource source, 
		StreamableOutput out,
		boolean provideProgress,
		com.aoindustries.aoserv.client.account.User.Name administrator
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			AccountUserHandler.checkAccessUser(conn, source, "getTransactionsForAdministrator", source.getCurrentAdministrator());

			// TODO: release conn before writing to out
			MasterServer.writeObjects(
				conn,
				source,
				out,
				provideProgress,
				CursorMode.FETCH,
				new Transaction(),
				"SELECT * FROM billing.\"Transaction\" WHERE username=? AND \"rate.currency\"=?",
				administrator,
				Currency.USD.getCurrencyCode()
			);
		} else {
			throw new IOException("getTransactionsForAdministrator only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	public static void getTransactionsSearch(
		DatabaseConnection conn,
		RequestSource source,
		StreamableOutput out,
		boolean provideProgress,
		TransactionSearchCriteria criteria
	) throws IOException, SQLException {
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
			com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
			User masterUser=MasterServer.getUser(conn, currentAdministrator);
			UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, currentAdministrator);
			StringBuilder sql;
			final List<Object> params = new ArrayList<>();
			if(masterUser!=null) {
				if(masterServers.length==0) {
					sql = new StringBuilder(
						"SELECT\n"
						+ "  tr.*\n"
						+ "FROM\n"
						+ "  billing.\"Transaction\" tr\n"
						+ "WHERE\n"
						+ "  tr.\"rate.currency\"=?"
					);
					params.add(Currency.USD.getCurrencyCode());
				} else {
					conn.releaseConnection();
					MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
					return;
				}
			} else {
				sql = new StringBuilder(
					"SELECT\n"
					+ "  tr.*\n"
					+ "FROM\n"
					+ "  account.\"User\" un1,\n"
					+ "  billing.\"Package\" pk1,\n"
					+ TableHandler.BU1_PARENTS_JOIN
					+ "  billing.\"Transaction\" tr\n"
					+ "WHERE\n"
					+ "  un1.username=?\n"
					+ "  AND un1.package=pk1.name\n"
					+ "  AND (\n"
					+ TableHandler.PK1_BU1_PARENTS_WHERE
					+ "  )\n"
					+ "  AND bu1.accounting=tr.accounting\n"
					+ "  AND tr.\"rate.currency\"=?"
				);
				params.add(source.getCurrentAdministrator());
				params.add(Currency.USD.getCurrencyCode());
			}

			if (criteria.getAfter() != null) {
				sql.append("  AND tr.time>=?\n");
				params.add(criteria.getAfter());
			}
			if (criteria.getBefore() != null) {
				sql.append("  AND tr.time<?\n");
				params.add(criteria.getBefore());
			}
			if (criteria.getTransid() != TransactionSearchCriteria.ANY) {
				sql.append("  AND tr.transid=?\n");
				params.add(criteria.getTransid());
			}
			if (criteria.getAccount() != null) {
				sql.append("  AND tr.accounting=?\n");
				params.add(criteria.getAccount());
			}
			if (criteria.getSourceAccount() != null) {
				sql.append("  AND tr.source_accounting=?\n");
				params.add(criteria.getSourceAccount());
			}
			if (criteria.getAdministrator() != null) {
				sql.append("  AND tr.username=?\n");
				params.add(criteria.getAdministrator());
			}
			if (criteria.getType() != null) {
				sql.append("  AND tr.type=?\n");
				params.add(criteria.getType());
			}

			// description words
			if (criteria.getDescription() != null && criteria.getDescription().length() > 0) {
				String[] descriptionWords = Strings.splitString(criteria.getDescription());
				for (String descriptionWord : descriptionWords) {
					sql.append("  AND lower(tr.description) like ('%' || lower(?) || '%')\n");
					params.add(descriptionWord);
				}
			}

			if (criteria.getPaymentType() != null) {
				sql.append("  AND tr.payment_type=?\n");
				params.add(criteria.getPaymentType());
			}

			// payment_info words
			if (criteria.getPaymentInfo() != null && criteria.getPaymentInfo().length() > 0) {
				String[] paymentInfoWords = Strings.splitString(criteria.getPaymentInfo());
				for (String paymentInfoWord : paymentInfoWords) {
					sql.append("  AND lower(tr.payment_info) like ('%' || lower(?) || '%')\n");
					params.add(paymentInfoWord);
				}
			}

			// payment_confirmed
			if (criteria.getPaymentConfirmed() != TransactionSearchCriteria.ANY) {
				sql.append("  AND tr.payment_confirmed=?\n");
				String dbValue;
				switch(criteria.getPaymentConfirmed()) {
					case Transaction.CONFIRMED :
						dbValue = "Y";
						break;
					case Transaction.NOT_CONFIRMED :
						dbValue = "N";
						break;
					case Transaction.WAITING_CONFIRMATION :
						dbValue = "W";
						break;
					default :
						throw new AssertionError();
				}
				params.add(dbValue);
			}

			Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
			try (
				PreparedStatement pstmt = dbConn.prepareStatement(
					sql.toString(),
					provideProgress ? ResultSet.TYPE_SCROLL_SENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY
				)
			) {
				DatabaseConnection.setParams(dbConn, pstmt, params.toArray());
				try (ResultSet results = pstmt.executeQuery()) {
					// TODO: Call other writeObjects, passing sql and parameters, to support cursor/fetch?
					// TODO: release conn before writing to out
					MasterServer.writeObjects(source, out, provideProgress, new Transaction(), results);
				}
			}
		} else {
			throw new IOException("getTransactionsSearch only supported for protocol < " + AoservProtocol.Version.VERSION_1_83_0);
		}
	}

	public static void transactionApproved(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		BankAccountHandler.checkIsAccounting(conn, source, "transactionApproved");
		checkAccessTransaction(conn, source, "transactionApproved", transaction);
		PaymentHandler.checkAccessPayment(conn, source, "transactionApproved", payment);

		transactionApproved(conn, invalidateList, transaction, payment, paymentInfo);
	}

	public static void transactionApproved(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		Account.Name account = getAccountForTransaction(conn, transaction);
		int updateCount;
		if(paymentInfo == null) {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_confirmed='Y' where transid=? and payment_confirmed='W'",
				payment,
				transaction
			);
		} else {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=?, payment_confirmed='Y' where transid=? and payment_confirmed='W'",
				payment,
				paymentInfo,
				transaction
			);
		}
		if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transaction+" and payment_confirmed='W'");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, account, InvalidateList.allHosts, false);
	}

	public static void transactionDeclined(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		BankAccountHandler.checkIsAccounting(conn, source, "transactionDeclined");
		checkAccessTransaction(conn, source, "transactionDeclined", transaction);
		PaymentHandler.checkAccessPayment(conn, source, "transactionApproved", payment);

		transactionDeclined(conn, invalidateList, transaction, payment, paymentInfo);
	}

	public static void transactionDeclined(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		Account.Name account = getAccountForTransaction(conn, transaction);

		int updateCount;
		if(paymentInfo == null) {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_confirmed='N' where transid=? and payment_confirmed='W'",
				payment,
				transaction
			);
		} else {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=?, payment_confirmed='N' where transid=? and payment_confirmed='W'",
				payment,
				paymentInfo,
				transaction
			);
		}
		if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transaction+" and payment_confirmed='W'");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, account, InvalidateList.allHosts, false);
	}

	public static void transactionHeld(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		BankAccountHandler.checkIsAccounting(conn, source, "transactionHeld");
		checkAccessTransaction(conn, source, "transactionHeld", transaction);
		PaymentHandler.checkAccessPayment(conn, source, "transactionHeld", payment);

		transactionHeld(conn, invalidateList, transaction, payment, paymentInfo);
	}

	public static void transactionHeld(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int transaction,
		int payment,
		String paymentInfo
	) throws IOException, SQLException {
		Account.Name account = getAccountForTransaction(conn, transaction);

		int updateCount;
		if(paymentInfo == null) {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=? where transid=? and payment_confirmed='W' and credit_card_transaction is null",
				payment,
				transaction
			);
		} else {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=? where transid=? and payment_confirmed='W' and credit_card_transaction is null",
				payment,
				paymentInfo,
				transaction
			);
		}
		if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transaction+" and payment_confirmed='W' and credit_card_transaction is null");

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, account, InvalidateList.allHosts, false);
	}

	public static Account.Name getAccountForTransaction(DatabaseConnection conn, int transaction) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select accounting from billing.\"Transaction\" where transid=?",
			transaction
		);
	}
}
