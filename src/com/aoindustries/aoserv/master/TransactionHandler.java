/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.billing.Transaction;
import com.aoindustries.aoserv.client.billing.TransactionSearchCriteria;
import com.aoindustries.aoserv.client.billing.TransactionType;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.util.StringUtility;
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
final public class TransactionHandler {

    private TransactionHandler() {
    }

    public static boolean canAccessTransaction(DatabaseConnection conn, RequestSource source, int transid) throws IOException, SQLException {
        return BusinessHandler.canAccessBusiness(conn, source, getBusinessForTransaction(conn, transid));
    }

    public static void checkAccessTransaction(DatabaseConnection conn, RequestSource source, String action, int transid) throws IOException, SQLException {
        BusinessHandler.checkAccessBusiness(conn, source, action, getBusinessForTransaction(conn, transid));
    }

    /**
     * Adds a transaction.
     */
    public static int addTransaction(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        Account.Name accounting,
        Account.Name sourceAccounting,
        com.aoindustries.aoserv.client.account.User.Name business_administrator,
        String type,
        String description,
        int quantity,
        int rate,
        String paymentType,
        String paymentInfo,
        String processor,
        byte payment_confirmed
    ) throws IOException, SQLException {
        BankAccountHandler.checkAccounting(conn, source, "addTransaction");
        BusinessHandler.checkAccessBusiness(conn, source, "addTransaction", accounting);
        BusinessHandler.checkAccessBusiness(conn, source, "addTransaction", sourceAccounting);
        UsernameHandler.checkAccessUsername(conn, source, "addTransaction", business_administrator);
        if(business_administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add Transaction for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

        return addTransaction(
            conn,
            invalidateList,
            new Timestamp(System.currentTimeMillis()),
            accounting,
            sourceAccounting,
            business_administrator,
            type,
            description,
            new BigDecimal(SQLUtility.getMilliDecimal(quantity)),
            new BigDecimal(SQLUtility.getDecimal(rate)),
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
        Timestamp time,
        Account.Name accounting,
        Account.Name sourceAccounting,
        com.aoindustries.aoserv.client.account.User.Name business_administrator,
        String type,
        String description,
        BigDecimal quantity,
        BigDecimal rate,
        String paymentType,
        String paymentInfo,
        String processor,
        byte payment_confirmed
    ) throws IOException, SQLException {
        if(business_administrator.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) throw new SQLException("Not allowed to add Transaction for user '"+com.aoindustries.aoserv.client.linux.User.MAIL+'\'');

        int transid = conn.executeIntUpdate(
            "INSERT INTO billing.\"Transaction\" VALUES (?,default,?,?,?,?,?,?,?,?,?,?,null,?) RETURNING transid",
            time,
            accounting,
            sourceAccounting,
            business_administrator,
            type,
            description,
            quantity,
            rate,
            paymentType,
            paymentInfo,
            processor,
            payment_confirmed==Transaction.CONFIRMED?"Y":payment_confirmed==Transaction.NOT_CONFIRMED?"N":"W"
        );

        // Notify all clients of the updates
        invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, accounting, BusinessHandler.getServersForBusiness(conn, accounting), false);
        return transid;
    }

    /**
     * Gets the balance for one account.
     */
    public static void getAccountBalance(
        DatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        Account.Name accounting
    ) throws IOException, SQLException {
        MasterServer.writePenniesCheckBusiness(
            conn,
            source,
            "getAccountBalance",
            accounting,
            out,
            "select coalesce(sum(cast((rate*quantity) as numeric(9,2))), 0) from billing.\"Transaction\" where accounting=? and payment_confirmed!='N'",
            accounting.toString()
        );
    }

    /**
     * Gets the balance for one account.
     */
    public static void getAccountBalanceBefore(
        DatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out, 
        Account.Name accounting,
        long before
    ) throws IOException, SQLException {
        MasterServer.writePenniesCheckBusiness(
            conn,
            source,
            "getAccountBalanceBefore",
            accounting,
            out,
            "select coalesce(sum(cast(rate*quantity as numeric(9,2))), 0) from billing.\"Transaction\" where accounting=? and time<? and payment_confirmed!='N'",
            accounting.toString(),
            new Timestamp(before)
        );
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static void getConfirmedAccountBalance(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        Account.Name accounting
    ) throws IOException, SQLException {
        MasterServer.writePenniesCheckBusiness(
            conn,
            source,
            "getConfirmedAccountBalance",
            accounting,
            out,
            "select coalesce(sum(cast(rate*quantity as numeric(9,2))), 0) from billing.\"Transaction\" where accounting=? and payment_confirmed='Y'",
            accounting.toString()
        );
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static int getConfirmedAccountBalance(
        DatabaseConnection conn,
        Account.Name accounting
    ) throws IOException, SQLException {
        return SQLUtility.getPennies(
            conn.executeStringQuery(
                "select coalesce(sum(cast(rate*quantity as numeric(9,2))), 0) from billing.\"Transaction\" where accounting=? and payment_confirmed='Y'",
                accounting.toString()
            )
        );
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static void getConfirmedAccountBalanceBefore(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        Account.Name accounting,
        long before
    ) throws IOException, SQLException {
        MasterServer.writePenniesCheckBusiness(
            conn,
            source,
            "getConfirmedAccountBalanceBefore",
            accounting,
            out,
            "select coalesce(sum(cast(rate*quantity as numeric(9,2))), 0) from billing.\"Transaction\" where accounting=? and time<? and payment_confirmed='Y'",
            accounting.toString(),
            new Timestamp(before)
        );
    }

    /**
     * Gets all pending payments.
     */
    public static void getPendingPayments(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out, 
        boolean provideProgress
    ) throws IOException, SQLException {
        User mu = MasterServer.getUser(conn, source.getUsername());
        if(mu!=null && mu.canAccessAccounting()) {
            MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
				CursorMode.AUTO,
                new Transaction(),
                "select * from billing.\"Transaction\" where type=? and payment_confirmed='W'",
				TransactionType.PAYMENT
            );
        } else {
            MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
        }
    }

    /**
     * Gets all billing.Transaction for one business.
     */
    public static void getTransactionsBusiness(
        DatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        boolean provideProgress,
        Account.Name accounting
    ) throws IOException, SQLException {
        com.aoindustries.aoserv.client.account.User.Name username=source.getUsername();
        User masterUser=MasterServer.getUser(conn, username);
        UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, username);
        if(masterUser!=null) {
            if(masterServers.length==0) MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
				CursorMode.AUTO,
                new Transaction(),
                "select * from billing.\"Transaction\" where accounting=?",
                accounting
            ); else MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
        } else MasterServer.writeObjects(
            conn,
            source,
            out,
            provideProgress,
			CursorMode.AUTO,
            new Transaction(),
            "select\n"
            + "  tr.*\n"
            + "from\n"
            + "  account.\"User\" un1,\n"
            + "  billing.\"Package\" pk1,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "  billing.\"Transaction\" tr\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and un1.package=pk1.name\n"
            + "  and (\n"
            + TableHandler.PK1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=tr.accounting\n"
            + "  and tr.accounting=?",
            username,
            accounting
        );
    }

    /**
     * Gets all billing.Transaction for one business administrator.
     */
    public static void getTransactionsBusinessAdministrator(
        DatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        boolean provideProgress,
        com.aoindustries.aoserv.client.account.User.Name username
    ) throws IOException, SQLException {
        UsernameHandler.checkAccessUsername(conn, source, "getTransactionsBusinessAdministrator", source.getUsername());

        MasterServer.writeObjects(
            conn,
            source,
            out,
            provideProgress,
			CursorMode.FETCH,
            new Transaction(),
            "select * from billing.\"Transaction\" where username=?",
            username
        );
    }

    public static void getTransactionsSearch(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        TransactionSearchCriteria criteria
    ) throws IOException, SQLException {
        com.aoindustries.aoserv.client.account.User.Name username=source.getUsername();
        User masterUser=MasterServer.getUser(conn, username);
        UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, username);
        StringBuilder sql;
		final List<Object> params = new ArrayList<>();
        boolean whereDone;
        if(masterUser!=null) {
            if(masterServers.length==0) {
                sql=new StringBuilder(
                    "select\n"
                    + "  tr.*\n"
                    + "from\n"
                    + "  billing.\"Transaction\" tr\n"
                );
                whereDone=false;
            } else {
                MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
                return;
            }
        } else {
            sql=new StringBuilder(
                "select\n"
                + "  tr.*\n"
                + "from\n"
                + "  account.\"User\" un1,\n"
                + "  billing.\"Package\" pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  billing.\"Transaction\" tr\n"
                + "where\n"
                + "  un1.username=?\n"
                + "  and un1.package=pk1.name\n"
                + "  and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=tr.accounting\n"
            );
            params.add(source.getUsername());
            whereDone=true;
        }

        if (criteria.getAfter() != TransactionSearchCriteria.ANY) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.time>=?\n");
			params.add(new Timestamp(criteria.getAfter()));
            whereDone=true;
        }
        if (criteria.getBefore() != TransactionSearchCriteria.ANY) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.time<?\n");
			params.add(new Timestamp(criteria.getBefore()));
            whereDone=true;
        }
        if (criteria.getTransID() != TransactionSearchCriteria.ANY) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.transid=?\n");
			params.add(criteria.getTransID());
            whereDone=true;
        }
        if (criteria.getBusiness() != null) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.accounting=?\n");
			params.add(criteria.getBusiness());
            whereDone=true;
        }
        if (criteria.getSourceBusiness() != null) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.source_accounting=?\n");
			params.add(criteria.getSourceBusiness());
            whereDone=true;
        }
        if (criteria.getBusinessAdministrator() != null) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.username=?\n");
			params.add(criteria.getBusinessAdministrator());
            whereDone=true;
        }
        if (criteria.getType() != null) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.type=?\n");
			params.add(criteria.getType());
            whereDone=true;
        }

        // description words
        if (criteria.getDescription() != null && criteria.getDescription().length() > 0) {
            String[] descriptionWords = StringUtility.splitString(criteria.getDescription());
			for (String descriptionWord : descriptionWords) {
                sql.append(whereDone?"  and ":"where\n  ").append("lower(tr.description) like ('%' || lower(?) || '%')\n");
				params.add(descriptionWord);
                whereDone=true;
            }
        }

        if (criteria.getPaymentType() != null) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.payment_type=?\n");
			params.add(criteria.getPaymentType());
            whereDone=true;
        }

        // payment_info words
        if (criteria.getPaymentInfo() != null && criteria.getPaymentInfo().length() > 0) {
            String[] paymentInfoWords = StringUtility.splitString(criteria.getPaymentInfo());
			for (String paymentInfoWord : paymentInfoWords) {
                sql.append(whereDone?"  and ":"where\n  ").append("lower(tr.payment_info) like ('%' || lower(?) || '%')\n");
				params.add(paymentInfoWord);
                whereDone=true;
            }
        }

        // payment_confirmed
        if (criteria.getPaymentConfirmed() != TransactionSearchCriteria.ANY) {
            sql.append(whereDone?"  and ":"where\n  ").append("tr.payment_confirmed=?\n");
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
            whereDone=true;
        }

		Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
        PreparedStatement pstmt = dbConn.prepareStatement(
			sql.toString(),
			provideProgress ? ResultSet.TYPE_SCROLL_SENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
			ResultSet.CONCUR_READ_ONLY
		);
        try {
			DatabaseConnection.setParams(dbConn, pstmt, params.toArray());
            ResultSet results = pstmt.executeQuery();
            try {
				// TODO: Call other writeObjects, passing sql and parameters, to support cursor/fetch?
                MasterServer.writeObjects(source, out, provideProgress, new Transaction(), results);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt);
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void transactionApproved(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        BankAccountHandler.checkAccounting(conn, source, "transactionApproved");
        checkAccessTransaction(conn, source, "transactionApproved", transid);
        CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionApproved", creditCardTransaction);

        transactionApproved(conn, invalidateList, transid, creditCardTransaction, paymentInfo);
    }

    public static void transactionApproved(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        Account.Name accounting = getBusinessForTransaction(conn, transid);
		int updateCount;
		if(paymentInfo == null) {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_confirmed='Y' where transid=? and payment_confirmed='W'",
				creditCardTransaction,
				transid
			);
		} else {
			updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=?, payment_confirmed='Y' where transid=? and payment_confirmed='W'",
				creditCardTransaction,
				paymentInfo,
				transid
			);
		}
        if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W'");

        // Notify all clients of the update
        invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
    }

    public static void transactionDeclined(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        BankAccountHandler.checkAccounting(conn, source, "transactionDeclined");
        checkAccessTransaction(conn, source, "transactionDeclined", transid);
        CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionApproved", creditCardTransaction);

        transactionDeclined(conn, invalidateList, transid, creditCardTransaction, paymentInfo);
    }

    public static void transactionDeclined(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        Account.Name accounting = getBusinessForTransaction(conn, transid);

		int updateCount;
		if(paymentInfo == null) {
	        updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_confirmed='N' where transid=? and payment_confirmed='W'",
				creditCardTransaction,
				transid
			);
		} else {
	        updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=?, payment_confirmed='N' where transid=? and payment_confirmed='W'",
				creditCardTransaction,
				paymentInfo,
				transid
			);
		}
        if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W'");

        // Notify all clients of the update
        invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
    }

    public static void transactionHeld(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        BankAccountHandler.checkAccounting(conn, source, "transactionHeld");
        checkAccessTransaction(conn, source, "transactionHeld", transid);
        CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionHeld", creditCardTransaction);

        transactionHeld(conn, invalidateList, transid, creditCardTransaction, paymentInfo);
    }

    public static void transactionHeld(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction,
		String paymentInfo
    ) throws IOException, SQLException {
        Account.Name accounting = getBusinessForTransaction(conn, transid);

		int updateCount;
		if(paymentInfo == null) {
	        updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=? where transid=? and payment_confirmed='W' and credit_card_transaction is null",
				creditCardTransaction,
				transid
			);
		} else {
	        updateCount = conn.executeUpdate(
				"update billing.\"Transaction\" set credit_card_transaction=?, payment_info=? where transid=? and payment_confirmed='W' and credit_card_transaction is null",
				creditCardTransaction,
				paymentInfo,
				transid
			);
		}
        if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W' and credit_card_transaction is null");

        // Notify all clients of the update
        invalidateList.addTable(conn, Table.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
    }

    public static Account.Name getBusinessForTransaction(DatabaseConnection conn, int transid) throws IOException, SQLException {
        return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
            "select accounting from billing.\"Transaction\" where transid=?",
            transid
        );
    }
}