package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>TransactionHandler</code> handles all the accesses to the transaction tables.
 *
 * @author  AO Industries, Inc.
 */
final public class TransactionHandler {

    public static boolean canAccessTransaction(MasterDatabaseConnection conn, RequestSource source, int transid) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "canAccessTransaction(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            return BusinessHandler.canAccessBusiness(conn, source, getBusinessForTransaction(conn, transid));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessTransaction(MasterDatabaseConnection conn, RequestSource source, String action, int transid) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "checkAccessTransaction(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            BusinessHandler.checkAccessBusiness(conn, source, action, getBusinessForTransaction(conn, transid));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a transaction.
     */
    public static int addTransaction(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String sourceAccounting,
        String business_administrator,
        String type,
        String description,
        int quantity,
        int rate,
        String paymentType,
        String paymentInfo,
        String processor,
        byte payment_confirmed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "addTransaction(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,String,String,int,int,String,String,String,byte)", null);

        try {
            BankAccountHandler.checkAccounting(conn, source, "addTransaction");
            BusinessHandler.checkAccessBusiness(conn, source, "addTransaction", accounting);
            BusinessHandler.checkAccessBusiness(conn, source, "addTransaction", sourceAccounting);
            UsernameHandler.checkAccessUsername(conn, source, "addTransaction", business_administrator);
            if(business_administrator.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add Transaction for user '"+LinuxAccount.MAIL+'\'');

            int transid=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('transactions_transid_seq')");

            conn.executeUpdate(
                "insert into transactions values(now(),?,?,?,?,?,?,?::decimal(8,3),?::decimal(9,2),?,?,?,null,?)",
                transid,
                accounting,
                sourceAccounting,
                business_administrator,
                type,
                description,
                SQLUtility.getMilliDecimal(quantity),
                SQLUtility.getDecimal(rate),
                paymentType,
                paymentInfo,
                processor,
                payment_confirmed==Transaction.CONFIRMED?"Y":payment_confirmed==Transaction.NOT_CONFIRMED?"N":"W"
            );

            // Notify all clients of the updates
            invalidateList.addTable(conn, SchemaTable.TableID.TRANSACTIONS, accounting, BusinessHandler.getServersForBusiness(conn, accounting), false);
            return transid;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the balance for one account.
     */
    public static void getAccountBalance(
        MasterDatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getAccountBalance(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,String)", null);
        try {
            MasterServer.writePenniesCheckBusiness(
                conn,
                source,
                "getAccountBalance",
                accounting,
                out,
                "select coalesce(sum(cast((rate*quantity) as decimal(9,2))), 0) from transactions where accounting=? and payment_confirmed!='N'",
                accounting
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the balance for one account.
     */
    public static void getAccountBalanceBefore(
        MasterDatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out, 
        String accounting, 
        long before
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getAccountBalanceBefore(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,String,long)", null);
        try {
            MasterServer.writePenniesCheckBusiness(
                conn,
                source,
                "getAccountBalanceBefore",
                accounting,
                out,
                "select coalesce(sum(cast(rate*quantity as decimal(9,2))), 0) from transactions where accounting=? and time<? and payment_confirmed!='N'",
                accounting,
                new Timestamp(before)
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static void getConfirmedAccountBalance(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getConfirmedAccountBalance(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,String)", null);
        try {
            MasterServer.writePenniesCheckBusiness(
                conn,
                source,
                "getConfirmedAccountBalance",
                accounting,
                out,
                "select coalesce(sum(cast(rate*quantity as decimal(9,2))), 0) from transactions where accounting=? and payment_confirmed='Y'",
                accounting
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static int getConfirmedAccountBalance(
        MasterDatabaseConnection conn,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getConfirmedAccountBalance(MasterDatabaseConnection,String)", null);
        try {
            return SQLUtility.getPennies(
                conn.executeStringQuery(
                    "select coalesce(sum(cast(rate*quantity as decimal(9,2))), 0) from transactions where accounting=? and payment_confirmed='Y'",
                    accounting
                )
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the confirmed balance for one account.
     */
    public static void getConfirmedAccountBalanceBefore(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String accounting, 
        long before
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getConfirmedAccountBalanceBefore(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,String,long)", null);
        try {
            MasterServer.writePenniesCheckBusiness(
                conn,
                source,
                "getConfirmedAccountBalanceBefore",
                accounting,
                out,
                "select coalesce(sum(cast(rate*quantity as decimal(9,2))), 0) from transactions where accounting=? and time<? and payment_confirmed='Y'",
                accounting,
                new Timestamp(before)
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all pending payments.
     */
    public static void getPendingPayments(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out, 
        boolean provideProgress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getPendingPayments(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null && mu.canAccessAccounting()) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Transaction(),
                    "select * from transactions where type='"+TransactionType.PAYMENT+"' and payment_confirmed='W'"
                );
            } else {
                MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());                
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all transactions for one business.
     */
    public static void getTransactionsBusiness(
        MasterDatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        boolean provideProgress,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getTransactionsBusiness(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            if(masterUser!=null) {
                if(masterServers.length==0) MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Transaction(),
                    "select * from transactions where accounting=?",
                    accounting
                ); else MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());
            } else MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
                new Transaction(),
                "select\n"
                + "  tr.*\n"
                + "from\n"
                + "  usernames un1,\n"
                + "  packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  transactions tr\n"
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all transactions for one business administrator.
     */
    public static void getTransactionsBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        boolean provideProgress,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getTransactionsBusinessAdministrator(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "getTransactionsBusinessAdministrator", source.getUsername());

            MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
                new Transaction(),
                "select * from transactions where username=?",
                username
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getTransactionsSearch(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        TransactionSearchCriteria criteria
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getTransactionsSearch(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,TransactionSearchCriteria)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            StringBuilder sql;
            boolean whereDone;
            boolean useUsername;
            if(masterUser!=null) {
                if(masterServers.length==0) {
                    sql=new StringBuilder(
                        "select\n"
                        + "  tr.*\n"
                        + "from\n"
                        + "  transactions tr\n"
                    );
                    whereDone=false;
                    useUsername=false;
                } else {
                    MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());
                    return;
                }
            } else {
                sql=new StringBuilder(
                    "select\n"
                    + "  tr.*\n"
                    + "from\n"
                    + "  usernames un1,\n"
                    + "  packages pk1,\n"
                    + TableHandler.BU1_PARENTS_JOIN
                    + "  transactions tr\n"
                    + "where\n"
                    + "  un1.username=?\n"
                    + "  and un1.package=pk1.name\n"
                    + "  and (\n"
                    + TableHandler.PK1_BU1_PARENTS_WHERE
                    + "  )\n"
                    + "  and bu1.accounting=tr.accounting\n"
                );
                whereDone=true;
                useUsername=true;
            }

            if (criteria.getAfter() != TransactionSearchCriteria.ANY) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.time>=?\n");
                whereDone=true;
            }
            if (criteria.getBefore() != TransactionSearchCriteria.ANY) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.time<?\n");
                whereDone=true;
            }
            if (criteria.getTransID() != TransactionSearchCriteria.ANY) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.transid=?\n");
                whereDone=true;
            }
            if (criteria.getBusiness() != null) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.accounting=?\n");
                whereDone=true;
            }
            if (criteria.getSourceBusiness() != null) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.source_accounting=?\n");
                whereDone=true;
            }
            if (criteria.getBusinessAdministrator() != null) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.username=?\n");
                whereDone=true;
            }
            if (criteria.getType() != null) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.type=?\n");
                whereDone=true;
            }

            // description words
            String[] descriptionWords=null;
            if (criteria.getDescription() != null && criteria.getDescription().length() > 0) {
                descriptionWords = StringUtility.splitString(criteria.getDescription());
                int len = descriptionWords.length;
                for (int c = 0; c < len; c++) {
                    sql.append(whereDone?"  and ":"where\n  ").append("lower(tr.description) like '%");
                    SQLUtility.escapeSQL(descriptionWords[c].toLowerCase(), sql);
                    sql.append("%'\n");
                    whereDone=true;
                }
            }

            if (criteria.getPaymentType() != null) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.payment_type=?\n");
                whereDone=true;
            }

            // payment_info words
            String[] paymentInfoWords=null;
            if (criteria.getPaymentInfo() != null && criteria.getPaymentInfo().length() > 0) {
                paymentInfoWords = StringUtility.splitString(criteria.getPaymentInfo());
                int len = paymentInfoWords.length;
                for (int c = 0; c < len; c++) {
                    sql.append(whereDone?"  and ":"where\n  ").append("lower(tr.payment_info) like '%");
                    SQLUtility.escapeSQL(paymentInfoWords[c].toLowerCase(), sql);
                    sql.append("%'\n");
                    whereDone=true;
                }
            }

            // payment_confirmed
            if (criteria.getPaymentConfirmed() != TransactionSearchCriteria.ANY) {
                sql.append(whereDone?"  and ":"where\n  ").append("tr.payment_confirmed=?\n");
                whereDone=true;
            }

            // Convert to string before allocating the DB connection for maximum DB concurrency
            String sqlString=sql.toString();

            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sqlString);
            try {
                int pos=1;
                if(useUsername) pstmt.setString(pos++, source.getUsername());
                if (criteria.getAfter() != TransactionSearchCriteria.ANY) pstmt.setTimestamp(pos++, new Timestamp(criteria.getAfter()));
                if (criteria.getBefore() != TransactionSearchCriteria.ANY) pstmt.setTimestamp(pos++, new Timestamp(criteria.getBefore()));
                if (criteria.getTransID() != TransactionSearchCriteria.ANY) pstmt.setInt(pos++, criteria.getTransID());
                if (criteria.getBusiness() != null) pstmt.setString(pos++, criteria.getBusiness());
                if (criteria.getSourceBusiness() != null) pstmt.setString(pos++, criteria.getSourceBusiness());
                if (criteria.getBusinessAdministrator() != null) pstmt.setString(pos++, criteria.getBusinessAdministrator());
                if (criteria.getType() != null) pstmt.setString(pos++, criteria.getType());
                if (criteria.getPaymentType() != null) pstmt.setString(pos++, criteria.getPaymentType());
                if (criteria.getPaymentConfirmed() != TransactionSearchCriteria.ANY) pstmt.setString(
                    pos++,
                    criteria.getPaymentConfirmed()==Transaction.CONFIRMED?"Y"
                    :criteria.getPaymentConfirmed()==Transaction.NOT_CONFIRMED?"N"
                    :"W"
                );

                conn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void transactionApproved(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "transactionApproved(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            BankAccountHandler.checkAccounting(conn, source, "transactionApproved");
            checkAccessTransaction(conn, source, "transactionApproved", transid);
            CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionApproved", creditCardTransaction);

            String accounting=getBusinessForTransaction(conn, transid);
            int updateCount = conn.executeUpdate(
                "update transactions set credit_card_transaction=?, payment_confirmed='Y' where transid=? and payment_confirmed='W'",
                creditCardTransaction,
                transid
            );
            if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W'");

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void transactionDeclined(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "transactionDeclined(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            BankAccountHandler.checkAccounting(conn, source, "transactionDeclined");
            checkAccessTransaction(conn, source, "transactionDeclined", transid);
            CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionApproved", creditCardTransaction);

            String accounting=getBusinessForTransaction(conn, transid);
            
            int updateCount = conn.executeUpdate("update transactions set credit_card_transaction=?, payment_confirmed='N' where transid=? and payment_confirmed='W'", creditCardTransaction, transid);
            if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W'");

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void transactionHeld(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int transid,
        int creditCardTransaction
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "transactionHeld(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            BankAccountHandler.checkAccounting(conn, source, "transactionHeld");
            checkAccessTransaction(conn, source, "transactionHeld", transid);
            CreditCardHandler.checkAccessCreditCardTransaction(conn, source, "transactionHeld", creditCardTransaction);

            String accounting=getBusinessForTransaction(conn, transid);
            
            int updateCount = conn.executeUpdate("update transactions set credit_card_transaction=? where transid=? and payment_confirmed='W' and credit_card_transaction is null", creditCardTransaction, transid);
            if(updateCount==0) throw new SQLException("Unable to find transaction with transid="+transid+" and payment_confirmed='W' and credit_card_transaction is null");

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.TRANSACTIONS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForTransaction(MasterDatabaseConnection conn, int transid) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TransactionHandler.class, "getBusinessForTransaction(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select accounting from transactions where transid=?", transid);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}