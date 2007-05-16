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
        String accounting,
        byte[] cardNumber,
        String cardInfo,
        byte[] expirationMonth,
        byte[] expirationYear,
        byte[] cardholderName,
        byte[] streetAddress,
        byte[] city,
        byte[] state,
        byte[] zip,
        boolean useMonthly,
        String description
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, CreditCardHandler.class, "addCreditCard(MasterDatabaseConnection,RequestSource,InvalidateList,String,byte[],String,byte[],byte[],byte[],byte[],byte[],byte[],byte[],boolean,String)", null);
        try {
            BusinessHandler.checkAccessBusiness(conn, source, "createBusinessProfile", accounting);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('credit_cards_pkey_seq')");

            int nextPriority=conn.executeIntQuery("select (coalesce(max(priority), 0))+1 from credit_cards where accounting=?", accounting);
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into credit_cards values(?,?,?,?,?,?,?,?,?,?,?,now(),?,?,true,null,null,?,?)");
            try {
                pstmt.setInt(1, pkey);
                pstmt.setString(2, accounting);
                pstmt.setString(3, new String(cardNumber));
                pstmt.setString(4, cardInfo);
                pstmt.setString(5, new String(expirationMonth));
                pstmt.setString(6, new String(expirationYear));
                pstmt.setString(7, new String(cardholderName));
                pstmt.setString(8, new String(streetAddress));
                pstmt.setString(9, new String(city));
                pstmt.setString(10, state==null?null:new String(state));
                pstmt.setString(11, zip==null?null:new String(zip));
                pstmt.setString(12, source.getUsername());
                pstmt.setBoolean(13, useMonthly);
                pstmt.setInt(14, nextPriority);
                pstmt.setString(15, description);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.CREDIT_CARDS, accounting, InvalidateList.allServers, false);
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
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
                SchemaTable.CREDIT_CARDS,
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
                SchemaTable.CREDIT_CARDS,
                business,
                BusinessHandler.getServersForBusiness(conn, business),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}