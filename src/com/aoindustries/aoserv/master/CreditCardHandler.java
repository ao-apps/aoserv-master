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
            BusinessHandler.checkPermission(conn, source, action, AOServPermission.Permission.get_credit_cards);
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
        String cardInfo,
        String processorName,
        String providerUniqueId,
        String firstName,
        String lastName,
        String companyName,
        String email,
        String phone,
        String fax,
        String customerTaxId,
        String streetAddress1,
        String streetAddress2,
        String city,
        String state,
        String postalCode,
        String countryCode,
        String description
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "addCreditCard", AOServPermission.Permission.add_credit_card);
        BusinessHandler.checkAccessBusiness(conn, source, "addCreditCard", accounting);

        int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('credit_cards_pkey_seq')");

        conn.executeUpdate(
            "insert into credit_cards values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,false,true,null,null,?)",
            pkey,
            accounting,
            cardInfo,
            processorName,
            providerUniqueId,
            firstName,
            lastName,
            companyName,
            email,
            phone,
            fax,
            customerTaxId,
            streetAddress1,
            streetAddress2,
            city,
            state,
            postalCode,
            countryCode,
            source.getUsername(),
            description
        );

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.CREDIT_CARDS, accounting, InvalidateList.allServers, false);
        return pkey;
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
                SchemaTable.TableID.CREDIT_CARDS,
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
            BusinessHandler.checkPermission(conn, source, "removeCreditCard", AOServPermission.Permission.delete_credit_card);
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
                SchemaTable.TableID.CREDIT_CARDS,
                business,
                BusinessHandler.getServersForBusiness(conn, business),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void updateCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String firstName,
        String lastName,
        String companyName,
        String email,
        String phone,
        String fax,
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
        BusinessHandler.checkPermission(conn, source, "updateCreditCard", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "updateCreditCard", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  first_name=?,\n"
            + "  last_name=?,\n"
            + "  company_name=?,\n"
            + "  email=?,\n"
            + "  phone=?,\n"
            + "  fax=?,\n"
            + "  customer_tax_id=?,\n"
            + "  street_address1=?,\n"
            + "  street_address2=?,\n"
            + "  city=?,\n"
            + "  state=?,\n"
            + "  postal_code=?,\n"
            + "  country_code=?,\n"
            + "  description=?\n"
            + "where\n"
            + "  pkey=?",
            firstName,
            lastName,
            companyName,
            email,
            phone,
            fax,
            customerTaxId,
            streetAddress1,
            streetAddress2,
            city,
            state,
            postalCode,
            countryCode,
            description,
            pkey
        );
        
        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void updateCreditCardCardInfo(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String cardInfo
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "updateCreditCardCardInfo", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "updateCreditCardCardInfo", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  card_info=?\n"
            + "where\n"
            + "  pkey=?",
            cardInfo,
            pkey
        );
        
        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void reactivateCreditCard(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "reactivateCreditCard", AOServPermission.Permission.edit_credit_card);
        checkAccessCreditCard(conn, source, "reactivateCreditCard", pkey);
        
        // Update row
        conn.executeUpdate(
            "update\n"
            + "  credit_cards\n"
            + "set\n"
            + "  active=true,\n"
            + "  deactivated_on=null,\n"
            + "  deactivate_reason=null\n"
            + "where\n"
            + "  pkey=?",
            pkey
        );

        String accounting = getBusinessForCreditCard(conn, pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void setCreditCardUseMonthly(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        int pkey
    ) throws IOException, SQLException {
        // Permission checks
        BusinessHandler.checkPermission(conn, source, "setCreditCardUseMonthly", AOServPermission.Permission.edit_credit_card);

        if(pkey==-1) {
            // Clear only
            conn.executeUpdate("update credit_cards set use_monthly=false where accounting=? and use_monthly", accounting);
        } else {
            checkAccessCreditCard(conn, source, "setCreditCardUseMonthly", pkey);

            // Make sure accounting codes match
            if(!accounting.equals(getBusinessForCreditCard(conn, pkey))) throw new SQLException("credit card and business accounting codes do not match");

            // Perform clear and set in one SQL statement - I thinks myself clever right now.
            conn.executeUpdate("update credit_cards set use_monthly=(pkey=?) where accounting=? and use_monthly!=(pkey=?)", pkey, accounting, pkey);
        }

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CREDIT_CARDS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }
}
