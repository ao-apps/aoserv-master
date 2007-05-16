package com.aoindustries.aoserv.master;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.SchemaTable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * The <code>SignupHandler</code> handles all the accesses to the signup tables.
 *
 * @author  AO Industries, Inc.
 */
final public class SignupHandler {

    private SignupHandler() {
    }

    /**
     * Creates a new <code>SignupRequest</code>.
     */
    public static int addSignupRequest(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String ip_address,
        int package_definition,
        String business_name,
        String business_phone,
        String business_fax,
        String business_address1,
        String business_address2,
        String business_city,
        String business_state,
        String business_country,
        String business_zip,
        String ba_name,
        String ba_title,
        String ba_work_phone,
        String ba_cell_phone,
        String ba_home_phone,
        String ba_fax,
        String ba_email,
        String ba_address1,
        String ba_address2,
        String ba_city,
        String ba_state,
        String ba_country,
        String ba_zip,
        String ba_username,
        String billing_contact,
        String billing_email,
        boolean billing_use_monthly,
        boolean billing_pay_one_year,
        // Encrypted values
        int recipient,
        String ciphertext,
        // options
        Map<String,String> options
    ) throws IOException, SQLException {
        // Security checks
        BusinessHandler.checkAccessBusiness(conn, source, "addSignupRequest", accounting);
        PackageHandler.checkAccessPackageDefinition(conn, source, "addSignupRequest", package_definition);
        CreditCardHandler.checkAccessEncryptionKey(conn, source, "addSignupRequest", recipient);

        // Make all database changes in one big transaction
        int pkey=conn.executeIntQuery("select nextval('signup_requests_pkey_seq')");

        // Add the entry
        /* TODO
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into dns_records values(?,?,?,?,?,?,null,?)");
        try {
            pstmt.setInt(1, pkey);
            pstmt.setString(2, zone);
            pstmt.setString(3, domain);
            pstmt.setString(4, type);

            conn.incrementUpdateCount();
            pstmt.executeUpdate();
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
         * TODO: Add options
         */
        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.SIGNUP_REQUESTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.SIGNUP_REQUEST_OPTIONS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

        return pkey;
    }
}
