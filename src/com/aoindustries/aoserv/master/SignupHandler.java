/*
 * Copyright 2007-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;

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
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        AccountingCode accounting,
        InetAddress ip_address,
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
        int from,
        int recipient,
        String ciphertext,
        // options
        Map<String,String> options
    ) throws IOException, SQLException {
        // Security checks
        BusinessHandler.checkAccessBusiness(conn, source, "addSignupRequest", accounting);
        PackageHandler.checkAccessPackageDefinition(conn, source, "addSignupRequest", package_definition);
        CreditCardHandler.checkAccessEncryptionKey(conn, source, "addSignupRequest", from);
        CreditCardHandler.checkAccessEncryptionKey(conn, source, "addSignupRequest", recipient);

        // Make all database changes in one big transaction
        int pkey=conn.executeIntQuery("select nextval('signup_requests_pkey_seq')");

        // Add the entry
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into signup_requests values(?,?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,null,null)");
        try {
            pstmt.setInt(1, pkey);
            pstmt.setString(2, accounting.toString());
            pstmt.setString(3, ip_address.toString());
            pstmt.setInt(4, package_definition);
            pstmt.setString(5, business_name);
            pstmt.setString(6, business_phone);
            pstmt.setString(7, business_fax);
            pstmt.setString(8, business_address1);
            pstmt.setString(9, business_address2);
            pstmt.setString(10, business_city);
            pstmt.setString(11, business_state);
            pstmt.setString(12, business_country);
            pstmt.setString(13, business_zip);
            pstmt.setString(14, ba_name);
            pstmt.setString(15, ba_title);
            pstmt.setString(16, ba_work_phone);
            pstmt.setString(17, ba_cell_phone);
            pstmt.setString(18, ba_home_phone);
            pstmt.setString(19, ba_fax);
            pstmt.setString(20, ba_email);
            pstmt.setString(21, ba_address1);
            pstmt.setString(22, ba_address2);
            pstmt.setString(23, ba_city);
            pstmt.setString(24, ba_state);
            pstmt.setString(25, ba_country);
            pstmt.setString(26, ba_zip);
            pstmt.setString(27, ba_username);
            pstmt.setString(28, billing_contact);
            pstmt.setString(29, billing_email);
            pstmt.setBoolean(30, billing_use_monthly);
            pstmt.setBoolean(31, billing_pay_one_year);
            pstmt.setString(32, ciphertext);
            pstmt.setInt(33, from);
            pstmt.setInt(34, recipient);

            pstmt.executeUpdate();
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }

        // Add the signup_options
        pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into signup_request_options values(DEFAULT,?,?,?)");
        try {
            for(String name : options.keySet()) {
                String value = options.get(name);
                pstmt.setInt(1, pkey);
                pstmt.setString(2, name);
                pstmt.setString(3, value);

                pstmt.executeUpdate();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.SIGNUP_REQUESTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.TableID.SIGNUP_REQUEST_OPTIONS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

        return pkey;
    }

    private static boolean cronDaemonAdded = false;

    private static final Schedule schedule = new Schedule() {
		@Override
        public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
            return minute==32 && hour==6;
        }
    };

    public static void start() {
        synchronized(System.out) {
            if(!cronDaemonAdded) {
                System.out.print("Starting SignupHandler: ");
                CronDaemon.addCronJob(
                    new CronJob() {
						@Override
                        public Schedule getCronJobSchedule() {
                            return schedule;
                        }

						@Override
                        public CronJobScheduleMode getCronJobScheduleMode() {
                            return CronJobScheduleMode.SKIP;
                        }

						@Override
                        public String getCronJobName() {
                            return "Remove completed signups";
                        }

						@Override
                        public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
                            try {
                                InvalidateList invalidateList = new InvalidateList();
                                MasterDatabase database = MasterDatabase.getDatabase();
                                if(database.executeUpdate("delete from signup_requests where completed_time is not null and (now()::date-completed_time::date)>31")>0) {
                                    invalidateList.addTable(
                                        database,
                                        SchemaTable.TableID.SIGNUP_REQUESTS,
                                        InvalidateList.allBusinesses,
                                        InvalidateList.allServers,
                                        false
                                    );
                                    invalidateList.addTable(
                                        database,
                                        SchemaTable.TableID.SIGNUP_REQUEST_OPTIONS,
                                        InvalidateList.allBusinesses,
                                        InvalidateList.allServers,
                                        false
                                    );
                                    MasterServer.invalidateTables(invalidateList, null);
                                }
                            } catch(ThreadDeath TD) {
                                throw TD;
                            } catch(Throwable T) {
                                LogFactory.getLogger(getClass()).log(Level.SEVERE, null, T);
                            }
                        }

						@Override
                        public int getCronJobThreadPriority() {
                            return Thread.NORM_PRIORITY-2;
                        }
                    }, LogFactory.getLogger(SignupHandler.class)
                );
                cronDaemonAdded = true;
                System.out.println("Done");
            }
        }
    }
}
