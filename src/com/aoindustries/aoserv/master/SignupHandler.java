/*
 * Copyright 2007-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.InetAddress;
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
     * Creates a new <code>signup.Request</code>.
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
        UserId ba_username,
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

        // Add the entry
        int requestId = conn.executeIntUpdate(
			"INSERT INTO signup.\"Request\" VALUES (default,?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,null,null) RETURNING id",
            accounting.toString(),
            ip_address.toString(),
            package_definition,
            business_name,
            business_phone,
            business_fax,
            business_address1,
            business_address2,
            business_city,
            business_state,
            business_country,
            business_zip,
            ba_name,
            ba_title,
            ba_work_phone,
            ba_cell_phone,
            ba_home_phone,
            ba_fax,
            ba_email,
            ba_address1,
            ba_address2,
            ba_city,
            ba_state,
            ba_country,
            ba_zip,
            ba_username.toString(),
            billing_contact,
            billing_email,
            billing_use_monthly,
            billing_pay_one_year,
            ciphertext,
            from,
            recipient
		);

        // Add the signup_options
		PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into signup.\"Option\" values(default,?,?,?)");
        try {
            for(String name : options.keySet()) {
                String value = options.get(name);
                pstmt.setInt(1, requestId);
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
        invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUESTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
        invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUEST_OPTIONS, InvalidateList.allBusinesses, InvalidateList.allServers, false);

        return requestId;
    }

    private static boolean cronDaemonAdded = false;

    private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==32 && hour==6;

    public static void start() {
        synchronized(System.out) {
            if(!cronDaemonAdded) {
                System.out.print("Starting " + SignupHandler.class.getSimpleName() + ": ");
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
                                if(database.executeUpdate("delete from signup.\"Request\" where completed_time is not null and (now()::date-completed_time::date)>31")>0) {
                                    invalidateList.addTable(
                                        database,
                                        Table.TableID.SIGNUP_REQUESTS,
                                        InvalidateList.allBusinesses,
                                        InvalidateList.allServers,
                                        false
                                    );
                                    invalidateList.addTable(
                                        database,
                                        Table.TableID.SIGNUP_REQUEST_OPTIONS,
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
