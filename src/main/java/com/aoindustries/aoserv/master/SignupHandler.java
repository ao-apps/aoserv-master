/*
 * Copyright 2007-2013, 2015, 2017, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.net.InetAddress;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>SignupHandler</code> handles all the accesses to the signup tables.
 *
 * @author  AO Industries, Inc.
 */
final public class SignupHandler {

	private static final Logger logger = Logger.getLogger(SignupHandler.class.getName());

	private SignupHandler() {
	}

	/**
	 * Creates a new <code>signup.Request</code>.
	 */
	public static int addRequest(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name account,
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
		User.Name administrator_user_name,
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
		AccountHandler.checkAccessAccount(conn, source, "addRequest", account);
		PackageHandler.checkAccessPackageDefinition(conn, source, "addRequest", package_definition);
		PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", from);
		PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", recipient);

		// Add the entry
		int requestId = conn.executeIntUpdate(
			"INSERT INTO signup.\"Request\" VALUES (default,?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,null,null) RETURNING id",
			account.toString(),
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
			administrator_user_name.toString(),
			billing_contact,
			billing_email,
			billing_use_monthly,
			billing_pay_one_year,
			ciphertext,
			from,
			recipient
		);

		// Add the signup_options
		try (
			PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into signup.\"Option\" values(default,?,?,?)")
		) {
			for(String name : options.keySet()) {
				String value = options.get(name);
				pstmt.setInt(1, requestId);
				pstmt.setString(2, name);
				pstmt.setString(3, value);

				pstmt.executeUpdate();
			}
		}

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUESTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
		invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUEST_OPTIONS, InvalidateList.allAccounts, InvalidateList.allHosts, false);

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
						public Schedule getSchedule() {
							return schedule;
						}

						@Override
						public String getName() {
							return "Remove completed signups";
						}

						@Override
						public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
							try {
								InvalidateList invalidateList = new InvalidateList();
								MasterDatabase database = MasterDatabase.getDatabase();
								if(database.executeUpdate("delete from signup.\"Request\" where completed_time is not null and (now()::date-completed_time::date)>31")>0) {
									invalidateList.addTable(database,
										Table.TableID.SIGNUP_REQUESTS,
										InvalidateList.allAccounts,
										InvalidateList.allHosts,
										false
									);
									invalidateList.addTable(database,
										Table.TableID.SIGNUP_REQUEST_OPTIONS,
										InvalidateList.allAccounts,
										InvalidateList.allHosts,
										false
									);
									MasterServer.invalidateTables(invalidateList, null);
								}
							} catch(ThreadDeath TD) {
								throw TD;
							} catch(Throwable T) {
								logger.log(Level.SEVERE, null, T);
							}
						}

						@Override
						public int getThreadPriority() {
							return Thread.NORM_PRIORITY-2;
						}
					},
					logger
				);
				cronDaemonAdded = true;
				System.out.println("Done");
			}
		}
	}
}
