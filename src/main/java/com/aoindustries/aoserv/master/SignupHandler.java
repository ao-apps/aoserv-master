/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoapps.net.InetAddress;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.User;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
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
		Map<String, String> options
	) throws IOException, SQLException {
		// Security checks
		AccountHandler.checkAccessAccount(conn, source, "addRequest", account);
		PackageHandler.checkAccessPackageDefinition(conn, source, "addRequest", package_definition);
		PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", from);
		PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", recipient);

		// Add the entry
		int requestId = conn.updateInt(
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
		try (PreparedStatement pstmt = conn.getConnection().prepareStatement("insert into signup.\"Option\" values(default,?,?,?)")) {
			try {
				for(String name : options.keySet()) {
					String value = options.get(name);
					pstmt.setInt(1, requestId);
					pstmt.setString(2, name);
					pstmt.setString(3, value);

					pstmt.executeUpdate();
				}
			} catch(Error | RuntimeException | SQLException e) {
				ErrorPrinter.addSQL(e, pstmt);
				throw e;
			}
		}

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUESTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
		invalidateList.addTable(conn, Table.TableID.SIGNUP_REQUEST_OPTIONS, InvalidateList.allAccounts, InvalidateList.allHosts, false);

		return requestId;
	}

	private static boolean cronDaemonAdded = false;

	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==32 && hour==6;

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
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
						@SuppressWarnings("UseSpecificCatch")
						public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
							try {
								InvalidateList invalidateList = new InvalidateList();
								try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
									if(conn.update("delete from signup.\"Request\" where completed_time is not null and (now()::date-completed_time::date)>31") > 0) {
										invalidateList.addTable(
											conn,
											Table.TableID.SIGNUP_REQUESTS,
											InvalidateList.allAccounts,
											InvalidateList.allHosts,
											false
										);
										invalidateList.addTable(
											conn,
											Table.TableID.SIGNUP_REQUEST_OPTIONS,
											InvalidateList.allAccounts,
											InvalidateList.allHosts,
											false
										);
										MasterServer.invalidateTables(conn, invalidateList, null);
									}
									conn.commit();
								}
							} catch(ThreadDeath td) {
								throw td;
							} catch(Throwable t) {
								logger.log(Level.SEVERE, null, t);
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
