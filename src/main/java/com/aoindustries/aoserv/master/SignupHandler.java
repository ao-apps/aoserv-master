/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
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
public final class SignupHandler {

  /** Make no instances. */
  private SignupHandler() {
    throw new AssertionError();
  }

  private static final Logger logger = Logger.getLogger(SignupHandler.class.getName());

  /**
   * Creates a new <code>signup.Request</code>.
   */
  public static int addRequest(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name account,
      InetAddress ipAddress,
      int packageDefinition,
      String businessName,
      String businessPhone,
      String businessFax,
      String businessAddress1,
      String businessAddress2,
      String businessCity,
      String businessState,
      String businessCountry,
      String businessZip,
      String baName,
      String baTitle,
      String baWorkPhone,
      String baCellPhone,
      String baHomePhone,
      String baFax,
      String baEmail,
      String baAddress1,
      String baAddress2,
      String baCity,
      String baState,
      String baCountry,
      String baZip,
      User.Name administrator_user_name,
      String billingContact,
      String billingEmail,
      boolean billingUseMonthly,
      boolean billingPayOneYear,
      // Encrypted values
      int from,
      int recipient,
      String ciphertext,
      // options
      Map<String, String> options
  ) throws IOException, SQLException {
    // Security checks
    AccountHandler.checkAccessAccount(conn, source, "addRequest", account);
    PackageHandler.checkAccessPackageDefinition(conn, source, "addRequest", packageDefinition);
    PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", from);
    PaymentHandler.checkAccessEncryptionKey(conn, source, "addRequest", recipient);

    // Add the entry
    int requestId = conn.updateInt(
        "INSERT INTO signup.\"Request\" VALUES (default,?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,null,null) RETURNING id",
        account.toString(),
        ipAddress.toString(),
        packageDefinition,
        businessName,
        businessPhone,
        businessFax,
        businessAddress1,
        businessAddress2,
        businessCity,
        businessState,
        businessCountry,
        businessZip,
        baName,
        baTitle,
        baWorkPhone,
        baCellPhone,
        baHomePhone,
        baFax,
        baEmail,
        baAddress1,
        baAddress2,
        baCity,
        baState,
        baCountry,
        baZip,
        administrator_user_name.toString(),
        billingContact,
        billingEmail,
        billingUseMonthly,
        billingPayOneYear,
        ciphertext,
        from,
        recipient
    );

    // Add the signup_options
    try (PreparedStatement pstmt = conn.getConnection().prepareStatement("insert into signup.\"Option\" values(default,?,?,?)")) {
      try {
        for (String name : options.keySet()) {
          String value = options.get(name);
          pstmt.setInt(1, requestId);
          pstmt.setString(2, name);
          pstmt.setString(3, value);

          pstmt.executeUpdate();
        }
      } catch (Error | RuntimeException | SQLException e) {
        ErrorPrinter.addSql(e, pstmt);
        throw e;
      }
    }

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.SIGNUP_REQUESTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
    invalidateList.addTable(conn, Table.TableId.SIGNUP_REQUEST_OPTIONS, InvalidateList.allAccounts, InvalidateList.allHosts, false);

    return requestId;
  }

  private static boolean cronDaemonAdded;

  private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute == 32 && hour == 6;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void start() {
    synchronized (System.out) {
      if (!cronDaemonAdded) {
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
                    if (conn.update("delete from signup.\"Request\" where completed_time is not null and (now()::date-completed_time::date)>31") > 0) {
                      invalidateList.addTable(
                          conn,
                          Table.TableId.SIGNUP_REQUESTS,
                          InvalidateList.allAccounts,
                          InvalidateList.allHosts,
                          false
                      );
                      invalidateList.addTable(
                          conn,
                          Table.TableId.SIGNUP_REQUEST_OPTIONS,
                          InvalidateList.allAccounts,
                          InvalidateList.allHosts,
                          false
                      );
                      AoservMaster.invalidateTables(conn, invalidateList, null);
                    }
                    conn.commit();
                  }
                } catch (ThreadDeath td) {
                  throw td;
                } catch (Throwable t) {
                  logger.log(Level.SEVERE, null, t);
                }
              }

              @Override
              public int getThreadPriority() {
                return Thread.NORM_PRIORITY - 2;
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
