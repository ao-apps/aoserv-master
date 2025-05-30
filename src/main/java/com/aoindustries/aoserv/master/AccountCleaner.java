/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2003-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2025  AO Industries, Inc.
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

import com.aoapps.collections.IntList;
import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.ProcessTimer;
import com.aoapps.lang.SysExits;
import com.aoapps.lang.i18n.Money;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoapps.lang.validation.ValidationException;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.backup.BackupReport;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Automatically cleans out old account resources.  The resources are left in tact for one month before cleaning.
 *
 * @author  AO Industries, Inc.
 */
public final class AccountCleaner implements CronJob {

  private static final Logger logger = Logger.getLogger(AccountCleaner.class.getName());

  /**
   * The maximum time for a cleaning.
   */
  private static final long TIMER_MAX_TIME = 20L * 60 * 1000;

  /**
   * The interval in which the administrators will be reminded.
   */
  private static final long TIMER_REMINDER_INTERVAL = 6L * 60 * 60 * 1000;

  /**
   * The number of days to keep canceled account resources.
   */
  private static final int CANCELED_KEEP_DAYS = 30;

  private static boolean started;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void start() {
    synchronized (System.out) {
      if (!started) {
        System.out.print("Starting " + AccountCleaner.class.getSimpleName() + ": ");
        CronDaemon.addCronJob(new AccountCleaner(), logger);
        started = true;
        System.out.println("Done");
      }
    }
  }

  private AccountCleaner() {
    // Do nothing
  }

  /**
   * Runs at 5:25 am daily.
   */
  private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute == 25 && hour == 5;

  @Override
  public Schedule getSchedule() {
    return schedule;
  }

  @Override
  public int getThreadPriority() {
    return Thread.NORM_PRIORITY - 1;
  }

  @Override
  @SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
  public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
    try {
      try (
          ProcessTimer timer = new ProcessTimer(
              logger,
              AccountCleaner.class.getName(),
              "runCronJob",
              "Account Cleaner",
              "Cleaning old account resources",
              TIMER_MAX_TIME,
              TIMER_REMINDER_INTERVAL
          )
          ) {
        AoservMaster.executorService.submit(timer);

        // Start the transaction
        final InvalidateList invalidateList = new InvalidateList();
        cleanNow(invalidateList);
        AoservMaster.invalidateTables(MasterDatabase.getDatabase(), invalidateList, null);
      }
    } catch (ThreadDeath td) {
      throw td;
    } catch (Throwable t) {
      logger.log(Level.SEVERE, null, t);
    }
  }

  private static void cleanNow(InvalidateList invalidateList) throws IOException, SQLException, ValidationException {
    try (final DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
      StringBuilder message = new StringBuilder();

      // backup.BackupReport
      {
        // Those that are part of canceled accounts
        if (
            conn.queryBoolean(
                "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      br.id\n"
                    + "    from\n"
                    + "      backup.\"BackupReport\" br,\n"
                    + "      billing.\"Package\" pk,\n"
                    + "      account.\"Account\" bu\n"
                    + "    where\n"
                    + "      br.package=pk.id\n"
                    + "      and pk.accounting=bu.accounting\n"
                    + "      and bu.canceled is not null\n"
                    + "      and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS + "\n"
                    + "    limit 1\n"
                    + "  ) is not null"
            )
        ) {
          conn.update(
              "delete from\n"
                  + "  backup.\"BackupReport\"\n"
                  + "where\n"
                  + "  id in (\n"
                  + "    select\n"
                  + "      br.id\n"
                  + "    from\n"
                  + "      backup.\"BackupReport\" br,\n"
                  + "      billing.\"Package\" pk,\n"
                  + "      account.\"Account\" bu\n"
                  + "    where\n"
                  + "      br.package=pk.id\n"
                  + "      and pk.accounting=bu.accounting\n"
                  + "      and bu.canceled is not null\n"
                  + "      and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS + "\n"
                  + "  )"
          );
          invalidateList.addTable(conn, Table.TableId.BACKUP_REPORTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
        }

        // Those that are older than BackupReport.SendmailSmtpStat.MAX_REPORT_AGE
        if (
            conn.queryBoolean(
                "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      id\n"
                    + "    from\n"
                    + "      backup.\"BackupReport\"\n"
                    + "    where\n"
                    + "      (CURRENT_DATE - date)>" + BackupReport.MAX_REPORT_AGE + "\n" // Convert to interval?
                    + "    limit 1\n"
                    + "  ) is not null"
            )
        ) {
          conn.update(
              "delete from\n"
                  + "  backup.\"BackupReport\"\n"
                  + "where\n"
                  + "  (CURRENT_DATE - date)>" + BackupReport.MAX_REPORT_AGE // Convert to interval?
          );
          invalidateList.addTable(conn, Table.TableId.BACKUP_REPORTS, InvalidateList.allAccounts, InvalidateList.allHosts, false);
        }
      }

      // account.Account
      {
        {
          // look for any accounts that have been canceled but not disabled
          List<Account.Name> bus = conn.queryList(
              ObjectFactories.accountNameFactory,
              "select accounting from account.\"Account\" where parent=? and canceled is not null and disable_log is null",
              AccountHandler.getRootAccount()
          );
          if (!bus.isEmpty()) {
            message
                .append("The following account.Account ")
                .append(bus.size() == 1 ? "has" : "have")
                .append(" been canceled but not disabled:\n");
            for (Account.Name bu : bus) {
              message.append(bu).append('\n');
            }
            message.append('\n');
          }
        }

        {
          // look for any accounts that have been disabled for over two months but not canceled
          List<Account.Name> bus = conn.queryList(
              ObjectFactories.accountNameFactory,
              "select\n"
                  + "  bu.accounting\n"
                  + "from\n"
                  + "  account.\"Account\" bu,\n"
                  + "  account.\"DisableLog\" dl\n"
                  + "where\n"
                  + "  bu.canceled is null\n"
                  + "  and bu.disable_log=dl.id\n"
                  + "  and (CURRENT_DATE - dl.time::date)>60"
          );
          if (!bus.isEmpty()) {
            message
                .append("The following account.Account ")
                .append(bus.size() == 1 ? "has" : "have")
                .append(" been disabled for over 60 days but not canceled:\n");
            for (Account.Name bu : bus) {
              message.append(bu).append('\n');
            }
            message.append('\n');
          }
        }
      }

      // payment.CreditCard
      {
        IntList ccs = conn.queryIntList(
            "select\n"
                + "  cc.id\n"
                + "from\n"
                + "  payment.\"CreditCard\" cc,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  cc.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < ccs.size(); c++) {
          PaymentHandler.removeCreditCard(conn, invalidateList, ccs.getInt(c));
        }
      }

      // account.Administrator over CANCELED_KEEP_DAYS days
      // remove if balance is zero and has not been used in ticket.Action or billing.Transaction
      {
        List<com.aoindustries.aoserv.client.account.User.Name> administrators = conn.queryList(
            ObjectFactories.userNameFactory,
            "select\n"
                + "  ba.username\n"
                + "from\n"
                + "  account.\"Administrator\"      ba\n"
                + "  inner join account.\"User\"    un on ba.username   = un.username\n"
                + "  inner join billing.\"Package\" pk on un.package    = pk.name\n"
                + "  inner join account.\"Account\" bu on pk.accounting = bu.accounting\n"
                + "where\n"
                + "  bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS + "\n"
                // payment.Payment
                // PostgresSQL 8.3 doing sequential scan on "or":
                // + "  and (select cct.id from payment.\"Payment\" cct where cct.credit_card_created_by=ba.username or cct.authorization_username=ba.username "
                // + "or cct.capture_username=ba.username or cct.void_username=ba.username limit 1) is null\n"
                + "  and (select cct1.id from payment.\"Payment\" cct1 where cct1.credit_card_created_by = ba.username limit 1) is null\n"
                + "  and (select cct2.id from payment.\"Payment\" cct2 where cct2.authorization_username = ba.username limit 1) is null\n"
                + "  and (select cct3.id from payment.\"Payment\" cct3 where cct3.capture_username       = ba.username limit 1) is null\n"
                + "  and (select cct4.id from payment.\"Payment\" cct4 where cct4.void_username          = ba.username limit 1) is null\n"
                // payment.CreditCard
                + "  and (select cc.id from payment.\"CreditCard\" cc where cc.created_by=ba.username limit 1) is null\n"
                // account.DisableLog
                + "  and (select dl.id from account.\"DisableLog\" dl where dl.disabled_by=ba.username limit 1) is null\n"
                // billing.MonthlyCharge
                + "  and (select mc.id from billing.\"MonthlyCharge\" mc where mc.created_by=ba.username limit 1) is null\n"
                // billing.Package
                + "  and (select pk2.id from billing.\"Package\" pk2 where pk2.created_by=ba.username limit 1) is null\n"
                // signup.Request
                + "  and (select sr.id from signup.\"Request\" sr where sr.completed_by=ba.username limit 1) is null\n"
                // ticket.Action
                // PostgresSQL 8.3 doing sequential scan on "or":
                // + "  and (select ta.id from ticket.Action ta where ta.administrator=ba.username or ta.old_assigned_to=ba.username or ta.new_assigned_to=ba.username limit 1) is null\n"
                + "  and (select ta1.id from ticket.\"Action\" ta1 where ta1.administrator   = ba.username limit 1) is null\n"
                + "  and (select ta2.id from ticket.\"Action\" ta2 where ta2.old_assigned_to = ba.username limit 1) is null\n"
                + "  and (select ta3.id from ticket.\"Action\" ta3 where ta3.new_assigned_to = ba.username limit 1) is null\n"
                // ticket.Assignment
                + "  and (select ta4.id from ticket.\"Assignment\" ta4 where ta4.administrator=ba.username limit 1) is null\n"
                // ticket.Ticket
                + "  and (select ti.id from ticket.\"Ticket\" ti where ti.created_by=ba.username limit 1) is null\n"
                // billing.Transaction
                + "  and (select tr.transid from billing.\"Transaction\" tr where tr.username=ba.username limit 1) is null"
        );
        for (com.aoindustries.aoserv.client.account.User.Name administrator : administrators) {
          Account.Name account = AccountUserHandler.getAccountForUser(conn, administrator);
          boolean hasBalance = false;
          for (Money balance : BillingTransactionHandler.getConfirmedAccountBalance(conn, account)) {
            if (balance.getUnscaledValue() > 0) {
              hasBalance = true;
            }
          }
          if (!hasBalance) {
            AccountHandler.removeAdministrator(conn, invalidateList, administrator);
          }
        }
      }

      // scm.CvsRepository
      {
        IntList crs = conn.queryIntList(
            "select\n"
                + "  cr.id\n"
                + "from\n"
                + "  scm.\"CvsRepository\" cr,\n"
                + "  linux.\"UserServer\" lsa,\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  cr.linux_server_account=lsa.id\n"
                + "  and lsa.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < crs.size(); c++) {
          CvsHandler.removeCvsRepository(conn, invalidateList, crs.getInt(c));
        }
      }

      // dns.Zone
      {
        DnsService dnsService = AoservMaster.getService(DnsService.class);
        List<String> dzs = conn.queryStringList(
            "select\n"
                + "  dz.zone\n"
                + "from\n"
                + "  dns.\"Zone\" dz,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  dz.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (String dz : dzs) {
          dnsService.removeDnsZone(conn, invalidateList, dz);
        }
      }

      // email.List
      {
        IntList els = conn.queryIntList(
            "select\n"
                + "  el.id\n"
                + "from\n"
                + "  email.\"List\" el,\n"
                + "  linux.\"GroupServer\" lsg,\n"
                + "  linux.\"Group\" lg,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  el.linux_server_group=lsg.id\n"
                + "  and lsg.name=lg.name\n"
                + "  and lg.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < els.size(); c++) {
          EmailHandler.removeList(conn, invalidateList, els.getInt(c));
        }
      }

      // email.Domain
      {
        IntList eds = conn.queryIntList(
            "select\n"
                + "  ed.id\n"
                + "from\n"
                + "  email.\"Domain\" ed,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  ed.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < eds.size(); c++) {
          EmailHandler.removeDomain(conn, invalidateList, eds.getInt(c));
        }
      }

      // email.Pipe
      {
        IntList eps = conn.queryIntList(
            "select\n"
                + "  ep.id\n"
                + "from\n"
                + "  email.\"Pipe\" ep,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  ep.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < eps.size(); c++) {
          EmailHandler.removePipe(conn, invalidateList, eps.getInt(c));
        }
      }

      // email.SmtpRelay
      {
        IntList esrs = conn.queryIntList(
            "select\n"
                + "  esr.id\n"
                + "from\n"
                + "  email.\"SmtpRelay\" esr,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  esr.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < esrs.size(); c++) {
          EmailHandler.removeSmtpRelay(conn, invalidateList, esrs.getInt(c));
        }
      }

      /*
      // backup.FileReplicationSetting
      {
        IntList fbss=conn.queryIntList(
          "select\n"
          + "  fbs.id\n"
          + "from\n"
          + "  account.\"Account\" bu,\n"
          + "  billing.\"Package\" pk,\n"
          + "  backup."FileReplicationSetting" fbs\n"
          + "where\n"
          + "  bu.canceled is not null\n"
          + "  and (CURRENT_DATE - bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
          + "  and bu.accounting=pk.accounting\n"
          + "  and pk.id=fbs.package"
        );
        for (int c=0;c<fbss.size();c++) {
          BackupHandler.removeFileBackupSetting(conn, invalidateList, fbss.getInt(c));
        }
      }*/
      // TODO: Should also remove backup servers

      // web.Site
      {
        IntList hss = conn.queryIntList(
            "select\n"
                + "  hs.id\n"
                + "from\n"
                + "  web.\"Site\" hs,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  hs.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < hss.size(); c++) {
          WebHandler.removeSite(conn, invalidateList, hss.getInt(c));
        }
      }

      // web.tomcat.SharedTomcat
      {
        IntList hsts = conn.queryIntList(
            "select\n"
                + "  hst.id\n"
                + "from\n"
                + "  \"web.tomcat\".\"SharedTomcat\" hst,\n"
                + "  linux.\"GroupServer\" lsg,\n"
                + "  linux.\"Group\" lg,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  hst.linux_server_group=lsg.id\n"
                + "  and lsg.name=lg.name\n"
                + "  and lg.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < hsts.size(); c++) {
          WebHandler.removeSharedTomcat(conn, invalidateList, hsts.getInt(c));
        }
      }

      // ftp.PrivateServer
      {
        IntList pfss = conn.queryIntList(
            "select\n"
                + "  pfs.net_bind\n"
                + "from\n"
                + "  ftp.\"PrivateServer\" pfs,\n"
                + "  net.\"Bind\" nb,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  pfs.net_bind=nb.id\n"
                + "  and nb.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < pfss.size(); c++) {
          FtpHandler.removePrivateServer(conn, invalidateList, pfss.getInt(c));
        }
      }

      // net.Bind
      {
        IntList nbs = conn.queryIntList(
            "select\n"
                + "  nb.id\n"
                + "from\n"
                + "  net.\"Bind\" nb,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  nb.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < nbs.size(); c++) {
          NetBindHandler.removeBind(conn, invalidateList, nbs.getInt(c));
        }

      }

      // net.IpAddress
      {
        IntList ias = conn.queryIntList(
            "select\n"
                + "  ia.id\n"
                + "from\n"
                + "  net.\"IpAddress\" ia,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  ia.package=pk.id\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < ias.size(); c++) {
          int ia = ias.getInt(c);
          IpAddressHandler.setIpAddressPackage(conn, invalidateList, ia, AccountHandler.getRootAccount());
          IpAddressHandler.releaseIpAddress(conn, invalidateList, ia);
        }
      }

      // web.HttpdServer
      {
        IntList hss = conn.queryIntList(
            "select\n"
                + "  hs.id\n"
                + "from\n"
                + "  web.\"HttpdServer\" hs,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  hs.package=pk.id\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < hss.size(); c++) {
          int hs = hss.getInt(c);
          WebHandler.removeHttpdServer(conn, invalidateList, hs);
        }
      }

      // linux.User
      {
        List<com.aoindustries.aoserv.client.linux.User.Name> las = conn.queryList(
            ObjectFactories.linuxUserNameFactory,
            "select\n"
                + "  la.username\n"
                + "from\n"
                + "  linux.\"User\" la,\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  la.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (com.aoindustries.aoserv.client.linux.User.Name la : las) {
          try {
            LinuxAccountHandler.removeUser(conn, invalidateList, la);
          } catch (SQLException err) {
            System.err.println("SQLException trying to remove User: " + la);
            throw err;
          }
        }
      }

      // linux.Group
      {
        List<Group.Name> lgs = conn.queryList(
            ObjectFactories.groupNameFactory,
            "select\n"
                + "  lg.name\n"
                + "from\n"
                + "  linux.\"Group\" lg,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  lg.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (Group.Name lg : lgs) {
          try {
            LinuxAccountHandler.removeGroup(conn, invalidateList, lg);
          } catch (SQLException err) {
            System.err.println("SQLException trying to remove Group: " + lg);
            throw err;
          }
        }
      }

      // mysql.Database
      {
        IntList mds = conn.queryIntList(
            "select\n"
                + "  md.id\n"
                + "from\n"
                + "  mysql.\"Database\" md,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  md.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < mds.size(); c++) {
          MysqlHandler.removeDatabase(conn, invalidateList, mds.getInt(c));
        }
      }

      // mysql.User
      {
        List<com.aoindustries.aoserv.client.mysql.User.Name> mus = conn.queryList(
            ObjectFactories.mysqlUserNameFactory,
            "select\n"
                + "  mu.username\n"
                + "from\n"
                + "  mysql.\"User\" mu,\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  mu.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (com.aoindustries.aoserv.client.mysql.User.Name mu : mus) {
          MysqlHandler.removeUser(conn, invalidateList, mu);
        }
      }

      // postgresql.Database
      {
        IntList pds = conn.queryIntList(
            "select\n"
                + "  pd.id\n"
                + "from\n"
                + "  postgresql.\"Database\" pd,\n"
                + "  postgresql.\"UserServer\" psu,\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  pd.datdba=psu.id\n"
                + "  and psu.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (int c = 0; c < pds.size(); c++) {
          PostgresqlHandler.removeDatabase(conn, invalidateList, pds.getInt(c));
        }
      }

      // postgresql.User
      {
        List<com.aoindustries.aoserv.client.postgresql.User.Name> pus = conn.queryList(
            ObjectFactories.postgresqlUserNameFactory,
            "select\n"
                + "  pu.username\n"
                + "from\n"
                + "  postgresql.\"User\" pu,\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  pu.username=un.username\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
        );
        for (com.aoindustries.aoserv.client.postgresql.User.Name pu : pus) {
          PostgresqlHandler.removeUser(conn, invalidateList, pu);
        }
      }

      // account.User
      // delete all closed account.User, unless used by a business_administrator that was left behind
      {
        List<com.aoindustries.aoserv.client.account.User.Name> uns = conn.queryList(
            ObjectFactories.userNameFactory,
            "select\n"
                + "  un.username\n"
                + "from\n"
                + "  account.\"User\" un,\n"
                + "  billing.\"Package\" pk,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  un.package=pk.name\n"
                + "  and pk.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS + "\n"
                + "  and (select ba.username from account.\"Administrator\" ba where ba.username=un.username) is null"
        );
        for (com.aoindustries.aoserv.client.account.User.Name un : uns) {
          AccountUserHandler.removeUser(conn, invalidateList, un);
        }
      }

      // account.DisableLog
      {
        IntList dls = conn.queryIntList(
            "select\n"
                + "  dl.id\n"
                + "from\n"
                + "  account.\"DisableLog\" dl,\n"
                + "  account.\"Account\" bu\n"
                + "where\n"
                + "  dl.accounting=bu.accounting\n"
                + "  and bu.canceled is not null\n"
                + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS + "\n"
                + "  and (select ba.username from account.\"Administrator\" ba where ba.disable_log=dl.id limit 1) is null\n"
                + "  and (select bu2.accounting from account.\"Account\" bu2 where bu2.disable_log=dl.id limit 1) is null\n"
                + "  and (select cr.id from scm.\"CvsRepository\" cr where cr.disable_log=dl.id limit 1) is null\n"
                + "  and (select el.id from email.\"List\" el where el.disable_log=dl.id limit 1) is null\n"
                + "  and (select ep.id from email.\"Pipe\" ep where ep.disable_log=dl.id limit 1) is null\n"
                + "  and (select esr.id from email.\"SmtpRelay\" esr where esr.disable_log=dl.id limit 1) is null\n"
                + "  and (select hst.id from \"web.tomcat\".\"SharedTomcat\" hst where hst.disable_log=dl.id limit 1) is null\n"
                + "  and (select hsb.id from web.\"VirtualHost\" hsb where hsb.disable_log=dl.id limit 1) is null\n"
                + "  and (select hs.id from web.\"Site\" hs where hs.disable_log=dl.id limit 1) is null\n"
                + "  and (select la.username from linux.\"User\" la where la.disable_log=dl.id limit 1) is null\n"
                + "  and (select lsa.id from linux.\"UserServer\" lsa where lsa.disable_log=dl.id limit 1) is null\n"
                + "  and (select msu.id from mysql.\"UserServer\" msu where msu.disable_log=dl.id limit 1) is null\n"
                + "  and (select mu.username from mysql.\"User\" mu where mu.disable_log=dl.id limit 1) is null\n"
                + "  and (select pk.name from billing.\"Package\" pk where pk.disable_log=dl.id limit 1) is null\n"
                + "  and (select psu.id from postgresql.\"UserServer\" psu where psu.disable_log=dl.id limit 1) is null\n"
                + "  and (select pu.username from postgresql.\"User\" pu where pu.disable_log=dl.id limit 1) is null\n"
                + "  and (select un.username from account.\"User\" un where un.disable_log=dl.id limit 1) is null"
        );
        for (int c = 0; c < dls.size(); c++) {
          AccountHandler.removeDisableLog(conn, invalidateList, dls.getInt(c));
        }
      }

      // account.AccountHost
      // delete all account.AccountHost for canceled account.Account
      {
        for (int depth = Account.MAXIMUM_BUSINESS_TREE_DEPTH; depth >= 1; depth--) {
          // non-default
          IntList bss = conn.queryIntList(
              "select\n"
                  + "  bs.id\n"
                  + "from\n"
                  + "  account.\"AccountHost\" bs,\n"
                  + "  account.\"Account\" bu\n"
                  + "where\n"
                  + "  not bs.is_default\n"
                  + "  and bs.accounting=bu.accounting\n"
                  + "  and bu.canceled is not null\n"
                  + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
          );
          for (int c = 0; c < bss.size(); c++) {
            int bs = bss.getInt(c);
            Account.Name account = conn.queryObject(ObjectFactories.accountNameFactory, "select accounting from account.\"AccountHost\" where id=?", bs);
            int bsDepth = AccountHandler.getDepthInAccountTree(conn, account);
            if (bsDepth == depth) {
              AccountHandler.removeAccountHost(conn, invalidateList, bs);
            }
          }

          // default
          bss = conn.queryIntList(
              "select\n"
                  + "  bs.id\n"
                  + "from\n"
                  + "  account.\"AccountHost\" bs,\n"
                  + "  account.\"Account\" bu\n"
                  + "where\n"
                  + "  bs.accounting=bu.accounting\n"
                  + "  and bu.canceled is not null\n"
                  + "  and (CURRENT_DATE - bu.canceled::date)>" + CANCELED_KEEP_DAYS
          );
          for (int c = 0; c < bss.size(); c++) {
            int bs = bss.getInt(c);
            Account.Name account = conn.queryObject(ObjectFactories.accountNameFactory, "select accounting from account.\"AccountHost\" where id=?", bs);
            int bsDepth = AccountHandler.getDepthInAccountTree(conn, account);
            if (bsDepth == depth) {
              AccountHandler.removeAccountHost(conn, invalidateList, bs);
            }
          }
        }
      }
      if (message.length() > 0) {
        logger.log(Level.WARNING, message.toString());
      }
      conn.commit();
    }
  }

  @SuppressWarnings({"UseOfSystemOutOrSystemErr", "UseSpecificCatch", "TooBroadCatch"})
  public static void main(String[] args) {
    try {
      InvalidateList invalidateList = new InvalidateList();
      cleanNow(invalidateList);
      for (Table.TableId tableId : Table.TableId.values()) {
        List<Integer> affectedHosts = invalidateList.getAffectedHosts(tableId);
        if (affectedHosts != null) {
          if (affectedHosts == InvalidateList.allHosts) {
            System.out.println("invalidate " + tableId.name().toLowerCase(Locale.ENGLISH));
          } else {
            for (int affectedHost : affectedHosts) {
              System.out.println("invalidate " + tableId.name().toLowerCase(Locale.ENGLISH) + " " + affectedHost);
            }
          }
        }
      }
    } catch (ThreadDeath td) {
      throw td;
    } catch (Throwable t) {
      ErrorPrinter.printStackTraces(t, System.err);
      System.exit(SysExits.getSysExit(t));
    }
  }
}
