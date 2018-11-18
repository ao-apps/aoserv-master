/*
 * Copyright 2003-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.BackupReport;
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.PostgresUserId;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.IntList;
import com.aoindustries.util.logging.ProcessTimer;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Automatically cleans out old account resources.  The resources are left in tact for one month before cleaning.
 *
 * @author  AO Industries, Inc.
 */
final public class AccountCleaner implements CronJob {

    private static final Logger logger = LogFactory.getLogger(AccountCleaner.class);

    /**
     * The maximum time for a cleaning.
     */
    private static final long TIMER_MAX_TIME=20L*60*1000;

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=6L*60*60*1000;

    /**
     * The number of days to keep canceled account resources.
     */
    private static final int CANCELED_KEEP_DAYS = 30;

    private static boolean started=false;

    public static void start() {
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting AccountCleaner: ");
                CronDaemon.addCronJob(new AccountCleaner(), logger);
                started=true;
                System.out.println("Done");
            }
        }
    }
    
    private AccountCleaner() {
    }

	/**
	 * Runs at 5:25 am daily.
	 */
    private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==25 && hour==5;

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
        return "AccountCleaner";
    }

	@Override
    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY-1;
    }

	@Override
    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        try {
            ProcessTimer timer=new ProcessTimer(
                logger,
                MasterServer.getRandom(),
                AccountCleaner.class.getName(),
                "runCronJob",
                "Account Cleaner",
                "Cleaning old account resources",
                TIMER_MAX_TIME,
                TIMER_REMINDER_INTERVAL
            );
            try {
                MasterServer.executorService.submit(timer);

                // Start the transaction
                final InvalidateList invalidateList=new InvalidateList();
                cleanNow(invalidateList);
                MasterServer.invalidateTables(invalidateList, null);
            } finally {
                timer.finished();
            }
        } catch(ThreadDeath TD) {
            throw TD;
        } catch(Throwable T) {
            logger.log(Level.SEVERE, null, T);
        }
    }
    
    private static void cleanNow(InvalidateList invalidateList) throws IOException, SQLException, ValidationException {
        final DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
        try {
            boolean connRolledBack=false;
            try {
                StringBuilder message=new StringBuilder();

                Timestamp now=new Timestamp(System.currentTimeMillis());

                    // backup_reports
                    {
                        // Those that are part of canceled accounts
                        if(
                            conn.executeBooleanQuery(
                                "select\n"
                                + "  (\n"
                                + "    select\n"
                                + "      br.pkey\n"
                                + "    from\n"
                                + "      backup_reports br,\n"
                                + "      packages pk,\n"
                                + "      businesses bu\n"
                                + "    where\n"
                                + "      br.package=pk.pkey\n"
                                + "      and pk.accounting=bu.accounting\n"
                                + "      and bu.canceled is not null\n"
                                + "      and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
                                + "    limit 1\n"
                                + "  ) is not null",
                                now
                            )
                        ) {
                            conn.executeUpdate(
                                "delete from\n"
                                + "  backup_reports\n"
                                + "where\n"
                                + "  pkey in (\n"
                                + "    select\n"
                                + "      br.pkey\n"
                                + "    from\n"
                                + "      backup_reports br,\n"
                                + "      packages pk,\n"
                                + "      businesses bu\n"
                                + "    where\n"
                                + "      br.package=pk.pkey\n"
                                + "      and pk.accounting=bu.accounting\n"
                                + "      and bu.canceled is not null\n"
                                + "      and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
                                + "  )",
                                now
                            );
                            invalidateList.addTable(conn, SchemaTable.TableID.BACKUP_REPORTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                        }

                        // Those that are older than BackupReport.SendmailSmtpStat.MAX_REPORT_AGE
                        if(
                            conn.executeBooleanQuery(
                                "select\n"
                                + "  (\n"
                                + "    select\n"
                                + "      pkey\n"
                                + "    from\n"
                                + "      backup_reports\n"
                                + "    where\n"
                                + "      (?::date-date)>"+BackupReport.MAX_REPORT_AGE+"\n" // Convert to interval?
                                + "    limit 1\n"
                                + "  ) is not null",
                                now
                            )
                        ) {
                            conn.executeUpdate(
                                "delete from\n"
                                + "  backup_reports\n"
                                + "where\n"
                                + "  (?::date-date)>"+BackupReport.MAX_REPORT_AGE, // Convert to interval?
                                now
                            );
                            invalidateList.addTable(conn, SchemaTable.TableID.BACKUP_REPORTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                        }
                    }

                // businesses
                {
                    {
                        // look for any accounts that have been canceled but not disabled
                        List<AccountingCode> bus = conn.executeObjectListQuery(
							ObjectFactories.accountingCodeFactory,
                            "select accounting from businesses where parent=? and canceled is not null and disable_log is null",
                            BusinessHandler.getRootBusiness()
                        );
                        if(!bus.isEmpty()) {
                            message
                                .append("The following ")
                                .append(bus.size()==1?"business has":"businesses have")
                                .append(" been canceled but not disabled:\n");
							for (AccountingCode bu : bus) {
								message.append(bu).append('\n');
							}
                            message.append('\n');
                        }
                    }

                    {
                        // look for any accounts that have been disabled for over two months but not canceled
                        List<AccountingCode> bus = conn.executeObjectListQuery(
                            ObjectFactories.accountingCodeFactory,
                            "select\n"
                            + "  bu.accounting\n"
                            + "from\n"
                            + "  businesses bu,\n"
                            + "  disable_log dl\n"
                            + "where\n"
                            + "  bu.canceled is null\n"
                            + "  and bu.disable_log=dl.pkey\n"
                            + "  and (?::date-dl.time::date)>60",
                            now
                        );
                        if(bus.size()>0) {
                            message
                                .append("The following ")
                                .append(bus.size()==1?"business has":"businesses have")
                                .append(" been disabled for over 60 days but not canceled:\n");
							for (AccountingCode bu : bus) {
								message.append(bu).append('\n');
							}
                            message.append('\n');
                        }
                    }
                }

                // credit_cards
                {
                    IntList ccs=conn.executeIntListQuery(
                        "select\n"
                        + "  cc.pkey\n"
                        + "from\n"
                        + "  credit_cards cc,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  cc.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<ccs.size();c++) {
                        CreditCardHandler.removeCreditCard(conn, invalidateList, ccs.getInt(c));
                    }
                }

                // business_administrators over CANCELED_KEEP_DAYS days
                // remove if balance is zero and has not been used in ticket_actions or transactions
                {
                    List<UserId> bas=conn.executeObjectListQuery(
						ObjectFactories.userIdFactory,
                        "select\n"
                        + "  ba.username\n"
                        + "from\n"
                        + "  business_administrators ba\n"
                        + "  inner join usernames    un on ba.username   = un.username\n"
                        + "  inner join packages     pk on un.package    = pk.name\n"
                        + "  inner join businesses   bu on pk.accounting = bu.accounting\n"
                        + "where\n"
                        + "  bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS + "\n"
						// credit_card_transactions
						// PostgresSQL 8.3 doing sequential scan on "or":
                        // + "  and (select cct.pkey from credit_card_transactions cct where cct.credit_card_created_by=ba.username or cct.authorization_username=ba.username or cct.capture_username=ba.username or cct.void_username=ba.username limit 1) is null\n"
						+ "  and (select cct1.pkey from credit_card_transactions cct1 where cct1.credit_card_created_by = ba.username limit 1) is null\n"
						+ "  and (select cct2.pkey from credit_card_transactions cct2 where cct2.authorization_username = ba.username limit 1) is null\n"
						+ "  and (select cct3.pkey from credit_card_transactions cct3 where cct3.capture_username       = ba.username limit 1) is null\n"
						+ "  and (select cct4.pkey from credit_card_transactions cct4 where cct4.void_username          = ba.username limit 1) is null\n"
						// credit_cards
                        + "  and (select cc.pkey from credit_cards cc where cc.created_by=ba.username limit 1) is null\n"
						// disable_log
                        + "  and (select dl.pkey from disable_log dl where dl.disabled_by=ba.username limit 1) is null\n"
						// monthly_charges
                        + "  and (select mc.pkey from monthly_charges mc where mc.created_by=ba.username limit 1) is null\n"
						// packages
                        + "  and (select pk2.pkey from packages pk2 where pk2.created_by=ba.username limit 1) is null\n"
						// signup_requests
                        + "  and (select sr.pkey from signup_requests sr where sr.completed_by=ba.username limit 1) is null\n"
						// ticket_actions
						// PostgresSQL 8.3 doing sequential scan on "or":
						// + "  and (select ta.pkey from ticket_actions ta where ta.administrator=ba.username or ta.old_assigned_to=ba.username or ta.new_assigned_to=ba.username limit 1) is null\n"
						+ "  and (select ta1.pkey from ticket_actions ta1 where ta1.administrator   = ba.username limit 1) is null\n"
						+ "  and (select ta2.pkey from ticket_actions ta2 where ta2.old_assigned_to = ba.username limit 1) is null\n"
						+ "  and (select ta3.pkey from ticket_actions ta3 where ta3.new_assigned_to = ba.username limit 1) is null\n"
						// ticket_assignments
                        + "  and (select ta4.pkey from ticket_assignments ta4 where ta4.administrator=ba.username limit 1) is null\n"
						// tickets
                        + "  and (select ti.pkey from tickets ti where ti.created_by=ba.username limit 1) is null\n"
						// transactions
                        + "  and (select tr.transid from transactions tr where tr.username=ba.username limit 1) is null",
                        now
                    );
					for (UserId username : bas) {
						AccountingCode business=UsernameHandler.getBusinessForUsername(conn, username);
						int balance=TransactionHandler.getConfirmedAccountBalance(conn, business);
						if(balance<=0) {
							BusinessHandler.removeBusinessAdministrator(conn, invalidateList, username);
						}
					}
                }

                // cvs_repositories
                {
                    IntList crs=conn.executeIntListQuery(
                        "select\n"
                        + "  cr.pkey\n"
                        + "from\n"
                        + "  cvs_repositories cr,\n"
                        + "  linux_server_accounts lsa,\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  cr.linux_server_account=lsa.pkey\n"
                        + "  and lsa.username=un.username\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<crs.size();c++) {
                        CvsHandler.removeCvsRepository(conn, invalidateList, crs.getInt(c));
                    }
                }

                // dns_zones
                {
                    List<String> dzs=conn.executeStringListQuery(
                        "select\n"
                        + "  dz.zone\n"
                        + "from\n"
                        + "  dns_zones dz,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  dz.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
					for (String dz : dzs) {
						DNSHandler.removeDNSZone(conn, invalidateList, dz);
					}
                }

                // email_lists
                {
                    IntList els=conn.executeIntListQuery(
                        "select\n"
                        + "  el.pkey\n"
                        + "from\n"
                        + "  email_lists el,\n"
                        + "  linux_server_groups lsg,\n"
                        + "  linux_groups lg,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  el.linux_server_group=lsg.pkey\n"
                        + "  and lsg.name=lg.name\n"
                        + "  and lg.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<els.size();c++) {
                        EmailHandler.removeEmailList(conn, invalidateList, els.getInt(c));
                    }
                }

                // email_domains
                {
                    IntList eds=conn.executeIntListQuery(
                        "select\n"
                        + "  ed.pkey\n"
                        + "from\n"
                        + "  email_domains ed,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  ed.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<eds.size();c++) {
                        EmailHandler.removeEmailDomain(conn, invalidateList, eds.getInt(c));
                    }
                }

                // email_pipes
                {
                    IntList eps=conn.executeIntListQuery(
                        "select\n"
                        + "  ep.pkey\n"
                        + "from\n"
                        + "  email_pipes ep,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  ep.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<eps.size();c++) {
                        EmailHandler.removeEmailPipe(conn, invalidateList, eps.getInt(c));
                    }
                }

                // email_smtp_relays
                {
                    IntList esrs=conn.executeIntListQuery(
                        "select\n"
                        + "  esr.pkey\n"
                        + "from\n"
                        + "  email_smtp_relays esr,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  esr.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<esrs.size();c++) {
                        EmailHandler.removeEmailSmtpRelay(conn, invalidateList, esrs.getInt(c));
                    }
                }

                /*
                // file_backup_settings
                {
                    IntList fbss=conn.executeIntListQuery(
                        Connection.TRANSACTION_READ_COMMITTED,
                        true,
                        "select\n"
                        + "  fbs.pkey\n"
                        + "from\n"
                        + "  businesses bu,\n"
                        + "  packages pk,\n"
                        + "  file_backup_settings fbs\n"
                        + "where\n"
                        + "  bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
                        + "  and bu.accounting=pk.accounting\n"
                        + "  and pk.pkey=fbs.package",
                        now
                    );
                    for(int c=0;c<fbss.size();c++) {
                        BackupHandler.removeFileBackupSetting(conn, invalidateList, fbss.getInt(c));
                    }
                }*/
                // TODO: Should also remove backup servers

                // httpd_sites
                {
                    IntList hss=conn.executeIntListQuery(
                        "select\n"
                        + "  hs.pkey\n"
                        + "from\n"
                        + "  httpd_sites hs,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  hs.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<hss.size();c++) {
                        HttpdHandler.removeHttpdSite(conn, invalidateList, hss.getInt(c));
                    }
                }

                // httpd_shared_tomcats
                {
                    IntList hsts=conn.executeIntListQuery(
                        "select\n"
                        + "  hst.pkey\n"
                        + "from\n"
                        + "  httpd_shared_tomcats hst,\n"
                        + "  linux_server_groups lsg,\n"
                        + "  linux_groups lg,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  hst.linux_server_group=lsg.pkey\n"
                        + "  and lsg.name=lg.name\n"
                        + "  and lg.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<hsts.size();c++) {
                        HttpdHandler.removeHttpdSharedTomcat(conn, invalidateList, hsts.getInt(c));
                    }
                }

                // private_ftp_servers
                {
                    IntList pfss=conn.executeIntListQuery(
                        "select\n"
                        + "  pfs.net_bind\n"
                        + "from\n"
                        + "  private_ftp_servers pfs,\n"
                        + "  net_binds nb,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  pfs.net_bind=nb.pkey\n"
                        + "  and nb.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<pfss.size();c++) {
                        FTPHandler.removePrivateFTPServer(conn, invalidateList, pfss.getInt(c));
                    }
                }

                // net_binds
                {
                    IntList nbs=conn.executeIntListQuery(
                        "select\n"
                        + "  nb.pkey\n"
                        + "from\n"
                        + "  net_binds nb,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  nb.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<nbs.size();c++) {
                        NetBindHandler.removeNetBind(conn, invalidateList, nbs.getInt(c));
                    }

                }

                // IPAddress
                {
                    IntList ias=conn.executeIntListQuery(
                        "select\n"
                        + "  ia.id\n"
                        + "from\n"
                        + "  \"IPAddress\" ia,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  ia.package=pk.pkey\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<ias.size();c++) {
                        int ia=ias.getInt(c);
                        IPAddressHandler.setIPAddressPackage(conn, invalidateList, ia, BusinessHandler.getRootBusiness());
                        IPAddressHandler.releaseIPAddress(conn, invalidateList, ia);
                    }
                }

                // httpd_servers
                {
                    IntList hss=conn.executeIntListQuery(
                        "select\n"
                        + "  hs.pkey\n"
                        + "from\n"
                        + "  httpd_servers hs,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  hs.package=pk.pkey\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<hss.size();c++) {
                        int hs=hss.getInt(c);
                        HttpdHandler.removeHttpdServer(conn, invalidateList, hs);
                    }
                }

                // linux_accounts
                {
                    List<UserId> las=conn.executeObjectListQuery(
						ObjectFactories.userIdFactory,
                        "select\n"
                        + "  la.username\n"
                        + "from\n"
                        + "  linux_accounts la,\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  la.username=un.username\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
					for (UserId la : las) {
						try {
							LinuxAccountHandler.removeLinuxAccount(conn, invalidateList, la);
						} catch (SQLException err) {
							System.err.println("SQLException trying to remove LinuxAccount: " + la);
							throw err;
						}
					}
                }

                // linux_groups
                {
                    List<GroupId> lgs=conn.executeObjectListQuery(
						ObjectFactories.groupIdFactory,
                        "select\n"
                        + "  lg.name\n"
                        + "from\n"
                        + "  linux_groups lg,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  lg.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
					for (GroupId lg : lgs) {
						try {
							LinuxAccountHandler.removeLinuxGroup(conn, invalidateList, lg);
						} catch (SQLException err) {
							System.err.println("SQLException trying to remove LinuxGroup: " + lg);
							throw err;
						}
					}
                }

                // mysql_databases
                {
                    IntList mds=conn.executeIntListQuery(
                        "select\n"
                        + "  md.pkey\n"
                        + "from\n"
                        + "  mysql_databases md,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  md.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<mds.size();c++) {
                        MySQLHandler.removeMySQLDatabase(conn, invalidateList, mds.getInt(c));
                    }
                }

                // mysql_users
                {
                    List<MySQLUserId> mus=conn.executeObjectListQuery(
						ObjectFactories.mySQLUserIdFactory,
                        "select\n"
                        + "  mu.username\n"
                        + "from\n"
                        + "  mysql_users mu,\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  mu.username=un.username\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
					for (MySQLUserId mu : mus) {
						MySQLHandler.removeMySQLUser(conn, invalidateList, mu);
					}
                }

                // postgres_databases
                {
                    IntList pds=conn.executeIntListQuery(
                        "select\n"
                        + "  pd.pkey\n"
                        + "from\n"
                        + "  postgres_databases pd,\n"
                        + "  postgres_server_users psu,\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  pd.datdba=psu.pkey\n"
                        + "  and psu.username=un.username\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
                    for(int c=0;c<pds.size();c++) {
                        PostgresHandler.removePostgresDatabase(conn, invalidateList, pds.getInt(c));
                    }
                }

                // postgres_users
                {
                    List<PostgresUserId> pus=conn.executeObjectListQuery(
						ObjectFactories.postgresUserIdFactory,
                        "select\n"
                        + "  pu.username\n"
                        + "from\n"
                        + "  postgres_users pu,\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  pu.username=un.username\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                        now
                    );
					for (PostgresUserId pu : pus) {
						PostgresHandler.removePostgresUser(conn, invalidateList, pu);
					}
                }

                // usernames
                // delete all closed usernames, unless used by a business_administrator that was left behind
                {
                    List<UserId> uns=conn.executeObjectListQuery(
						ObjectFactories.userIdFactory,
                        "select\n"
                        + "  un.username\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  un.package=pk.name\n"
                        + "  and pk.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
                        + "  and (select ba.username from business_administrators ba where ba.username=un.username) is null",
                        now
                    );
					for (UserId un : uns) {
						UsernameHandler.removeUsername(conn, invalidateList, un);
					}
                }

                // disable_log
                {
                    IntList dls=conn.executeIntListQuery(
                        "select\n"
                        + "  dl.pkey\n"
                        + "from\n"
                        + "  disable_log dl,\n"
                        + "  businesses bu\n"
                        + "where\n"
                        + "  dl.accounting=bu.accounting\n"
                        + "  and bu.canceled is not null\n"
                        + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS+"\n"
                        + "  and (select ba.username from business_administrators ba where ba.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select bu2.accounting from businesses bu2 where bu2.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select cr.pkey from cvs_repositories cr where cr.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select el.pkey from email_lists el where el.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select ep.pkey from email_pipes ep where ep.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select esr.pkey from email_smtp_relays esr where esr.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select hst.pkey from httpd_shared_tomcats hst where hst.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select hsb.pkey from httpd_site_binds hsb where hsb.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select hs.pkey from httpd_sites hs where hs.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select la.username from linux_accounts la where la.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select lsa.pkey from linux_server_accounts lsa where lsa.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select msu.pkey from mysql_server_users msu where msu.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select mu.username from mysql_users mu where mu.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select pk.name from packages pk where pk.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select psu.pkey from postgres_server_users psu where psu.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select pu.username from postgres_users pu where pu.disable_log=dl.pkey limit 1) is null\n"
                        + "  and (select un.username from usernames un where un.disable_log=dl.pkey limit 1) is null",
                        now
                    );
                    for(int c=0;c<dls.size();c++) BusinessHandler.removeDisableLog(conn, invalidateList, dls.getInt(c));
                }

                // business_servers
                // delete all business_servers for canceled businesses
                {
                    for(int depth = Business.MAXIMUM_BUSINESS_TREE_DEPTH; depth>=1; depth--) {
                        // non-default
                        IntList bss=conn.executeIntListQuery(
                            "select\n"
                            + "  bs.pkey\n"
                            + "from\n"
                            + "  business_servers bs,\n"
                            + "  businesses bu\n"
                            + "where\n"
                            + "  not bs.is_default\n"
                            + "  and bs.accounting=bu.accounting\n"
                            + "  and bu.canceled is not null\n"
                            + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                            now
                        );
                        for(int c=0;c<bss.size();c++) {
                            int bs = bss.getInt(c);
                            AccountingCode accounting = AccountingCode.valueOf(conn.executeStringQuery("select accounting from business_servers where pkey=?", bs));
                            int bsDepth = BusinessHandler.getDepthInBusinessTree(conn, accounting);
                            if(bsDepth==depth) BusinessHandler.removeBusinessServer(conn, invalidateList, bs);
                        }

                        // default
                        bss=conn.executeIntListQuery(
                            "select\n"
                            + "  bs.pkey\n"
                            + "from\n"
                            + "  business_servers bs,\n"
                            + "  businesses bu\n"
                            + "where\n"
                            + "  bs.accounting=bu.accounting\n"
                            + "  and bu.canceled is not null\n"
                            + "  and (?::date-bu.canceled::date)>"+CANCELED_KEEP_DAYS,
                            now
                        );
                        for(int c=0;c<bss.size();c++) {
                            int bs = bss.getInt(c);
                            AccountingCode accounting = AccountingCode.valueOf(conn.executeStringQuery("select accounting from business_servers where pkey=?", bs));
                            int bsDepth = BusinessHandler.getDepthInBusinessTree(conn, accounting);
                            if(bsDepth==depth) BusinessHandler.removeBusinessServer(conn, invalidateList, bs);
                        }
                    }
                }
                if(message.length()>0) {
                    logger.log(Level.WARNING, message.toString());
                }
            } catch(RuntimeException | ValidationException | IOException err) {
                if(conn.rollback()) {
                    connRolledBack=true;
                    invalidateList=null;
                }
                throw err;
            } catch(SQLException err) {
                if(conn.rollbackAndClose()) {
                    connRolledBack=true;
                    invalidateList=null;
                }
                throw err;
            } finally {
                if(!connRolledBack && !conn.isClosed()) conn.commit();
            }
        } finally {
            conn.releaseConnection();
        }
    }
    
    public static void main(String[] args) {
        try {
            InvalidateList invalidateList = new InvalidateList();
            cleanNow(invalidateList);
            for(SchemaTable.TableID tableId : SchemaTable.TableID.values()) {
                List<Integer> affectedServers = invalidateList.getAffectedServers(tableId);
                if(affectedServers!=null) {
                    if(affectedServers==InvalidateList.allServers) {
                        System.out.println("invalidate "+tableId.name().toLowerCase(Locale.ENGLISH));
                    } else {
                        for(int pkey : affectedServers) {
                            System.out.println("invalidate "+tableId.name().toLowerCase(Locale.ENGLISH)+" "+pkey);
                        }
                    }
                }
            }
        } catch(RuntimeException | ValidationException | IOException | SQLException err) {
            ErrorPrinter.printStackTraces(err);
            System.exit(1);
        }
    }
}