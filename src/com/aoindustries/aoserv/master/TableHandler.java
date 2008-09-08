package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOSHCommand;
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.client.AOServerDaemonHost;
import com.aoindustries.aoserv.client.Action;
import com.aoindustries.aoserv.client.ActionType;
import com.aoindustries.aoserv.client.Architecture;
import com.aoindustries.aoserv.client.BackupPartition;
import com.aoindustries.aoserv.client.BackupReport;
import com.aoindustries.aoserv.client.BackupRetention;
import com.aoindustries.aoserv.client.Bank;
import com.aoindustries.aoserv.client.BankAccount;
import com.aoindustries.aoserv.client.BankTransaction;
import com.aoindustries.aoserv.client.BankTransactionType;
import com.aoindustries.aoserv.client.BlackholeEmailAddress;
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorPermission;
import com.aoindustries.aoserv.client.BusinessProfile;
import com.aoindustries.aoserv.client.BusinessServer;
import com.aoindustries.aoserv.client.CountryCode;
import com.aoindustries.aoserv.client.CreditCard;
import com.aoindustries.aoserv.client.CreditCardProcessor;
import com.aoindustries.aoserv.client.CreditCardTransaction;
import com.aoindustries.aoserv.client.CvsRepository;
import com.aoindustries.aoserv.client.DNSForbiddenZone;
import com.aoindustries.aoserv.client.DNSRecord;
import com.aoindustries.aoserv.client.DNSTLD;
import com.aoindustries.aoserv.client.DNSType;
import com.aoindustries.aoserv.client.DNSZone;
import com.aoindustries.aoserv.client.DaemonProfile;
import com.aoindustries.aoserv.client.DisableLog;
import com.aoindustries.aoserv.client.DistroFile;
import com.aoindustries.aoserv.client.DistroFileType;
import com.aoindustries.aoserv.client.EmailAddress;
import com.aoindustries.aoserv.client.EmailAttachmentBlock;
import com.aoindustries.aoserv.client.EmailAttachmentType;
import com.aoindustries.aoserv.client.EmailDomain;
import com.aoindustries.aoserv.client.EmailForwarding;
import com.aoindustries.aoserv.client.EmailList;
import com.aoindustries.aoserv.client.EmailListAddress;
import com.aoindustries.aoserv.client.EmailPipe;
import com.aoindustries.aoserv.client.EmailPipeAddress;
import com.aoindustries.aoserv.client.EmailSmtpRelay;
import com.aoindustries.aoserv.client.EmailSmtpRelayType;
import com.aoindustries.aoserv.client.EmailSpamAssassinIntegrationMode;
import com.aoindustries.aoserv.client.EncryptionKey;
import com.aoindustries.aoserv.client.ExpenseCategory;
import com.aoindustries.aoserv.client.FTPGuestUser;
import com.aoindustries.aoserv.client.FailoverFileLog;
import com.aoindustries.aoserv.client.FailoverFileReplication;
import com.aoindustries.aoserv.client.FailoverFileSchedule;
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.FileBackupSetting;
import com.aoindustries.aoserv.client.HttpdBind;
import com.aoindustries.aoserv.client.HttpdJBossSite;
import com.aoindustries.aoserv.client.HttpdJBossVersion;
import com.aoindustries.aoserv.client.HttpdJKCode;
import com.aoindustries.aoserv.client.HttpdJKProtocol;
import com.aoindustries.aoserv.client.HttpdServer;
import com.aoindustries.aoserv.client.HttpdSharedTomcat;
import com.aoindustries.aoserv.client.HttpdSite;
import com.aoindustries.aoserv.client.HttpdSiteAuthenticatedLocation;
import com.aoindustries.aoserv.client.HttpdSiteBind;
import com.aoindustries.aoserv.client.HttpdSiteURL;
import com.aoindustries.aoserv.client.HttpdStaticSite;
import com.aoindustries.aoserv.client.HttpdTomcatContext;
import com.aoindustries.aoserv.client.HttpdTomcatDataSource;
import com.aoindustries.aoserv.client.HttpdTomcatParameter;
import com.aoindustries.aoserv.client.HttpdTomcatSharedSite;
import com.aoindustries.aoserv.client.HttpdTomcatSite;
import com.aoindustries.aoserv.client.HttpdTomcatStdSite;
import com.aoindustries.aoserv.client.HttpdTomcatVersion;
import com.aoindustries.aoserv.client.HttpdWorker;
import com.aoindustries.aoserv.client.IPAddress;
import com.aoindustries.aoserv.client.LinuxAccAddress;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxAccountType;
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.LinuxGroupAccount;
import com.aoindustries.aoserv.client.LinuxGroupType;
import com.aoindustries.aoserv.client.LinuxServerAccount;
import com.aoindustries.aoserv.client.LinuxServerGroup;
import com.aoindustries.aoserv.client.MajordomoList;
import com.aoindustries.aoserv.client.MajordomoServer;
import com.aoindustries.aoserv.client.MajordomoVersion;
import com.aoindustries.aoserv.client.MasterHost;
import com.aoindustries.aoserv.client.MasterServerProfile;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.MonthlyCharge;
import com.aoindustries.aoserv.client.MySQLDBUser;
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLReservedWord;
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.MySQLServerUser;
import com.aoindustries.aoserv.client.MySQLUser;
import com.aoindustries.aoserv.client.NetBind;
import com.aoindustries.aoserv.client.NetDevice;
import com.aoindustries.aoserv.client.NetDeviceID;
import com.aoindustries.aoserv.client.NetProtocol;
import com.aoindustries.aoserv.client.NetTcpRedirect;
import com.aoindustries.aoserv.client.NoticeLog;
import com.aoindustries.aoserv.client.NoticeType;
import com.aoindustries.aoserv.client.OperatingSystem;
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.PackageCategory;
import com.aoindustries.aoserv.client.PackageDefinition;
import com.aoindustries.aoserv.client.PackageDefinitionLimit;
import com.aoindustries.aoserv.client.PaymentType;
import com.aoindustries.aoserv.client.PostgresDatabase;
import com.aoindustries.aoserv.client.PostgresEncoding;
import com.aoindustries.aoserv.client.PostgresReservedWord;
import com.aoindustries.aoserv.client.PostgresServer;
import com.aoindustries.aoserv.client.PostgresServerUser;
import com.aoindustries.aoserv.client.PostgresUser;
import com.aoindustries.aoserv.client.PostgresVersion;
import com.aoindustries.aoserv.client.PrivateFTPServer;
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.client.Resource;
import com.aoindustries.aoserv.client.SchemaColumn;
import com.aoindustries.aoserv.client.SchemaForeignKey;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.SchemaType;
import com.aoindustries.aoserv.client.Server;
import com.aoindustries.aoserv.client.ServerFarm;
import com.aoindustries.aoserv.client.Shell;
import com.aoindustries.aoserv.client.SignupRequest;
import com.aoindustries.aoserv.client.SignupRequestOption;
import com.aoindustries.aoserv.client.SpamEmailMessage;
import com.aoindustries.aoserv.client.SystemEmailAlias;
import com.aoindustries.aoserv.client.Technology;
import com.aoindustries.aoserv.client.TechnologyClass;
import com.aoindustries.aoserv.client.TechnologyName;
import com.aoindustries.aoserv.client.TechnologyVersion;
import com.aoindustries.aoserv.client.Ticket;
import com.aoindustries.aoserv.client.TicketPriority;
import com.aoindustries.aoserv.client.TicketStatus;
import com.aoindustries.aoserv.client.TicketType;
import com.aoindustries.aoserv.client.TimeZone;
import com.aoindustries.aoserv.client.Transaction;
import com.aoindustries.aoserv.client.TransactionType;
import com.aoindustries.aoserv.client.USState;
import com.aoindustries.aoserv.client.Username;
import com.aoindustries.aoserv.client.WhoisHistory;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.profiler.MethodProfile;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>TableHandler</code> handles all the accesses to the AOServ tables.
 *
 * @author  AO Industries, Inc.
 */
final public class TableHandler {

    /**
     * The number of rows that will be loaded into each ResultSet for large tables.
     * This is done to get around the fact that the PostgreSQL JDBC loads all results
     * into the ResultSet, causing OutOfMemoryErrors on tables with millions of rows.
     */
    public static final int RESULT_SET_BATCH_SIZE=500;

    /**
     * The number of rows statements that should typically be used per update/insert/delete batch.
     */
    public static final int UPDATE_BATCH_SIZE=500;

    /**
     * The number of updates that will typically be done before the changes are committed.
     */
    public static final int BATCH_COMMIT_INTERVAL=500;

    /**
     * The joins used for the business tree.
     */
    public static final String
        BU1_PARENTS_JOIN=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting,\n",
        BU1_PARENTS_JOIN_NO_COMMA=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting\n",
        BU2_PARENTS_JOIN=
              "      businesses bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+"\n"
            + "      left join businesses bu8 on bu7.parent=bu8.accounting\n"
            + "      left join businesses bu9 on bu8.parent=bu9.accounting\n"
            + "      left join businesses bu10 on bu9.parent=bu10.accounting\n"
            + "      left join businesses bu11 on bu10.parent=bu11.accounting\n"
            + "      left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+" on bu11.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".accounting,\n"
    ;
    
    /**
     * The where clauses that accompany the joins.
     */
    public static final String
        PK_BU1_PARENTS_WHERE=
              "    pk.accounting=bu1.accounting\n"
            + "    or pk.accounting=bu1.parent\n"
            + "    or pk.accounting=bu2.parent\n"
            + "    or pk.accounting=bu3.parent\n"
            + "    or pk.accounting=bu4.parent\n"
            + "    or pk.accounting=bu5.parent\n"
            + "    or pk.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        PK1_BU1_PARENTS_OR_WHERE=
              "    or pk1.accounting=bu1.accounting\n"
            + "    or pk1.accounting=bu1.parent\n"
            + "    or pk1.accounting=bu2.parent\n"
            + "    or pk1.accounting=bu3.parent\n"
            + "    or pk1.accounting=bu4.parent\n"
            + "    or pk1.accounting=bu5.parent\n"
            + "    or pk1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        PK1_BU1_PARENTS_WHERE=
              "    pk1.accounting=bu1.accounting\n"
            + "    or pk1.accounting=bu1.parent\n"
            + "    or pk1.accounting=bu2.parent\n"
            + "    or pk1.accounting=bu3.parent\n"
            + "    or pk1.accounting=bu4.parent\n"
            + "    or pk1.accounting=bu5.parent\n"
            + "    or pk1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        PK3_BU2_PARENTS_OR_WHERE=
              "        or pk3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or pk3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or pk3.accounting=bu8.parent\n"
            + "        or pk3.accounting=bu9.parent\n"
            + "        or pk3.accounting=bu10.parent\n"
            + "        or pk3.accounting=bu11.parent\n"
            + "        or pk3.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n",
        PK3_BU2_PARENTS_WHERE=
              "        pk3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or pk3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or pk3.accounting=bu8.parent\n"
            + "        or pk3.accounting=bu9.parent\n"
            + "        or pk3.accounting=bu10.parent\n"
            + "        or pk3.accounting=bu11.parent\n"
            + "        or pk3.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n"
    ;

    /**
     * Gets one object from a table.
     */
    public static void getObject(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataInputStream in,
        CompressedDataOutputStream out,
        SchemaTable.TableID tableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "getObject(MasterDatabaseConnection,RequestSource,CompressedDataInputStream,CompressedDataOutputStream,SchemaTable.TableID)", getTableName(conn, tableID));
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, source.getUsername());
            switch(tableID) {
                case ACTIONS :
                    int actionId=in.readCompressedInt();
                    if(TicketHandler.canAccessAction(conn, source, actionId)) {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select * from actions where pkey=?",
                            actionId,
                            new Action()
                        );
                    } else {
                        out.writeByte(AOServProtocol.DONE);
                    }
                    break;
                case BACKUP_REPORTS :
                {
                    int pkey=in.readCompressedInt();
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select * from backup_reports where pkey=?",
                            pkey,
                            new BackupReport()
                        ); else MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select\n"
                            + "  br.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  backup_reports br\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=br.server\n"
                            + "  and br.pkey=?",
                            username,
                            pkey,
                            new BackupReport()
                        );
                    } else {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select\n"
                            + "  br.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  packages pk2,\n"
                            + "  backup_reports br\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=pk2.accounting\n"
                            + "  and pk2.pkey=br.package\n"
                            + "  and br.pkey=?",
                            username,
                            pkey,
                            new BackupReport()
                        );
                    }
                    break;
                }
                case BANK_TRANSACTIONS :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select\n"
                            + "  time::date,\n"
                            + "  transid,\n"
                            + "  bank_account,\n"
                            + "  processor,\n"
                            + "  administrator,\n"
                            + "  type,\n"
                            + "  expense_code,\n"
                            + "  description,\n"
                            + "  check_no,\n"
                            + "  amount,\n"
                            + "  confirmed\n"
                            + "from\n"
                            + "  bank_transactions\n"
                            + "where\n"
                            + "  transid=?",
                            in.readCompressedInt(),
                            new BankTransaction()
                        );
                    } else out.writeByte(AOServProtocol.DONE);
                    break;
                case SPAM_EMAIL_MESSAGES :
                    {
                        int pkey=in.readCompressedInt();
                        if(masterUser!=null && masterServers.length==0) MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select * from spam_email_messages where pkey=?",
                            pkey,
                            new SpamEmailMessage()
                        ); else throw new SQLException("Only master users may access spam_email_messages.");
                    }
                    break;
                case TICKETS :
                    int ticketId=in.readCompressedInt();
                    if(TicketHandler.isTicketAdmin(conn, source)) {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                              "select "
                            + "  *\n"
                            + "from\n"
                            + "  tickets\n"
                            + "where\n"
                            + "  pkey=?",
                            ticketId,
                            new Ticket()
                        );
                    } else if(TicketHandler.canAccessTicket(conn, source, ticketId)) {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select\n"
                            + "  *\n"
                            + "from\n"
                            + "  tickets\n"
                            + "where\n"
                            + "  pkey=?",
                            ticketId,
                            new Ticket()
                        );
                    } else {
                        out.writeByte(AOServProtocol.DONE);
                    }
                    break;
                case TRANSACTIONS :
                    int transid=in.readCompressedInt();
                    if(TransactionHandler.canAccessTransaction(conn, source, transid)) {
                        MasterServer.writeObject(
                            conn,
                            source,
                            out,
                            "select * from transactions where transid=?",
                            transid,
                            new Transaction()
                        );
                    } else {
                        out.writeShort(AOServProtocol.DONE);
                    }
                    break;
                default :
                    throw new IOException("Unknown table ID: "+tableID);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Caches row counts for each table on a per-username basis.
     */
    private static final Map<String,int[]> rowCountsPerUsername=new HashMap<String,int[]>();
    private static final Map<String,long[]> expireTimesPerUsername=new HashMap<String,long[]>();

    private static final int MAX_ROW_COUNT_CACHE_AGE=60*60*1000;

    /** Copy used to avoid multiple array copies on each access. */
    private static final SchemaTable.TableID[] tableIDs = SchemaTable.TableID.values();
    private static final int numTables = tableIDs.length;

    public static int getCachedRowCount(
        MasterDatabaseConnection conn,
        RequestSource source,
        SchemaTable.TableID tableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "getCachedRowCount(MasterDatabaseConnection,RequestSource,SchemaTable.TableID)", getTableName(conn, tableID));
        try {
            String username=source.getUsername();

            // Synchronize to get the correct objects
            int[] rowCounts;
            long[] expireTimes;
            synchronized(rowCountsPerUsername) {
                rowCounts=rowCountsPerUsername.get(username);
                if(rowCounts==null) {
                    rowCountsPerUsername.put(username, rowCounts=new int[numTables]);
                    expireTimesPerUsername.put(username, expireTimes=new long[numTables]);
                } else expireTimes=expireTimesPerUsername.get(username);
            }

            // Synchronize on the array to provide a per-user lock
            synchronized(rowCounts) {
                long expireTime=expireTimes[tableID.ordinal()];
                long startTime=System.currentTimeMillis();
                if(
                    expireTime==0
                    || expireTime<=startTime
                    || expireTime>(startTime+MAX_ROW_COUNT_CACHE_AGE)
                ) {
                    rowCounts[tableID.ordinal()]=getRowCount(
                        conn,
                        source,
                        tableID
                    );
                    expireTimes[tableID.ordinal()]=System.currentTimeMillis()+MAX_ROW_COUNT_CACHE_AGE;
                }

                return rowCounts[tableID.ordinal()];
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the number of accessible rows in a table.
     */
    public static int getRowCount(
        MasterDatabaseConnection conn,
        RequestSource source,
        SchemaTable.TableID tableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "getRowCount(MasterDatabaseConnection,RequestSource,SchemaTable.TableID)", getTableName(conn, tableID));
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, source.getUsername());
            switch(tableID) {
                case ACTIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) return conn.executeIntQuery(
                            Connection.TRANSACTION_READ_COMMITTED,
                            true,
                            true,
                            "select count(*) from actions"
                        ); else return 0;
                    } else return conn.executeIntQuery(
                        Connection.TRANSACTION_READ_COMMITTED,
                        true,
                        true,
                        "select\n"
                        + "  count(*)\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  tickets ti,\n"
                        + "  actions ac\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=ti.accounting\n"
                        + "  and ti.pkey=ac.ticket_id",
                        username
                    );
                case DISTRO_FILES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            if(AOServProtocol.compareVersions(source.getProtocolVersion(), AOServProtocol.VERSION_1_0_A_107)<=0) {
                                return 0;
                            } else {
                                return conn.executeIntQuery(
                                    Connection.TRANSACTION_READ_COMMITTED,
                                    true,
                                    true,
                                    "select count(*) from distro_files"
                                );
                            }
                        } else {
                            // Restrict to the operating system versions accessible to this user
                            IntList osVersions=getOperatingSystemVersions(conn, source);
                            if(osVersions.size()==0) return 0;
                            StringBuilder sql=new StringBuilder();
                            sql.append("select count(*) from distro_files where operating_system_version in (");
                            for(int c=0;c<osVersions.size();c++) {
                                if(c>0) sql.append(',');
                                sql.append(osVersions.getInt(c));
                            }
                            sql.append(')');
                            return conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, sql.toString());
                        }
                    } else return 0;
                case TICKETS :
                    if(masterUser!=null) {
                        if(masterServers.length==0 || masterUser.isTicketAdmin()) return conn.executeIntQuery(
                            Connection.TRANSACTION_READ_COMMITTED,
                            true,
                            true,
                            "select count(*) from tickets"
                        ); else return 0;
                    } else return conn.executeIntQuery(
                        Connection.TRANSACTION_READ_COMMITTED,
                        true,
                        true,
                        "select\n"
                        + "  count(*)\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  tickets ti\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=ti.accounting",
                        username
                    );
                default :
                    throw new IOException("Unknown table ID: "+tableID);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets an entire table.
     */
    public static void getTable(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataInputStream in,
        CompressedDataOutputStream out,
        boolean provideProgress,
        final SchemaTable.TableID tableID
    ) throws IOException, SQLException {
        final int profilerLevel=tableID==SchemaTable.TableID.MASTER_PROCESSES?Profiler.FAST:Profiler.UNKNOWN;
        Profiler.startProfile(profilerLevel, TableHandler.class, "getTable(MasterDatabaseConnection,RequestSource,CompressedDataInputStream,CompressedDataOutputStream,boolean,SchemaTable.TableID)", getTableName(conn, tableID));
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);

            switch(tableID) {
                case ACTIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Action(),
                            "select * from actions"
                        ); else {
                            List<Action> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Action(),
                        "select\n"
                        + "  ac.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  tickets ti,\n"
                        + "  actions ac\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=ti.accounting\n"
                        + "  and ti.pkey=ac.ticket_id",
                        username
                    );
                    break;
                case ACTION_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new ActionType(),
                        "select * from action_types"
                    );
                    break;
                case AO_SERVER_DAEMON_HOSTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new AOServerDaemonHost(),
                            "select * from ao_server_daemon_hosts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new AOServerDaemonHost(),
                            "select\n"
                            + "  sdh.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_server_daemon_hosts sdh\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=sdh.ao_server",
                            username
                        );
                    } else {
                        List<AOServerDaemonHost> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case AO_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new AOServer(),
                            "select * from ao_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new AOServer(),
                            "select distinct\n"
                            + "  ao2.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  inner join ao_servers ao on ms.server=ao.server\n"
                            // Allow its failover parent
                            + "  left join ao_servers ff on ao.failover_server=ff.server\n"
                            // Allow its failover children
                            + "  left join ao_servers fs on ao.server=fs.failover_server\n"
                            // Allow servers it replicates to
                            + "  left join failover_file_replications ffr on ms.server=ffr.server\n"
                            + "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
                            + "  ao_servers ao2\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            // Allow direct access
                            + "    ms.server=ao2.server\n"
                            // Allow its failover parent
                            + "    or ff.server=ao2.server\n"
                            // Allow its failover children
                            + "    or fs.server=ao2.server\n"
                            // Allow servers it replicates to
                            + "    or bp.ao_server=ao2.server\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new AOServer(),
                        "select distinct\n"
                        + "  ao.server,\n"
                        + "  ao.hostname,\n"
                        + "  ao.daemon_bind,\n"
                        + "  '"+AOServProtocol.FILTERED+"'::text,\n"
                        + "  ao.pool_size,\n"
                        + "  ao.distro_hour,\n"
                        + "  ao.last_distro_time,\n"
                        + "  ao.failover_server,\n"
                        + "  ao.daemon_device_id,\n"
                        + "  ao.daemon_connect_bind,\n"
                        + "  ao.time_zone,\n"
                        + "  ao.jilter_bind,\n"
                        + "  ao.restrict_outbound_email,\n"
                        + "  ao.daemon_connect_address,\n"
                        + "  ao.failover_batch_size\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        // Allow servers it replicates to
                        //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
                        //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
                        + "  ao_servers ao\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and (\n"
                        + "    bs.server=ao.server\n"
                        // Allow servers it replicates to
                        //+ "    or bp.ao_server=ao.server\n"
                        + "  )",
                        username
                    );
                    break;
                case AOSERV_PERMISSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new AOServPermission(),
                        "select * from aoserv_permissions"
                    );
                    break;
                case AOSERV_PROTOCOLS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new AOServProtocol(),
                        "select * from aoserv_protocols"
                    );
                    break;
                case AOSH_COMMANDS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new AOSHCommand(),
                        "select\n"
                        + "  ac.*\n"
                        + "from\n"
                        + "  aoserv_protocols client_ap,\n"
                        + "  aosh_commands ac\n"
                        + "  inner join aoserv_protocols since_ap on ac.since_version=since_ap.version\n"
                        + "  left outer join aoserv_protocols last_ap on ac.last_version=last_ap.version\n"
                        + "where\n"
                        + "  client_ap.version=?\n"
                        + "  and client_ap.created>=since_ap.created\n"
                        + "  and (\n"
                        + "    last_ap.created is null\n"
                        + "    or client_ap.created<=last_ap.created\n"
                        + "  )",
                        source.getProtocolVersion()
                    );
                    break;
                case ARCHITECTURES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Architecture(),
                        "select * from architectures"
                    );
                    break;
                case BACKUP_PARTITIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BackupPartition(),
                            "select * from backup_partitions"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BackupPartition(),
                            "select\n"
                            + "  bp.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  backup_partitions bp\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            + "    ms.server=bp.ao_server\n"
                            + "    or (\n"
                            + "      select\n"
                            + "        ffr.pkey\n"
                            + "      from\n"
                            + "        failover_file_replications ffr\n"
                            + "        inner join backup_partitions bp2 on ffr.backup_partition=bp2.pkey\n"
                            + "      where\n"
                            + "        ms.server=ffr.server\n"
                            + "        and bp.ao_server=bp2.ao_server\n"
                            + "      limit 1\n"
                            + "    ) is not null\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BackupPartition(),
                        "select distinct\n"
                        + "  bp.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  backup_partitions bp\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and (\n"
                        + "    bs.server=bp.ao_server\n"
                        //+ "    or (\n"
                        //+ "      select\n"
                        //+ "        ffr.pkey\n"
                        //+ "      from\n"
                        //+ "        failover_file_replications ffr\n"
                        //+ "        inner join backup_partitions bp2 on ffr.backup_partition=bp2.pkey\n"
                        //+ "      where\n"
                        //+ "        bs.server=ffr.server\n"
                        //+ "        and bp.ao_server=bp2.ao_server\n"
                        //+ "      limit 1\n"
                        //+ "    ) is not null\n"
                        + "  )",
                        username
                    );
                    break;
                case BACKUP_REPORTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BackupReport(),
                            "select * from backup_reports"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BackupReport(),
                            "select\n"
                            + "  br.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  backup_reports br\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=br.server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BackupReport(),
                        "select\n"
                        + "  br.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  backup_reports br\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.pkey=br.package",
                        username
                    );
                    break;
                case BACKUP_RETENTIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BackupRetention(),
                        "select * from backup_retentions"
                    );
                    break;
                case BANK_ACCOUNTS :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BankAccount(),
                            "select * from bank_accounts"
                        );
                    } else {
                        List<BankAccount> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case BANK_TRANSACTIONS :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BankTransaction(),
                            "select * from bank_transactions"
                        );
                    } else {
                        List<BankTransaction> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case BANK_TRANSACTION_TYPES :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BankTransactionType(),
                            "select * from bank_transaction_types"
                        );
                    } else {
                        List<BankTransactionType> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case BANKS :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Bank(),
                            "select * from banks"
                        );
                    } else {
                        List<Bank> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case BLACKHOLE_EMAIL_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BlackholeEmailAddress(),
                            "select * from blackhole_email_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BlackholeEmailAddress(),
                            "select\n"
                            + "  bh.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea,\n"
                            + "  blackhole_email_addresses bh\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain\n"
                            + "  and ea.pkey=bh.email_address",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BlackholeEmailAddress(),
                        "select\n"
                        + "  bh.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea,\n"
                        + "  blackhole_email_addresses bh\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain\n"
                        + "  and ea.pkey=bh.email_address",
                        username
                    );
                    break;
                case BUSINESS_ADMINISTRATORS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessAdministrator(),
                            "select * from business_administrators"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessAdministrator(),
                            "select distinct\n"
                            + "  ba.username,\n"
                            + "  '"+BusinessAdministrator.NO_PASSWORD+"'::text,\n"
                            + "  ba.name,\n"
                            + "  ba.title,\n"
                            + "  ba.birthday,\n"
                            + "  ba.is_preferred,\n"
                            + "  ba.private,\n"
                            + "  ba.created,\n"
                            + "  ba.work_phone,\n"
                            + "  ba.home_phone,\n"
                            + "  ba.cell_phone,\n"
                            + "  ba.fax,\n"
                            + "  ba.email,\n"
                            + "  ba.address1,\n"
                            + "  ba.address2,\n"
                            + "  ba.city,\n"
                            + "  ba.state,\n"
                            + "  ba.country,\n"
                            + "  ba.zip,\n"
                            + "  ba.disable_log,\n"
                            + "  ba.can_switch_users\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  business_administrators ba\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=ba.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BusinessAdministrator(),
                        "select\n"
                        + "  ba.username,\n"
                        + "  '"+BusinessAdministrator.NO_PASSWORD+"'::text,\n"
                        + "  ba.name,\n"
                        + "  ba.title,\n"
                        + "  ba.birthday,\n"
                        + "  ba.is_preferred,\n"
                        + "  ba.private,\n"
                        + "  ba.created,\n"
                        + "  ba.work_phone,\n"
                        + "  ba.home_phone,\n"
                        + "  ba.cell_phone,\n"
                        + "  ba.fax,\n"
                        + "  ba.email,\n"
                        + "  ba.address1,\n"
                        + "  ba.address2,\n"
                        + "  ba.city,\n"
                        + "  ba.state,\n"
                        + "  ba.country,\n"
                        + "  ba.zip,\n"
                        + "  ba.disable_log,\n"
                        + "  ba.can_switch_users\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  business_administrators ba\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=ba.username",
                        username
                    );
                    break;
                case BUSINESS_ADMINISTRATOR_PERMISSIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessAdministratorPermission(),
                            "select * from business_administrator_permissions"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessAdministratorPermission(),
                            "select distinct\n"
                            + "  bp.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  business_administrator_permissions bp\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=bp.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BusinessAdministratorPermission(),
                        "select\n"
                        + "  bp.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  business_administrator_permissions bp\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=bp.username",
                        username
                    );
                    break;
                case BUSINESS_PROFILES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessProfile(),
                            "select * from business_profiles"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessProfile(),
                            "select distinct\n"
                            + "  bp.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  business_profiles bp\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=bp.accounting",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BusinessProfile(),
                        "select\n"
                        + "  bp.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN
                        + "  business_profiles bp\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=bp.accounting",
                        username
                    );
                    break;
                case BUSINESS_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessServer(),
                            "select * from business_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new BusinessServer(),
                            "select distinct\n"
                            + "  bs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new BusinessServer(),
                        "select\n"
                        + "  bs.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN
                        + "  business_servers bs\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=bs.accounting",
                        username
                    );
                    break;
                case BUSINESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Business(),
                            "select * from businesses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Business(),
                            "select distinct\n"
                            + "  bu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  businesses bu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=bu.accounting",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Business(),
                        "select\n"
                        + "  bu1.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN_NO_COMMA
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )",
                        username
                    );
                    break;
                case CLIENT_JVM_PROFILE :
                    throw new IOException("ClientJvmProfiles should be generated, not obtained from the MasterDatabase.getDatabase().");
                case CREDIT_CARD_PROCESSORS :
                    if(BusinessHandler.hasPermission(conn, source, AOServPermission.Permission.get_credit_card_processors)) {
                        if(masterUser!=null) {
                            if(masterServers.length==0) MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new CreditCardProcessor(),
                                "select * from credit_card_processors"
                            ); else {
                                List<CreditCardProcessor> emptyList = Collections.emptyList();
                                MasterServer.writeObjects(source, out, provideProgress, emptyList);
                            }
                        } else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new CreditCardProcessor(),
                            "select\n"
                            + "  ccp.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk,\n"
                            + BU1_PARENTS_JOIN
                            + "  credit_card_processors ccp\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk.name\n"
                            + "  and (\n"
                            + PK_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=ccp.accounting",
                            username
                        );
                    } else {
                        // No permission, return empty list
                        List<CreditCardProcessor> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case CREDIT_CARDS :
                    if(BusinessHandler.hasPermission(conn, source, AOServPermission.Permission.get_credit_cards)) {
                        if(masterUser!=null) {
                            if(masterServers.length==0) MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new CreditCard(),
                                "select * from credit_cards"
                            ); else {
                                List<CreditCard> emptyList = Collections.emptyList();
                                MasterServer.writeObjects(source, out, provideProgress, emptyList);
                            }
                        } else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new CreditCard(),
                            "select\n"
                            + "  cc.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk,\n"
                            + BU1_PARENTS_JOIN
                            + "  credit_cards cc\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk.name\n"
                            + "  and (\n"
                            + PK_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=cc.accounting",
                            username
                        );
                    } else {
                        // No permission, return empty list
                        List<CreditCard> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case CREDIT_CARD_TRANSACTIONS :
                    if(BusinessHandler.hasPermission(conn, source, AOServPermission.Permission.get_credit_card_transactions)) {
                        if(masterUser!=null) {
                            if(masterServers.length==0) MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new CreditCardTransaction(),
                                "select * from credit_card_transactions"
                            ); else {
                                List<CreditCardTransaction> emptyList = Collections.emptyList();
                                MasterServer.writeObjects(source, out, provideProgress, emptyList);
                            }
                        } else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new CreditCardTransaction(),
                            "select\n"
                            + "  cct.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk,\n"
                            + BU1_PARENTS_JOIN
                            + "  credit_card_transactions cct\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk.name\n"
                            + "  and (\n"
                            + PK_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=cct.accounting",
                            username
                        );
                    } else {
                        // No permission, return empty list
                        List<CreditCardTransaction> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case COUNTRY_CODES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new CountryCode(),
                        "select * from country_codes"
                    );
                    break;
                case CVS_REPOSITORIES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new CvsRepository(),
                            "select * from cvs_repositories"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new CvsRepository(),
                            "select\n"
                            + "  cr.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  linux_server_accounts lsa,\n"
                            + "  cvs_repositories cr\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=lsa.ao_server\n"
                            + "  and lsa.pkey=cr.linux_server_account",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new CvsRepository(),
                        "select\n"
                        + "  cr.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  linux_server_accounts lsa,\n"
                        + "  cvs_repositories cr\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=lsa.username\n"
                        + "  and lsa.pkey=cr.linux_server_account",
                        username
                    );
                    break;
                case DAEMON_PROFILE :
                    {
                        List<DaemonProfile> objs=new ArrayList<DaemonProfile>();
                        if(masterUser!=null && masterServers.length==0) {
                            // Get a list of all the servers
                            IntList aoServers=AOServerHandler.getAOServers(conn);
                            int serversSize=aoServers.size();
                            for(int c=0;c<serversSize;c++) {
                                int aoServer=aoServers.getInt(c);
                                if(DaemonHandler.isDaemonAvailable(aoServer)) {
                                    try {
                                        DaemonHandler.getDaemonConnector(conn, aoServer).getDaemonProfile(objs);
                                    } catch(SocketException err) {
                                        DaemonHandler.flagDaemonAsDown(aoServer);

                                        // Connection reset happens when the daemon is not available
                                        String message=err.getMessage();
                                        if(!"Connection reset".equalsIgnoreCase(message)) {
                                            MasterServer.reportError(err, null);
                                        }
                                    } catch(IOException err) {
                                        DaemonHandler.flagDaemonAsDown(aoServer);
                                        MasterServer.reportError(err, null);
                                    } catch(SQLException err) {
                                        DaemonHandler.flagDaemonAsDown(aoServer);
                                        MasterServer.reportError(err, null);
                                    }
                                }
                            }
                        }
                        MasterServer.writeObjects(source, out, provideProgress, objs);
                    }
                    break;
                case DISABLE_LOG :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new DisableLog(),
                            "select * from disable_log"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new DisableLog(),
                            "select distinct\n"
                            + "  dl.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao\n"
                            + "  left join ao_servers ff on ao.server=ff.failover_server,\n"
                            + "  business_servers bs,\n"
                            + "  disable_log dl\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and (\n"
                            + "    ao.server=bs.server\n"
                            + "    or ff.server=bs.server\n"
                            + "  ) and bs.accounting=dl.accounting",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DisableLog(),
                        "select\n"
                        + "  dl.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN
                        + "  disable_log dl\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=dl.accounting",
                        username
                    );
                    break;
                case DISTRO_FILE_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DistroFileType(),
                        "select * from distro_file_types"
                    );
                    break;
                case DISTRO_FILES :
                    if(masterUser!=null && masterUser.isActive()) {
                        if(masterServers.length==0) {
                            if(provideProgress) throw new SQLException("Unable to provide progress when fetching rows for "+getTableName(conn, SchemaTable.TableID.DISTRO_FILES));
                            if(AOServProtocol.compareVersions(source.getProtocolVersion(), AOServProtocol.VERSION_1_0_A_107)<=0) {
                                List<DistroFile> emptyList = Collections.emptyList();
                                MasterServer.writeObjects(source, out, false, emptyList);
                            } else {
                                MasterServer.fetchObjects(
                                    conn,
                                    source,
                                    out, 
                                    new DistroFile(),
                                    "select * from distro_files"
                                );
                            }
                        } else {
                            // Restrict to the operating system versions accessible to this user
                            IntList osVersions=getOperatingSystemVersions(conn, source);
                            if(osVersions.size()==0) {
                                List<DistroFile> emptyList = Collections.emptyList();
                                MasterServer.writeObjects(source, out, provideProgress, emptyList);
                            } else {
                                if(provideProgress) throw new SQLException("Unable to provide progress when fetching rows for "+getTableName(conn, SchemaTable.TableID.DISTRO_FILES));
                                StringBuilder sql=new StringBuilder();
                                sql.append("select * from distro_files where operating_system_version in (");
                                for(int c=0;c<osVersions.size();c++) {
                                    if(c>0) sql.append(',');
                                    sql.append(osVersions.getInt(c));
                                }
                                sql.append(')');
                                MasterServer.fetchObjects(
                                    conn,
                                    source,
                                    out, 
                                    new DistroFile(),
                                    sql.toString()
                                );
                            }
                        }
                    } else {
                        List<DistroFile> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case DNS_FORBIDDEN_ZONES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DNSForbiddenZone(),
                        "select * from dns_forbidden_zones"
                    );
                    break;
                case DNS_RECORDS :
                    if(masterUser!=null) {
                        if(masterServers.length==0 || masterUser.isDNSAdmin()) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new DNSRecord(),
                            "select * from dns_records"
                        ); else {
                            List<DNSRecord> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DNSRecord(),
                        "select\n"
                        + "  dr.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  dns_zones dz,\n"
                        + "  dns_records dr\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=dz.package\n"
                        + "  and dz.zone=dr.zone",
                        username
                    );
                    break;
                case DNS_TLDS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DNSTLD(),
                        "select * from dns_tlds"
                    );
                    break;
                case DNS_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DNSType(),
                        "select * from dns_types"
                    );
                    break;
                case DNS_ZONES :
                    if(masterUser!=null) {
                        if(masterServers.length==0 || masterUser.isDNSAdmin()) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new DNSZone(),
                            "select * from dns_zones"
                        ); else {
                            List<DNSZone> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new DNSZone(),
                        "select\n"
                        + "  dz.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  dns_zones dz\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=dz.package",
                        username
                    );
                    break;
                case EMAIL_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailAddress(),
                            "select * from email_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailAddress(),
                            "select\n"
                            + "  ea.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailAddress(),
                        "select\n"
                        + "  ea.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain",
                        username
                    );
                    break;
                case EMAIL_ATTACHMENT_BLOCKS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailAttachmentBlock(),
                            "select * from email_attachment_blocks"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailAttachmentBlock(),
                            "select\n"
                            + "  eab.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  linux_server_accounts lsa,\n"
                            + "  email_attachment_blocks eab\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=lsa.ao_server\n"
                            + "  and lsa.pkey=eab.linux_server_account",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailAttachmentBlock(),
                        "select\n"
                        + "  eab.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  linux_server_accounts lsa,\n"
                        + "  email_attachment_blocks eab\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=lsa.username\n"
                        + "  and lsa.pkey=eab.linux_server_account",
                        username
                    );
                    break;
                case EMAIL_ATTACHMENT_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailAttachmentType(),
                        "select * from email_attachment_types"
                    );
                    break;
                case EMAIL_FORWARDING :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailForwarding(),
                            "select * from email_forwarding"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailForwarding(),
                            "select\n"
                            + "  ef.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea,\n"
                            + "  email_forwarding ef\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain\n"
                            + "  and ea.pkey=ef.email_address",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailForwarding(),
                        "select\n"
                        + "  ef.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea,\n"
                        + "  email_forwarding ef\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain\n"
                        + "  and ea.pkey=ef.email_address",
                        username
                    );
                    break;
                case EMAIL_LIST_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailListAddress(),
                            "select * from email_list_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailListAddress(),
                            "select\n"
                            + "  ela.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea,\n"
                            + "  email_list_addresses ela\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain\n"
                            + "  and ea.pkey=ela.email_address",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailListAddress(),
                        "select\n"
                        + "  ela.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea,\n"
                        + "  email_list_addresses ela\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain\n"
                        + "  and ea.pkey=ela.email_address",
                        username
                    );
                    break;
                case EMAIL_LISTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailList(),
                            "select * from email_lists"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailList(),
                            "select\n"
                            + "  el.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  linux_server_groups lsg,\n"
                            + "  email_lists el\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=lsg.ao_server\n"
                            + "  and lsg.pkey=el.linux_server_group",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailList(),
                        "select\n"
                        + "  el.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  linux_groups lg,\n"
                        + "  linux_server_groups lsg,\n"
                        + "  email_lists el\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=lg.package\n"
                        + "  and lg.name=lsg.name\n"
                        + "  and lsg.pkey=el.linux_server_group",
                        username
                    );
                    break;
                case EMAIL_PIPE_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailPipeAddress(),
                            "select * from email_pipe_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailPipeAddress(),
                            "select\n"
                            + "  epa.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea,\n"
                            + "  email_pipe_addresses epa\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain\n"
                            + "  and ea.pkey=epa.email_address",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailPipeAddress(),
                        "select\n"
                        + "  epa.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea,\n"
                        + "  email_pipe_addresses epa\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain\n"
                        + "  and ea.pkey=epa.email_address",
                        username
                    );
                    break;
                case EMAIL_PIPES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailPipe(),
                            "select * from email_pipes"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailPipe(),
                            "select\n"
                            + "  ep.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_pipes ep\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ep.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailPipe(),
                        "select\n"
                        + "  ep.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_pipes ep\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ep.package",
                        username
                    );
                    break;
                case EMAIL_SPAMASSASSIN_INTEGRATION_MODES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailSpamAssassinIntegrationMode(),
                        "select * from email_sa_integration_modes"
                    );
                    break;
                case ENCRYPTION_KEYS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new EncryptionKey(),
                                "select * from encryption_keys"
                            );
                        } else {
                            List<EncryptionKey> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EncryptionKey(),
                            "select\n"
                            + "  ek.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  encryption_keys ek\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=ek.accounting",
                            username
                        );
                    }
                    break;
                case EXPENSE_CATEGORIES :
                    if(BankAccountHandler.isBankAccounting(conn, source)) {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new ExpenseCategory(),
                            "select * from expense_categories"
                        );
                    } else {
                        List<ExpenseCategory> emptyList = Collections.emptyList();
                        MasterServer.writeObjects(source, out, provideProgress, emptyList);
                    }
                    break;
                case FAILOVER_FILE_LOG :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileLog(),
                            "select * from failover_file_log"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileLog(),
                            "select\n"
                            + "  ffl.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  failover_file_replications ffr,\n"
                            + "  failover_file_log ffl\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ffr.server\n"
                            + "  and ffr.pkey=ffl.replication",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FailoverFileLog(),
                        "select\n"
                        + "  ffl.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  failover_file_replications ffr,\n"
                        + "  failover_file_log ffl\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ffr.server\n"
                        + "  and ffr.pkey=ffl.replication",
                        username
                    );
                    break;
                case FAILOVER_FILE_REPLICATIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileReplication(),
                            "select * from failover_file_replications"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileReplication(),
                            "select\n"
                            + "  ffr.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  failover_file_replications ffr\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ffr.server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FailoverFileReplication(),
                        "select\n"
                        + "  ffr.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  failover_file_replications ffr\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ffr.server",
                        username
                    );
                    break;
                case FAILOVER_FILE_SCHEDULE :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileSchedule(),
                            "select * from failover_file_schedule"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverFileSchedule(),
                            "select\n"
                            + "  ffs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  failover_file_replications ffr,\n"
                            + "  failover_file_schedule ffs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ffr.server\n"
                            + "  and ffr.pkey=ffs.replication",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FailoverFileSchedule(),
                        "select\n"
                        + "  ffs.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  failover_file_replications ffr,\n"
                        + "  failover_file_schedule ffs\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ffr.server\n"
                        + "  and ffr.pkey=ffs.replication",
                        username
                    );
                    break;
                case FAILOVER_MYSQL_REPLICATIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverMySQLReplication(),
                            "select * from failover_mysql_replications"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FailoverMySQLReplication(),
                            "select\n"
                            + "  fmr.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  failover_file_replications ffr,\n"
                            + "  failover_mysql_replications fmr\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ffr.server\n"
                            + "  and ffr.pkey=fmr.replication",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FailoverMySQLReplication(),
                        "select\n"
                        + "  fmr.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  failover_file_replications ffr,\n"
                        + "  failover_mysql_replications fmr\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ffr.server\n"
                        + "  and ffr.pkey=fmr.replication",
                        username
                    );
                    break;
                case FILE_BACKUP_SETTINGS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FileBackupSetting(),
                            "select * from file_backup_settings"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FileBackupSetting(),
                            "select\n"
                            + "  fbs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  failover_file_replications ffr,\n"
                            + "  file_backup_settings fbs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ffr.server\n"
                            + "  and ffr.pkey=fbs.replication",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FileBackupSetting(),
                        "select\n"
                        + "  fbs.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  servers se,\n"
                        + "  failover_file_replications ffr,\n"
                        + "  file_backup_settings fbs\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.pkey=se.package\n"
                        + "  and se.pkey=ffr.server\n"
                        + "  and ffr.pkey=fbs.replication",
                        username
                    );
                    break;
                case FTP_GUEST_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FTPGuestUser(),
                            "select * from ftp_guest_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new FTPGuestUser(),
                            "select distinct\n"
                            + "  fgu.username\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  ftp_guest_users fgu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and ao.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=fgu.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new FTPGuestUser(),
                        "select\n"
                        + "  fgu.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  ftp_guest_users fgu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=fgu.username",
                        username
                    );
                    break;
                case HTTPD_BINDS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdBind(),
                            "select * from httpd_binds"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdBind(),
                            "select\n"
                            + "  hb.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  net_binds nb,\n"
                            + "  httpd_binds hb\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=nb.server\n"
                            + "  and nb.pkey=hb.net_bind",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdBind(),
                        "select\n"
                        + "  hb.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_site_binds hsb,\n"
                        + "  httpd_binds hb,\n"
                        + "  net_binds nb\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hsb.httpd_site\n"
                        + "  and hsb.httpd_bind=hb.net_bind\n"
                        + "  and hb.net_bind=nb.pkey\n"
                        + "group by\n"
                        + "  hb.net_bind,\n"
                        + "  hb.httpd_server,\n"
                        + "  nb.server,\n"
                        + "  nb.ip_address,\n"
                        + "  nb.port,\n"
                        + "  nb.net_protocol",
                        username
                    );
                    break;
                case HTTPD_JBOSS_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new HttpdJBossSite(),
                                "select * from httpd_jboss_sites"
                            );
                        } else {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new HttpdJBossSite(),
                                "select\n"
                                + "  hjs.*\n"
                                + "from\n"
                                + "  master_servers ms,\n"
                                + "  httpd_sites hs,\n"
                                + "  httpd_jboss_sites hjs\n"
                                + "where\n"
                                + "  ms.username=?\n"
                                + "  and ms.server=hs.ao_server\n"
                                + "  and hs.pkey=hjs.tomcat_site",
                                username
                            );
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdJBossSite(),
                        "select\n"
                        + "  hjs.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_jboss_sites hjs\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hjs.tomcat_site",
                        username
                    );
                    break;
                case HTTPD_JBOSS_VERSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdJBossVersion(),
                        "select * from httpd_jboss_versions"
                    );
                    break;
                case HTTPD_JK_CODES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdJKCode(),
                        "select * from httpd_jk_codes"
                    );
                    break;
                case HTTPD_JK_PROTOCOLS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdJKProtocol(),
                        "select * from httpd_jk_protocols"
                    );
                    break;
                case HTTPD_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdServer(),
                            "select * from httpd_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdServer(),
                            "select\n"
                            + "  hs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_servers hs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server",
                            username
                        );
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdServer(),
                            "select\n"
                            + "  hs.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk,\n"
                            + "  business_servers bs,\n"
                            + "  httpd_servers hs\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk.name\n"
                            + "  and pk.accounting=bs.accounting\n"
                            + "  and bs.server=hs.ao_server",
                            username
                        );
                    }
                    break;
                case HTTPD_SHARED_TOMCATS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSharedTomcat(),
                            "select * from httpd_shared_tomcats"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSharedTomcat(),
                            "select\n"
                            + "  hst.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_shared_tomcats hst\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hst.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdSharedTomcat(),
                        "select\n"
                        + "  hst.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  linux_groups lg,\n"
                        + "  linux_server_groups lsg,\n"
                        + "  httpd_shared_tomcats hst\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=lg.package\n"
                        + "  and lg.name=lsg.name\n"
                        + "  and lsg.pkey=hst.linux_server_group",
                        username
                    );
                    break;
                case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteAuthenticatedLocation(),
                            "select * from httpd_site_authenticated_locations"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteAuthenticatedLocation(),
                            "select\n"
                            + "  hsal.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_site_authenticated_locations hsal\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=hsal.httpd_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdSiteAuthenticatedLocation(),
                        "select\n"
                        + "  hsal.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_site_authenticated_locations hsal\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hsal.httpd_site",
                        username
                    );
                    break;
                case HTTPD_SITE_BINDS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteBind(),
                            "select * from httpd_site_binds"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteBind(),
                            "select\n"
                            + "  hsb.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_site_binds hsb\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=hsb.httpd_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdSiteBind(),
                        "select\n"
                        + "  hsb.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_site_binds hsb\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hsb.httpd_site",
                        username
                    );
                    break;
                case HTTPD_SITE_URLS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteURL(),
                            "select * from httpd_site_urls"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSiteURL(),
                            "select\n"
                            + "  hsu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_site_binds hsb,\n"
                            + "  httpd_site_urls hsu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=hsb.httpd_site\n"
                            + "  and hsb.pkey=hsu.httpd_site_bind",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdSiteURL(),
                        "select\n"
                        + "  hsu.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_site_binds hsb,\n"
                        + "  httpd_site_urls hsu\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hsb.httpd_site\n"
                        + "  and hsb.pkey=hsu.httpd_site_bind",
                        username
                    );
                    break;
                case HTTPD_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSite(),
                            "select * from httpd_sites"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdSite(),
                            "select\n"
                            + "  hs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdSite(),
                        "select\n"
                        + "  hs.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package",
                        username
                    );
                    break;
                case HTTPD_STATIC_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdStaticSite(),
                            "select * from httpd_static_sites"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdStaticSite(),
                            "select\n"
                            + "  hss.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_static_sites hss\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=hss.httpd_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdStaticSite(),
                        "select\n"
                        + "  hss.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_static_sites hss\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hss.httpd_site",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_CONTEXTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatContext(),
                            "select * from httpd_tomcat_contexts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatContext(),
                            "select\n"
                            + "  htc.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_contexts htc\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=htc.tomcat_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatContext(),
                        "select\n"
                        + "  htc.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_contexts htc\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=htc.tomcat_site",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_DATA_SOURCES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatDataSource(),
                            "select * from httpd_tomcat_data_sources"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatDataSource(),
                            "select\n"
                            + "  htds.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_contexts htc,\n"
                            + "  httpd_tomcat_data_sources htds\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=htc.tomcat_site\n"
                            + "  and htc.pkey=htds.tomcat_context",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatDataSource(),
                        "select\n"
                        + "  htds.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_contexts htc,\n"
                        + "  httpd_tomcat_data_sources htds\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=htc.tomcat_site\n"
                        + "  and htc.pkey=htds.tomcat_context",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_PARAMETERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatParameter(),
                            "select * from httpd_tomcat_parameters"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatParameter(),
                            "select\n"
                            + "  htp.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_contexts htc,\n"
                            + "  httpd_tomcat_parameters htp\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=htc.tomcat_site\n"
                            + "  and htc.pkey=htp.tomcat_context",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatParameter(),
                        "select\n"
                        + "  htp.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_contexts htc,\n"
                        + "  httpd_tomcat_parameters htp\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=htc.tomcat_site\n"
                        + "  and htc.pkey=htp.tomcat_context",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatSite(),
                            "select * from httpd_tomcat_sites"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatSite(),
                            "select\n"
                            + "  hts.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_sites hts\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=hts.httpd_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatSite(),
                        "select\n"
                        + "  hts.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_sites hts\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=hts.httpd_site",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_SHARED_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatSharedSite(),
                            "select * from httpd_tomcat_shared_sites"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatSharedSite(),
                            "select\n"
                            + "  htss.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_shared_sites htss\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=htss.tomcat_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatSharedSite(),
                        "select\n"
                        + "  htss.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_shared_sites htss\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=htss.tomcat_site",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_STD_SITES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatStdSite(),
                            "select * from httpd_tomcat_std_sites"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdTomcatStdSite(),
                            "select\n"
                            + "  htss.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  httpd_sites hs,\n"
                            + "  httpd_tomcat_std_sites htss\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=hs.ao_server\n"
                            + "  and hs.pkey=htss.tomcat_site",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatStdSite(),
                        "select\n"
                        + "  htss.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  httpd_sites hs,\n"
                        + "  httpd_tomcat_std_sites htss\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=hs.package\n"
                        + "  and hs.pkey=htss.tomcat_site",
                        username
                    );
                    break;
                case HTTPD_TOMCAT_VERSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdTomcatVersion(),
                        "select * from httpd_tomcat_versions"
                    );
                    break;
                case HTTPD_WORKERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdWorker(),
                            "select * from httpd_workers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new HttpdWorker(),
                            "select\n"
                            + "  hw.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  net_binds nb,\n"
                            + "  httpd_workers hw\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=nb.server\n"
                            + "  and nb.pkey=hw.net_bind",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new HttpdWorker(),
                        "select\n"
                        + "  hw.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  net_binds nb,\n"
                        + "  httpd_workers hw\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=nb.package\n"
                        + "  and nb.pkey=hw.net_bind",
                        username
                    );
                    break;
                case IP_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new IPAddress(),
                            "select * from ip_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new IPAddress(),
                            "select distinct\n"
                            + "  ia.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
                            + "  net_devices nd\n"
                            + "  right outer join ip_addresses ia on nd.pkey=ia.net_device\n"
                            + "where\n"
                            + "  ia.ip_address='"+IPAddress.WILDCARD_IP+"' or (\n"
                            + "    ms.username=?\n"
                            + "    and (\n"
                            + "      ms.server=nd.server\n"
                            + "      or ff.server=nd.server\n"
                            + "      or (\n"
                            + "        select\n"
                            + "          ffr.pkey\n"
                            + "        from\n"
                            + "          failover_file_replications ffr\n"
                            + "          inner join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
                            + "          inner join ao_servers bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
                            + "        where\n"
                            + "          ms.server=ffr.server\n"
                            + "          and bp.ao_server=nd.server\n"
                            + "          and bpao.daemon_device_id=nd.device_id\n" // Only allow access to the device device ID for failovers
                            + "        limit 1\n"
                            + "      ) is not null\n"
                            + "    )\n"
                            + "  )",
                            username
                        );
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new IPAddress(),
                            "select\n"
                            + "  ia.*\n"
                            + "from\n"
                            + "  ip_addresses ia\n"
                            + "where\n"
                            + "  ia.ip_address='"+IPAddress.WILDCARD_IP+"'\n"
                            + "  or ia.pkey in (\n"
                            + "    select\n"
                            + "      ia2.pkey\n"
                            + "    from\n"
                            + "      usernames un1,\n"
                            + "      packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "      packages pk2,\n"
                            + "      ip_addresses ia2\n"
                            + "    where\n"
                            + "      un1.username=?\n"
                            + "      and un1.package=pk1.name\n"
                            + "      and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "      )\n"
                            + "      and bu1.accounting=pk2.accounting\n"
                            + "      and pk2.name=ia2.package\n"
                            + "  )\n"
                            + "  or ia.pkey in (\n"
                            + "    select\n"
                            + "      nb.ip_address\n"
                            + "    from\n"
                            + "      usernames un3,\n"
                            + "      packages pk3,\n"
                            + BU2_PARENTS_JOIN
                            + "      packages pk4,\n"
                            + "      httpd_sites hs,\n"
                            + "      httpd_site_binds hsb,\n"
                            + "      net_binds nb\n"
                            + "    where\n"
                            + "      un3.username=?\n"
                            + "      and un3.package=pk3.name\n"
                            + "      and (\n"
                            + PK3_BU2_PARENTS_WHERE
                            + "      )\n"
                            + "      and bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
                            + "      and pk4.name=hs.package\n"
                            + "      and hs.pkey=hsb.httpd_site\n"
                            + "      and hsb.httpd_bind=nb.pkey\n"
                            + "  ) or ia.pkey in (\n"
                            + "    select\n"
                            + "      ia5.pkey\n"
                            + "    from\n"
                            + "      usernames un5,\n"
                            + "      packages pk5,\n"
                            + "      business_servers bs5,\n"
                            + "      net_devices nd5,\n"
                            + "      ip_addresses ia5\n"
                            + "    where\n"
                            + "      un5.username=?\n"
                            + "      and un5.package=pk5.name\n"
                            + "      and pk5.accounting=bs5.accounting\n"
                            + "      and bs5.server=nd5.server\n"
                            + "      and nd5.pkey=ia5.net_device\n"
                            + "      and (ia5.ip_address='"+IPAddress.LOOPBACK_IP+"' or ia5.is_overflow)\n"
                            /*+ "  ) or ia.pkey in (\n"
                            + "    select \n"
                            + "      ia6.pkey\n"
                            + "    from\n"
                            + "      usernames un6,\n"
                            + "      packages pk6,\n"
                            + "      business_servers bs6,\n"
                            + "      failover_file_replications ffr6,\n"
                            + "      backup_partitions bp6,\n"
                            + "      ao_servers ao6,\n"
                            + "      net_devices nd6,\n"
                            + "      ip_addresses ia6\n"
                            + "    where\n"
                            + "      un6.username=?\n"
                            + "      and un6.package=pk6.name\n"
                            + "      and pk6.accounting=bs6.accounting\n"
                            + "      and bs6.server=ffr6.server\n"
                            + "      and ffr6.backup_partition=bp6.pkey\n"
                            + "      and bp6.ao_server=ao6.server\n"
                            + "      and ao6.server=nd6.ao_server and ao6.daemon_device_id=nd6.device_id\n"
                            + "      and nd6.pkey=ia6.net_device and not ia6.is_alias\n"*/
                            + "  )",
                            username,
                            username,
                            username//,
                            //username
                        );
                    }
                    break;
                case LINUX_ACC_ADDRESSES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxAccAddress(),
                            "select * from linux_acc_addresses"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxAccAddress(),
                            "select\n"
                            + "  laa.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  email_addresses ea,\n"
                            + "  linux_acc_addresses laa\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ea.domain\n"
                            + "  and ea.pkey=laa.email_address",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxAccAddress(),
                        "select\n"
                        + "  laa.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  email_addresses ea,\n"
                        + "  linux_acc_addresses laa\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ea.domain\n"
                        + "  and ea.pkey=laa.email_address",
                        username
                    );
                    break;
                case LINUX_ACCOUNTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxAccount(),
                            "select * from linux_accounts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxAccount(),
                            "select distinct\n"
                            + "  la.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao\n"
                            + "  left join ao_servers ff on ao.server=ff.failover_server,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  linux_accounts la\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and (\n"
                            + "    ao.server=bs.server\n"
                            + "    or ff.server=bs.server\n"
                            + "  ) and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=la.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxAccount(),
                        "select\n"
                        + "  la.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  linux_accounts la\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username='"+LinuxAccount.MAIL+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=la.username",
                        username
                    );
                    break;
                case LINUX_ACCOUNT_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxAccountType(),
                        "select * from linux_account_types"
                    );
                    break;
                case LINUX_GROUP_ACCOUNTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxGroupAccount(),
                            "select * from linux_group_accounts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxGroupAccount(),
                            "select\n"
                            + "  *\n"
                            + "from\n"
                            + "  linux_group_accounts\n"
                            + "where\n"
                            + "  group_name in (\n"
                            + "    select\n"
                            + "      lsg.name\n"
                            + "      from\n"
                            + "        master_servers ms1,\n"
                            + "        linux_server_groups lsg\n"
                            + "      where\n"
                            + "        ms1.username=?\n"
                            + "        and ms1.server=lsg.ao_server\n"
                            + "  )\n"
                            + "  and username in (\n"
                            + "    select\n"
                            + "      lsa.username\n"
                            + "      from\n"
                            + "        master_servers ms2,\n"
                            + "        linux_server_accounts lsa\n"
                            + "      where\n"
                            + "        ms2.username=?\n"
                            + "        and ms2.server=lsa.ao_server\n"
                            + "  )",
                            username,
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxGroupAccount(),
                        "select\n"
                        + " *\n"
                        + "from\n"
                        + "  linux_group_accounts\n"
                        + "where\n"
                        + "  group_name in (\n"
                        + "    select\n"
                        + "      lg.name\n"
                        + "    from\n"
                        + "      usernames un1,\n"
                        + "      packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "      packages pk2,\n"
                        + "      linux_groups lg\n"
                        + "    where\n"
                        + "      un1.username=?\n"
                        + "      and un1.package=pk1.name\n"
                        + "      and (\n"
                        + "        lg.name='"+LinuxGroup.FTPONLY+"'\n"
                        + "        or lg.name='"+LinuxGroup.MAIL+"'\n"
                        + "        or lg.name='"+LinuxGroup.MAILONLY+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "      )\n"
                        + "      and bu1.accounting=pk2.accounting\n"
                        + "      and pk2.name=lg.package\n"
                        + "  )\n"
                        + "  and username in (\n"
                        + "    select\n"
                        + "      la.username\n"
                        + "    from\n"
                        + "      usernames un2,\n"
                        + "      packages pk3,\n"
                        + BU2_PARENTS_JOIN
                        + "      packages pk4,\n"
                        + "      usernames un3,\n"
                        + "      linux_accounts la\n"
                        + "    where\n"
                        + "      un2.username=?\n"
                        + "      and un2.package=pk3.name\n"
                        + "      and (\n"
                        + "        un3.username='"+LinuxAccount.MAIL+"'\n"
                        + PK3_BU2_PARENTS_OR_WHERE
                        + "      )\n"
                        + "      and bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
                        + "      and pk4.name=un3.package\n"
                        + "      and un3.username=la.username\n"
                        + "  )",
                        username,
                        username
                    );
                    break;
                case LINUX_GROUPS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxGroup(),
                            "select * from linux_groups"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxGroup(),
                            "select distinct\n"
                            + "  lg.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  linux_groups lg\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and ao.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=lg.package",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxGroup(),
                        "select\n"
                        + "  lg.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  linux_groups lg\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + "    lg.name='"+LinuxGroup.FTPONLY+"'\n"
                        + "    or lg.name='"+LinuxGroup.MAIL+"'\n"
                        + "    or lg.name='"+LinuxGroup.MAILONLY+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=lg.package",
                        username
                    );
                    break;
                case LINUX_GROUP_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxGroupType(),
                        "select * from linux_group_types"
                    );
                    break;
                case LINUX_IDS :
                    throw new IOException("LinuxIDs should be generated, not obtained from the MasterServer");
                case LINUX_SERVER_ACCOUNTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxServerAccount(),
                            "select * from linux_server_accounts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxServerAccount(),
                            "select distinct\n"
                            + "  lsa.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
                            + "  linux_server_accounts lsa\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            + "    ms.server=lsa.ao_server\n"
                            + "    or ff.server=lsa.ao_server\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxServerAccount(),
                        "select\n"
                        + "  lsa.pkey,\n"
                        + "  lsa.username,\n"
                        + "  lsa.ao_server,\n"
                        + "  lsa.uid,\n"
                        + "  lsa.home,\n"
                        + "  lsa.autoresponder_from,\n"
                        + "  lsa.autoresponder_subject,\n"
                        + "  lsa.autoresponder_path,\n"
                        + "  lsa.is_autoresponder_enabled,\n"
                        + "  lsa.disable_log,\n"
                        + "  case when lsa.predisable_password is null then null else '"+AOServProtocol.FILTERED+"' end,\n"
                        + "  lsa.created,\n"
                        + "  lsa.use_inbox,\n"
                        + "  lsa.trash_email_retention,\n"
                        + "  lsa.junk_email_retention,\n"
                        + "  lsa.sa_integration_mode,\n"
                        + "  lsa.sa_required_score\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  business_servers bs,\n"
                        + "  linux_server_accounts lsa\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username='"+LinuxAccount.MAIL+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and pk1.accounting=bs.accounting\n"
                        + "  and un2.username=lsa.username\n"
                        + "  and bs.server=lsa.ao_server",
                        username
                    );
                    break;
                case LINUX_SERVER_GROUPS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxServerGroup(),
                            "select * from linux_server_groups"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new LinuxServerGroup(),
                            "select\n"
                            + "  lsg.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  linux_server_groups lsg\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=lsg.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new LinuxServerGroup(),
                        "select\n"
                        + "  lsg.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  linux_groups lg,\n"
                        + "  business_servers bs,\n"
                        + "  linux_server_groups lsg\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + "    lg.name='"+LinuxGroup.FTPONLY+"'\n"
                        + "    or lg.name='"+LinuxGroup.MAIL+"'\n"
                        + "    or lg.name='"+LinuxGroup.MAILONLY+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=lg.package\n"
                        + "  and pk1.accounting=bs.accounting\n"
                        + "  and lg.name=lsg.name\n"
                        + "  and bs.server=lsg.ao_server",
                        username
                    );
                    break;
                case MAJORDOMO_LISTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MajordomoList(),
                            "select * from majordomo_lists"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MajordomoList(),
                            "select\n"
                            + "  ml.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  majordomo_lists ml\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=ml.majordomo_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MajordomoList(),
                        "select\n"
                        + "  ml.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  majordomo_lists ml\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ml.majordomo_server",
                        username
                    );
                    break;
                case MAJORDOMO_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MajordomoServer(),
                            "select * from majordomo_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MajordomoServer(),
                            "select\n"
                            + "  mjs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed,\n"
                            + "  majordomo_servers mjs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server\n"
                            + "  and ed.pkey=mjs.domain",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MajordomoServer(),
                        "select\n"
                        + "  ms.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed,\n"
                        + "  majordomo_servers ms\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package\n"
                        + "  and ed.pkey=ms.domain",
                        username
                    );
                    break;
                case MAJORDOMO_VERSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MajordomoVersion(),
                        "select * from majordomo_versions"
                    );
                    break;
                case MASTER_HISTORY :
                    MasterServer.writeHistory(
                        conn,
                        source,
                        out,
                        provideProgress,
                        masterUser,
                        masterServers
                    );
                    break;
                case MASTER_HOSTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MasterHost(),
                            "select * from master_hosts"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MasterHost(),
                            "select distinct\n"
                            + "  mh.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  master_hosts mh\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=mh.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MasterHost(),
                        "select\n"
                        + "  mh.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  master_hosts mh\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=mh.username",
                        username
                    );
                    break;
                case MASTER_PROCESSES :
                    MasterProcessManager.writeProcesses(
                        conn,
                        out,
                        provideProgress,
                        source,
                        masterUser,
                        masterServers
                    );
                    break;
                case MASTER_SERVER_PROFILE :
                    {
                        List<MasterServerProfile> objs=new ArrayList<MasterServerProfile>();
                        if(Profiler.getProfilerLevel()>Profiler.NONE) {
                            // Get all the methods
                            List<MethodProfile> profs=Profiler.getMethodProfiles();
                            int len=profs.size();
                            //objs.ensureCapacity(len);
                            for(int c=0;c<len;c++) {
                                MethodProfile mp=profs.get(c);
                                Object param1=mp.getParameter1();
                                objs.add(
                                    new MasterServerProfile(
                                        mp.getLevel(),
                                        mp.getProfiledClassName(),
                                        mp.getMethodName(),
                                        param1==null?null:param1.toString(),
                                        mp.getUseCount(),
                                        mp.getTotalTime(),
                                        mp.getMinTime(),
                                        mp.getMaxTime()
                                    )
                                );
                            }
                        }
                        MasterServer.writeObjects(source, out, provideProgress, objs);
                    }
                    break;
                case MASTER_SERVER_STATS :
                    MasterServer.writeStats(
                        source,
                        out,
                        provideProgress
                    );
                    break;
                case MASTER_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new com.aoindustries.aoserv.client.MasterServer(),
                            "select * from master_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new com.aoindustries.aoserv.client.MasterServer(),
                            "select\n"
                            + "  ms2.*\n"
                            + "from\n"
                            + "  master_servers ms1,\n"
                            + "  master_servers ms2\n"
                            + "where\n"
                            + "  ms1.username=?\n"
                            + "  and ms1.server=ms2.server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new com.aoindustries.aoserv.client.MasterServer(),
                        "select\n"
                        + "  ms.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  master_servers ms\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=ms.username",
                        username
                    );
                    break;
                case MASTER_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MasterUser(),
                            "select * from master_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MasterUser(),
                            "select distinct\n"
                            + "  mu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  master_users mu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=mu.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MasterUser(),
                        "select\n"
                        + "  mu.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  master_users mu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=mu.username",
                        username
                    );
                    break;
                case MONTHLY_CHARGES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new MonthlyCharge(),
                                "select * from monthly_charges"
                            );
                        } else {
                            List<MonthlyCharge> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else {
                        if(BusinessHandler.canSeePrices(conn, source)) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new MonthlyCharge(),
                                "select\n"
                                + "  mc.*\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk1,\n"
                                + BU1_PARENTS_JOIN
                                + "  packages pk2,\n"
                                + "  monthly_charges mc\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk1.name\n"
                                + "  and (\n"
                                + PK1_BU1_PARENTS_WHERE
                                + "  )\n"
                                + "  and bu1.accounting=pk2.accounting\n"
                                + "  and pk2.name=mc.package",
                                username
                            );
                        } else {
                            List<MonthlyCharge> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    }
                    break;
                case MYSQL_DATABASES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLDatabase(),
                            "select * from mysql_databases"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLDatabase(),
                            "select\n"
                            + "  md.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  mysql_servers mys,\n"
                            + "  mysql_databases md\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=mys.ao_server\n"
                            + "  and mys.pkey=md.mysql_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLDatabase(),
                        "select\n"
                        + "  md.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  mysql_databases md\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=md.package",
                        username
                    );
                    break;
                case MYSQL_DB_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLDBUser(),
                            "select * from mysql_db_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLDBUser(),
                            "select\n"
                            + "  mdu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  mysql_servers mys,\n"
                            + "  mysql_databases md,\n"
                            + "  mysql_db_users mdu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=mys.ao_server\n"
                            + "  and mys.pkey=md.mysql_server\n"
                            + "  and md.pkey=mdu.mysql_database",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLDBUser(),
                        "select\n"
                        + "  mdu.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  mysql_databases md,\n"
                        + "  mysql_db_users mdu\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=md.package\n"
                        + "  and md.pkey=mdu.mysql_database",
                        username
                    );
                    break;
                case MYSQL_RESERVED_WORDS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLReservedWord(),
                        "select * from mysql_reserved_words"
                    );
                    break;
                case MYSQL_SERVER_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLServerUser(),
                            "select * from mysql_server_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLServerUser(),
                            "select\n"
                            + "  msu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  mysql_servers mys,\n"
                            + "  mysql_server_users msu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=mys.ao_server\n"
                            + "  and mys.pkey=msu.mysql_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLServerUser(),
                         "select\n"
                        + "  msu.pkey,\n"
                        + "  msu.username,\n"
                        + "  msu.mysql_server,\n"
                        + "  msu.host,\n"
                        + "  msu.disable_log,\n"
                        + "  case when msu.predisable_password is null then null else '"+AOServProtocol.FILTERED+"' end,\n"
                        + "  msu.max_questions,\n"
                        + "  msu.max_updates\n,"
                        + "  msu.max_connections,\n"
                        + "  msu.max_user_connections\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  mysql_server_users msu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=msu.username",
                        username
                    );
                    break;
                case MYSQL_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLServer(),
                            "select * from mysql_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLServer(),
                            "select\n"
                            + "  ps.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  mysql_servers ps\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ps.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLServer(),
                        "select\n"
                        + "  ps.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  mysql_servers ps\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ps.ao_server",
                        username
                    );
                    break;
                case MYSQL_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLUser(),
                            "select * from mysql_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new MySQLUser(),
                            "select distinct\n"
                            + "  mu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  mysql_users mu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and ao.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=mu.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new MySQLUser(),
                        "select\n"
                        + "  mu.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  mysql_users mu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=mu.username",
                        username
                    );
                    break;
                case NET_BINDS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetBind(),
                            "select * from net_binds"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetBind(),
                            "select\n"
                            + "  nb.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  servers se,\n"
                            + "  net_binds nb\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=se.pkey\n"
                            + "  and (\n"
                            + "    ms.server=nb.server\n"
                            + "    or (\n"
                            + "      select\n"
                            + "        ffr.pkey\n"
                            + "      from\n"
                            + "        failover_file_replications ffr\n"
                            + "        inner join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
                            + "      where\n"
                            + "        ms.server=ffr.server\n"
                            + "        and bp.ao_server=nb.server\n"
                            + "        and (\n"
                            + "          nb.app_protocol='"+Protocol.AOSERV_DAEMON+"'\n"
                            + "          or nb.app_protocol='"+Protocol.AOSERV_DAEMON_SSL+"'\n"
                            + "        )\n"
                            + "      limit 1\n"
                            + "    ) is not null\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NetBind(),
                        "select\n"
                        + "  nb.pkey,\n"
                        + "  nb.package,\n"
                        + "  nb.server,\n"
                        + "  nb.ip_address,\n"
                        + "  nb.port,\n"
                        + "  nb.net_protocol,\n"
                        + "  nb.app_protocol,\n"
                        + "  nb.open_firewall,\n"
                        + "  nb.monitoring_enabled\n"
                        + "from\n"
                        + "  net_binds nb\n"
                        + "where\n"
                        + "  nb.pkey in (\n"
                        + "    select\n"
                        + "      nb2.pkey\n"
                        + "    from\n"
                        + "      usernames un1,\n"
                        + "      packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "      packages pk2,\n"
                        + "      net_binds nb2\n"
                        + "    where\n"
                        + "      un1.username=?\n"
                        + "      and un1.package=pk1.name\n"
                        + "      and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "      )\n"
                        + "      and bu1.accounting=pk2.accounting\n"
                        + "      and pk2.name=nb2.package\n"
                        + "  )\n"
                        + "  or nb.pkey in (\n"
                        + "    select\n"
                        + "      nb3.pkey\n"
                        + "    from\n"
                        + "      usernames un3,\n"
                        + "      packages pk3,\n"
                        + BU2_PARENTS_JOIN
                        + "      packages pk4,\n"
                        + "      httpd_sites hs,\n"
                        + "      httpd_site_binds hsb,\n"
                        + "      net_binds nb3\n"
                        + "    where\n"
                        + "      un3.username=?\n"
                        + "      and un3.package=pk3.name\n"
                        + "      and (\n"
                        + PK3_BU2_PARENTS_WHERE
                        + "      )\n"
                        + "      and bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
                        + "      and pk4.name=hs.package\n"
                        + "      and hs.pkey=hsb.httpd_site\n"
                        + "      and hsb.httpd_bind=nb3.pkey\n"
                        + "  ) or nb.pkey in (\n"
			+ "    select\n"
			+ "      ms4.net_bind\n"
			+ "    from\n"
                        + "      usernames un4,\n"
                        + "      packages pk4,\n"
                        + "      business_servers bs4,\n"
                        + "      mysql_servers ms4\n"
                        + "    where\n"
                        + "      un4.username=?\n"
                        + "      and un4.package=pk4.name\n"
                        + "      and pk4.accounting=bs4.accounting\n"
                        + "      and bs4.server=ms4.ao_server\n"
                        + "  ) or nb.pkey in (\n"
			+ "    select\n"
			+ "      ps5.net_bind\n"
			+ "    from\n"
                        + "      usernames un5,\n"
                        + "      packages pk5,\n"
                        + "      business_servers bs5,\n"
                        + "      postgres_servers ps5\n"
                        + "    where\n"
                        + "      un5.username=?\n"
                        + "      and un5.package=pk5.name\n"
                        + "      and pk5.accounting=bs5.accounting\n"
                        + "      and bs5.server=ps5.ao_server\n"
			/*+ "  ) or nb.pkey in (\n"
                        // Allow net_binds of receiving failover_file_replications (exact package match - no tree inheritence)
                        + "    select\n"
                        + "      nb6.pkey\n"
                        + "    from\n"
                        + "      usernames un6,\n"
                        + "      packages pk6,\n"
                        + "      servers se6,\n"
                        + "      failover_file_replications ffr6,\n"
                        + "      backup_partitions bp6,\n"
                        + "      net_binds nb6\n"
                        + "    where\n"
                        + "      un6.username=?\n"
                        + "      and un6.package=pk6.name\n"
                        + "      and pk6.pkey=se6.package\n"
                        + "      and se6.pkey=ffr6.server\n"
                        + "      and ffr6.backup_partition=bp6.pkey\n"
                        + "      and bp6.ao_server=nb6.ao_server\n"
                        + "      and (\n"
                        + "        nb6.app_protocol='"+Protocol.AOSERV_DAEMON+"'\n"
                        + "        or nb6.app_protocol='"+Protocol.AOSERV_DAEMON_SSL+"'\n"
                        + "      )\n"*/
                        + "  )",
                        username,
                        username,
                        username,
			username//,
			//username
                    );
                    break;
                case NET_DEVICE_IDS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NetDeviceID(),
                        "select * from net_device_ids"
                    );
                    break;
                case NET_DEVICES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetDevice(),
                            "select * from net_devices"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetDevice(),
                            "select distinct\n"
                            + "  nd.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
                            + "  net_devices nd\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            + "    ms.server=nd.server\n"
                            + "    or ff.server=nd.server\n"
                            + "    or (\n"
                            + "      select\n"
                            + "        ffr.pkey\n"
                            + "      from\n"
                            + "        failover_file_replications ffr\n"
                            + "        inner join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
                            + "        inner join ao_servers bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
                            + "      where\n"
                            + "        ms.server=ffr.server\n"
                            + "        and bp.ao_server=nd.server\n"
                            + "        and bpao.daemon_device_id=nd.device_id\n" // Only allow access to the device device ID for failovers
                            + "      limit 1\n"
                            + "    ) is not null\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NetDevice(),
                        "select distinct\n"
                        + "  nd.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        // Allow failover destinations
                        //+ "  left outer join failover_file_replications ffr on bs.server=ffr.server\n"
                        //+ "  left outer join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
                        //+ "  left outer join ao_servers bpao on bp.ao_server=bpao.server,\n"
                        + "  net_devices nd\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and (\n"
                        + "    bs.server=nd.server\n"
                        //+ "    or (bp.ao_server=nd.ao_server and nd.device_id=bpao.daemon_device_id)\n"
                        + "  )",
                        username
                    );
                    break;
                case NET_PORTS :
                    throw new IOException("NetPorts should be generated, not obtained from the MasterDatabase.getDatabase().");
                case NET_PROTOCOLS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NetProtocol(),
                        "select * from net_protocols"
                    );
                    break;
                case NET_TCP_REDIRECTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetTcpRedirect(),
                            "select * from net_tcp_redirects"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NetTcpRedirect(),
                            "select\n"
                            + "  ntr.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  net_binds nb,\n"
                            + "  net_tcp_redirects ntr\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=nb.server\n"
                            + "  and nb.pkey=ntr.net_bind",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NetTcpRedirect(),
                        "select\n"
                        + "  ntr.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  net_binds nb,\n"
                        + "  net_tcp_redirects ntr\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=nb.package\n"
                        + "  and nb.pkey=ntr.net_bind",
                        username
                    );
                    break;
                case NOTICE_LOG :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new NoticeLog(),
                            "select * from notice_log"
                        ); else {
                            List<NoticeLog> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NoticeLog(),
                        "select\n"
                        + "  nl.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN
                        + "  notice_log nl\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=nl.accounting",
                        username
                    );
                    break;
                case NOTICE_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new NoticeType(),
                        "select * from notice_types"
                    );
                    break;
                case OPERATING_SYSTEM_VERSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new OperatingSystemVersion(),
                        "select * from operating_system_versions"
                    );
                    break;
                case OPERATING_SYSTEMS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new OperatingSystem(),
                        "select * from operating_systems"
                    );
                    break;
                case PACKAGE_CATEGORIES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PackageCategory(),
                        "select * from package_categories"
                    );
                    break;
                case PACKAGE_DEFINITION_LIMITS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PackageDefinitionLimit(),
                            "select * from package_definition_limits"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PackageDefinitionLimit(),
                            "select distinct\n"
                            + "  pdl.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  package_definition_limits pdl\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.package_definition=pdl.package_definition",
                            username
                        );
                    } else {
                        if(BusinessHandler.canSeePrices(conn, source)) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new PackageDefinitionLimit(),
                                "select distinct\n"
                                + "  pdl.*\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk1,\n"
                                + BU1_PARENTS_JOIN
                                + "  packages pk2,\n"
                                + "  package_definitions pd,\n"
                                + "  package_definition_limits pdl\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk1.name\n"
                                + "  and (\n"
                                + PK1_BU1_PARENTS_WHERE
                                + "  )\n"
                                + "  and bu1.accounting=pk2.accounting\n"
                                + "  and (\n"
                                + "    pk2.package_definition=pd.pkey\n"
                                + "    or bu1.accounting=pd.accounting\n"
                                + "  ) and pd.pkey=pdl.package_definition",
                                username
                            );
                        } else {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new PackageDefinitionLimit(),
                                "select distinct\n"
                                + "  pdl.pkey,\n"
                                + "  pdl.package_definition,\n"
                                + "  pdl.resource,\n"
                                + "  pdl.soft_limit,\n"
                                + "  pdl.hard_limit,\n"
                                + "  null,\n"
                                + "  null\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk1,\n"
                                + BU1_PARENTS_JOIN
                                + "  packages pk2,\n"
                                + "  package_definitions pd,\n"
                                + "  package_definition_limits pdl\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk1.name\n"
                                + "  and (\n"
                                + PK1_BU1_PARENTS_WHERE
                                + "  )\n"
                                + "  and bu1.accounting=pk2.accounting\n"
                                + "  and (\n"
                                + "    pk2.package_definition=pd.pkey\n"
                                + "    or bu1.accounting=pd.accounting\n"
                                + "  ) and pd.pkey=pdl.package_definition",
                                username
                            );
                        }
                    }
                    break;
                case PACKAGE_DEFINITIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PackageDefinition(),
                            "select * from package_definitions"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PackageDefinition(),
                            "select distinct\n"
                            + "  pd.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  package_definitions pd\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.package_definition=pd.pkey",
                            username
                        );
                    } else {
                        if(BusinessHandler.canSeePrices(conn, source)) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new PackageDefinition(),
                                "select distinct\n"
                                + "  pd.*\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk1,\n"
                                + BU1_PARENTS_JOIN
                                + "  packages pk2,\n"
                                + "  package_definitions pd\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk1.name\n"
                                + "  and (\n"
                                + PK1_BU1_PARENTS_WHERE
                                + "  )\n"
                                + "  and bu1.accounting=pk2.accounting\n"
                                + "  and (\n"
                                + "    pk2.package_definition=pd.pkey\n"
                                + "    or bu1.accounting=pd.accounting\n"
                                + "  )",
                                username
                            );
                        } else {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new PackageDefinition(),
                                "select distinct\n"
                                + "  pd.pkey,\n"
                                + "  pd.accounting,\n"
                                + "  pd.category,\n"
                                + "  pd.name,\n"
                                + "  pd.version,\n"
                                + "  pd.display,\n"
                                + "  pd.description,\n"
                                + "  null,\n"
                                + "  null,\n"
                                + "  null,\n"
                                + "  null,\n"
                                + "  pd.active\n"
                                + "from\n"
                                + "  usernames un,\n"
                                + "  packages pk1,\n"
                                + BU1_PARENTS_JOIN
                                + "  packages pk2,\n"
                                + "  package_definitions pd\n"
                                + "where\n"
                                + "  un.username=?\n"
                                + "  and un.package=pk1.name\n"
                                + "  and (\n"
                                + PK1_BU1_PARENTS_WHERE
                                + "  )\n"
                                + "  and bu1.accounting=pk2.accounting\n"
                                + "  and (\n"
                                + "    pk2.package_definition=pd.pkey\n"
                                + "    or bu1.accounting=pd.accounting\n"
                                + "  )",
                                username
                            );
                        }
                    }
                    break;
                case PACKAGES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Package(),
                            "select * from packages"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Package(),
                            "select distinct\n"
                            + "  pk.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting",
                            username
                        );
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Package(),
                            "select\n"
                            + "  pk2.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  packages pk2\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=pk2.accounting",
                            username
                        );
                    }
                    break;
                case PAYMENT_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PaymentType(),
                        "select * from payment_types"
                    );
                    break;
                case POSTGRES_DATABASES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresDatabase(),
                            "select * from postgres_databases"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresDatabase(),
                            "select\n"
                            + "  pd.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  postgres_servers ps,\n"
                            + "  postgres_databases pd\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ps.ao_server\n"
                            + "  and ps.pkey=pd.postgres_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresDatabase(),
                        "select\n"
                        + "  pd.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  postgres_server_users psu,\n"
                        + "  postgres_databases pd\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=psu.username\n"
                        + "  and psu.pkey=pd.datdba",
                        username
                    );
                    break;
                case POSTGRES_ENCODINGS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresEncoding(),
                        "select * from postgres_encodings"
                    );
                    break;
                case POSTGRES_RESERVED_WORDS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresReservedWord(),
                        "select * from postgres_reserved_words"
                    );
                    break;
                case POSTGRES_SERVER_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresServerUser(),
                            "select * from postgres_server_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresServerUser(),
                            "select\n"
                            + "  psu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  postgres_servers ps,\n"
                            + "  postgres_server_users psu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ps.ao_server\n"
                            + "  and ps.pkey=psu.postgres_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresServerUser(),
                        "select\n"
                        + "  psu.pkey,\n"
                        + "  psu.username,\n"
                        + "  psu.postgres_server,\n"
                        + "  psu.disable_log,\n"
                        + "  case when psu.predisable_password is null then null else '"+AOServProtocol.FILTERED+"' end\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  postgres_server_users psu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=psu.username",
                        username
                    );
                    break;
                case POSTGRES_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresServer(),
                            "select * from postgres_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresServer(),
                            "select\n"
                            + "  ps.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  postgres_servers ps\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ps.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresServer(),
                        "select\n"
                        + "  ps.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  postgres_servers ps\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=ps.ao_server",
                        username
                    );
                    break;
                case POSTGRES_USERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresUser(),
                            "select * from postgres_users"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PostgresUser(),
                            "select distinct\n"
                            + "  pu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un,\n"
                            + "  postgres_users pu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and ao.server=bs.server\n"
                            + "  and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package\n"
                            + "  and un.username=pu.username",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresUser(),
                        "select\n"
                        + "  pu.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2,\n"
                        + "  postgres_users pu\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package\n"
                        + "  and un2.username=pu.username",
                        username
                    );
                    break;
                case POSTGRES_VERSIONS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new PostgresVersion(),
                        "select * from postgres_versions"
                    );
                    break;
                case PRIVATE_FTP_SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PrivateFTPServer(),
                            "select * from private_ftp_servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PrivateFTPServer(),
                            "select\n"
                            + "  pfs.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  net_binds nb,\n"
                            + "  private_ftp_servers pfs\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=nb.server\n"
                            + "  and nb.pkey=pfs.net_bind",
                            username
                        );
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new PrivateFTPServer(),
                            "select\n"
                            + "  pfs.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  packages pk2,\n"
                            + "  net_binds nb,\n"
                            + "  private_ftp_servers pfs\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=pk2.accounting\n"
                            + "  and pk2.name=nb.package\n"
                            + "  and nb.pkey=pfs.net_bind",
                            username
                        );
                    }
                    break;
                case PROTOCOLS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Protocol(),
                        "select * from protocols"
                    );
                    break;
                case RESOURCES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Resource(),
                        "select * from resources"
                    );
                    break;
                case SCHEMA_COLUMNS :
                    {
                        List<SchemaColumn> clientColumns=new ArrayList<SchemaColumn>();
                        PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                            "select\n"
                            + "  sc.*\n"
                            + "from\n"
                            + "  aoserv_protocols client_ap,\n"
                            + "  schema_columns sc\n"
                            + "  inner join aoserv_protocols sc_ap on sc.since_version=sc_ap.version\n"
                            + "  left join aoserv_protocols last_ap on sc.last_version=last_ap.version,\n"
                            + "  schema_tables st\n"
                            + "where\n"
                            + "  client_ap.version=?\n"
                            + "  and client_ap.created>=sc_ap.created\n"
                            + "  and (last_ap.created is null or client_ap.created<=last_ap.created)\n"
                            + "  and sc.table_name=st.name\n"
                            + "order by\n"
                            + "  st.table_id,\n"
                            + "  sc.index"
                        );
                        try {
                            pstmt.setString(1, source.getProtocolVersion());
                            
                            conn.incrementQueryCount();
                            ResultSet results=pstmt.executeQuery();
                            try {
                                int clientColumnIndex=0;
                                String lastTableName=null;
                                SchemaColumn tempSC=new SchemaColumn();
                                while(results.next()) {
                                    tempSC.init(results);
                                    // Change the table ID if on next table
                                    String tableName=tempSC.getSchemaTableName();
                                    if(lastTableName==null || !lastTableName.equals(tableName)) {
                                        clientColumnIndex=0;
                                        lastTableName=tableName;
                                    }
                                    clientColumns.add(
                                        new SchemaColumn(
                                            tempSC.getPkey(),
                                            tableName,
                                            tempSC.getColumnName(),
                                            clientColumnIndex++,
                                            tempSC.getSchemaTypeName(),
                                            tempSC.isNullable(),
                                            tempSC.isUnique(),
                                            tempSC.isPublic(),
                                            tempSC.getDescription(),
                                            tempSC.getSinceVersion(),
                                            tempSC.getLastVersion()
                                        )
                                    );
                                }
                            } finally {
                                results.close();
                            }
                        } catch(SQLException err) {
                            System.err.println("Error from query: "+pstmt.toString());
                            throw err;
                        } finally {
                            pstmt.close();
                        }
                        MasterServer.writeObjects(
                            source,
                            out,
                            provideProgress,
                            clientColumns
                        );
                    }
                    break;
                case SCHEMA_FOREIGN_KEYS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new SchemaForeignKey(),
                        "select\n"
                        + "  sfk.*\n"
                        + "from\n"
                        + "  aoserv_protocols client_ap,\n"
                        + "  schema_foreign_keys sfk\n"
                        + "  inner join aoserv_protocols since_version on sfk.since_version=since_version.version\n"
                        + "  left join aoserv_protocols last_version on sfk.last_version=last_version.version\n"
                        + "where\n"
                        + "  client_ap.version=?\n"
                        + "  and client_ap.created>=since_version.created\n"
                        + "  and (last_version.created is null or client_ap.created<=last_version.created)",
                        source.getProtocolVersion()
                    );
                    break;
                case SCHEMA_TABLES :
                    {
                        List<SchemaTable> clientTables=new ArrayList<SchemaTable>();
                        PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                            "select\n"
                            + "  st.*\n"
                            + "from\n"
                            + "  aoserv_protocols client_ap,\n"
                            + "  schema_tables st\n"
                            + "  inner join aoserv_protocols since_version on st.since_version=since_version.version\n"
                            + "  left join aoserv_protocols last_version on st.last_version=last_version.version\n"
                            + "where\n"
                            + "  client_ap.version=?\n"
                            + "  and client_ap.created>=since_version.created\n"
                            + "  and (last_version.created is null or client_ap.created<=last_version.created)\n"
                            + "order by\n"
                            + "  st.table_id"
                        );
                        try {
                            pstmt.setString(1, source.getProtocolVersion());
                            
                            conn.incrementQueryCount();
                            ResultSet results=pstmt.executeQuery();
                            try {
                                int clientTableID=0;
                                SchemaTable tempST=new SchemaTable();
                                while(results.next()) {
                                    tempST.init(results);
                                    clientTables.add(
                                        new SchemaTable(
                                            tempST.getName(),
                                            clientTableID++,
                                            tempST.getDisplay(),
                                            tempST.isPublic(),
                                            tempST.getDescription(),
                                            tempST.getSinceVersion(),
                                            tempST.getLastVersion()
                                        )
                                    );
                                }
                            } finally {
                                results.close();
                            }
                        } catch(SQLException err) {
                            System.err.println("Error from query: "+pstmt.toString());
                            throw err;
                        } finally {
                            pstmt.close();
                        }
                        MasterServer.writeObjects(
                            source,
                            out,
                            provideProgress,
                            clientTables
                        );
                    }
                    break;
                case SCHEMA_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new SchemaType(),
                        "select * from schema_types order by num"
                    );
                    break;
                case EMAIL_DOMAINS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailDomain(),
                            "select * from email_domains"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailDomain(),
                            "select\n"
                            + "  ed.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_domains ed\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ed.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailDomain(),
                        "select\n"
                        + "  ed.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_domains ed\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=ed.package",
                        username
                    );
                    break;
                case EMAIL_SMTP_RELAY_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailSmtpRelayType(),
                        "select * from email_smtp_relay_types"
                    );
                    break;
                case EMAIL_SMTP_RELAYS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailSmtpRelay(),
                            "select * from email_smtp_relays"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new EmailSmtpRelay(),
                            "select distinct\n"
                            + "  esr.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  email_smtp_relays esr\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            + "    esr.ao_server is null\n"
                            + "    or ms.server=esr.ao_server\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new EmailSmtpRelay(),
                        "select distinct\n"
                        + "  esr.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  email_smtp_relays esr\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and (\n"
                        + "    pk2.name=esr.package\n"
                        + "    or esr.ao_server is null\n"
                        + "  )",
                        username
                    );
                    break;
                case SERVER_FARMS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new ServerFarm(),
                            "select * from server_farms"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new ServerFarm(),
                            "select distinct\n"
                            + "  sf.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  servers se\n"
                            + "  left outer join failover_file_replications ffr on se.pkey=ffr.server\n"
                            + "  left outer join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
                            + "  left outer join servers fs on bp.ao_server=fs.pkey,\n"
                            + "  server_farms sf\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=se.pkey\n"
                            + "  and (\n"
                            + "    se.farm=sf.name\n"
                            + "    or fs.farm=sf.name\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new ServerFarm(),
                        "select distinct\n"
                        + "  sf.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  servers se,\n"
                        + "  server_farms sf\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + "    (\n"
                        + "      pk.accounting=bs.accounting\n"
                        + "      and bs.server=se.pkey\n"
                        + "      and se.farm=sf.name\n"
                        + "    ) or pk.pkey=sf.owner\n"
                        + "  )",
                        username
                    );
                    break;
                case SERVERS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Server(),
                            "select * from servers"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Server(),
                            "select distinct\n"
                            + "  se.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  left join ao_servers ao on ms.server=ao.server\n"
                            // Allow its failover parent
                            + "  left join ao_servers ff on ao.failover_server=ff.server\n"
                            // Allow its failover children
                            + "  left join ao_servers fs on ao.server=fs.failover_server\n"
                            // Allow servers it replicates to
                            + "  left join failover_file_replications ffr on ms.server=ffr.server\n"
                            + "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
                            + "  servers se\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            // Allow direct access
                            + "    ms.server=se.pkey\n"
                            // Allow its failover parent
                            + "    or ff.server=se.pkey\n"
                            // Allow its failover children
                            + "    or fs.server=se.pkey\n"
                            // Allow servers it replicates to
                            + "    or bp.ao_server=se.pkey\n"
                            + "  )",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Server(),
                        "select distinct\n"
                        + "  se.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        // Allow servers it replicates to
                        //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
                        //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
                        + "  servers se\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and (\n"
                        + "    bs.server=se.pkey\n"
                        // Allow servers it replicates to
                        //+ "    or bp.ao_server=se.pkey\n"
                        + "  )",
                        username
                    );
                    break;
                case SHELLS :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Shell(),
                        "select * from shells"
                    );
                    break;
                case SIGNUP_REQUEST_OPTIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new SignupRequestOption(),
                                "select * from signup_request_options"
                            );
                        } else {
                            List<SignupRequestOption> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new SignupRequestOption(),
                            "select\n"
                            + "  sro.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  signup_requests sr,\n"
                            + "  signup_request_options sro\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=sr.accounting\n"
                            + "  and sr.pkey=sro.request",
                            username
                        );
                    }
                    break;
                case SIGNUP_REQUESTS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new SignupRequest(),
                                "select * from signup_requests"
                            );
                        } else {
                            List<SignupRequest> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else {
                        MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new SignupRequest(),
                            "select\n"
                            + "  sr.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk1,\n"
                            + BU1_PARENTS_JOIN
                            + "  signup_requests sr\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk1.name\n"
                            + "  and (\n"
                            + PK1_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=sr.accounting",
                            username
                        );
                    }
                    break;
                case SPAM_EMAIL_MESSAGES :
                    if(masterUser!=null && masterServers.length==0) MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new SpamEmailMessage(),
                        "select * from spam_email_messages"
                    ); else MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());
                    break;
                case SYSTEM_EMAIL_ALIASES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new SystemEmailAlias(),
                            "select * from system_email_aliases"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new SystemEmailAlias(),
                            "select\n"
                            + "  sea.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  system_email_aliases sea\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=sea.ao_server",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new SystemEmailAlias(),
                        "select\n"
                        + "  sea.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + "  business_servers bs,\n"
                        + "  system_email_aliases sea\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and pk.accounting=bs.accounting\n"
                        + "  and bs.server=sea.ao_server",
                        username
                    );
                    break;
                case TECHNOLOGY_CLASSES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TechnologyClass(),
                        "select * from technology_classes"
                    );
                    break;
                case TECHNOLOGY_NAMES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TechnologyName(),
                        "select * from technology_names"
                    );
                    break;
                case TECHNOLOGIES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Technology(),
                        "select * from technologies"
                    );
                    break;
                case TECHNOLOGY_VERSIONS :
                    if(masterUser!=null) MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TechnologyVersion(),
                        "select * from technology_versions"
                    ); else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TechnologyVersion(),
                        "select\n"
                        + "  pkey,\n"
                        + "  name,\n"
                        + "  version,\n"
                        + "  updated,\n"
                        + "  '"+AOServProtocol.FILTERED+"'::text,\n"
                        + "  operating_system_version\n"
                        + "from\n"
                        + "  technology_versions"
                    );
                    break;
                case TICKETS :
                    if(masterUser!=null) {
                        if(masterServers.length==0 || masterUser.isTicketAdmin()) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Ticket(),
                            "select * from tickets"
                        ); else MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Ticket(),
                        "select\n"
                        + "  ti.*\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  tickets ti\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=ti.accounting",
                        username
                    );
                    break;
                case TICKET_PRIORITIES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TicketPriority(),
                        "select * from ticket_priorities"
                    );
                    break;
                case TICKET_STATI :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TicketStatus(),
                        "select * from ticket_stati"
                    );
                    break;
                case TICKET_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TicketType(),
                        "select * from ticket_types"
                    );
                    break;
                case TIME_ZONES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TimeZone(),
                        "select * from time_zones"
                    );
                    break;
                case TRANSACTION_TYPES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new TransactionType(),
                        "select * from transaction_types"
                    );
                    break;
                case TRANSACTIONS :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Transaction(),
                            "select * from transactions"
                        ); else MasterServer.writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Transaction(),
                        "select\n"
                        + "  tr.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  transactions tr\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=tr.accounting",
                        username
                    );
                    break;
                case USERNAMES :
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Username(),
                            "select * from usernames"
                        ); else MasterServer.writeObjects(
                            conn,
                            source,
                            out,
                            provideProgress,
                            new Username(),
                            "select distinct\n"
                            + "  un.*\n"
                            + "from\n"
                            + "  master_servers ms\n"
                            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
                            + "  business_servers bs,\n"
                            + "  packages pk,\n"
                            + "  usernames un\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and (\n"
                            + "    ms.server=bs.server\n"
                            + "    or ff.server=bs.server\n"
                            + "  ) and bs.accounting=pk.accounting\n"
                            + "  and pk.name=un.package",
                            username
                        );
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Username(),
                        "select\n"
                        + "  un2.*\n"
                        + "from\n"
                        + "  usernames un1,\n"
                        + "  packages pk1,\n"
                        + BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  usernames un2\n"
                        + "where\n"
                        + "  un1.username=?\n"
                        + "  and un1.package=pk1.name\n"
                        + "  and (\n"
                        + "    un2.username=un1.username\n"
                        + "    or un2.username='"+LinuxAccount.MAIL+"'\n"
                        + PK1_BU1_PARENTS_OR_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.name=un2.package",
                        username
                    );
                    break;
                case US_STATES :
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new USState(),
                        "select * from us_states"
                    );
                    break;
                case WHOIS_HISTORY :
                    if(masterUser!=null) {
                        if(masterServers.length==0) {
                            MasterServer.writeObjects(
                                conn,
                                source,
                                out,
                                provideProgress,
                                new WhoisHistory(),
                                "select pkey, time, accounting, zone from whois_history"
                            );
                        } else {
                            // The servers don't need access to this information
                            List<WhoisHistory> emptyList = Collections.emptyList();
                            MasterServer.writeObjects(source, out, provideProgress, emptyList);
                        }
                    } else MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new WhoisHistory(),
                        "select\n"
                        + "  wh.pkey,\n"
                        + "  wh.time,\n"
                        + "  wh.accounting,\n"
                        + "  wh.zone\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk,\n"
                        + BU1_PARENTS_JOIN
                        + "  whois_history wh\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk.name\n"
                        + "  and (\n"
                        + PK_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=wh.accounting",
                        username
                    );
                    break;
                default :
                    throw new IOException("Unknown table ID: "+tableID);
            }
        } finally {
            Profiler.endProfile(profilerLevel);
        }
    }

    public static void invalidate(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        SchemaTable.TableID tableID,
        int server
    ) throws SQLException, IOException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "invalidate(RequestSource,CompressedDataOutputStream,int)", getTableName(conn, tableID));
        try {
            checkInvalidator(conn, source, "invalidate");
            invalidateList.addTable(
                conn,
                tableID,
                InvalidateList.allBusinesses,
                server==-1 ? InvalidateList.allServers : InvalidateList.getServerCollection(server),
                true
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkInvalidator(MasterDatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "checkInvalidator(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            if(!isInvalidator(conn, source)) throw new SQLException("Table invalidation not allowed, '"+action+"'");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isInvalidator(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "isInvalidator(MasterDatabaseConnection,RequestSource)", null);
        try {
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            return mu!=null && mu.canInvalidateTables();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static Map<SchemaTable.TableID,String> tableNames=new EnumMap<SchemaTable.TableID,String>(SchemaTable.TableID.class);

    public static String getTableName(MasterDatabaseConnection conn, SchemaTable.TableID tableID) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "getTableName(MasterDatabaseConnection,SchemaTable.TableID)", null);
        try {
            synchronized(tableNames) {
                String name=tableNames.get(tableID);
                if(name==null) {
                    name=conn.executeStringQuery("select name from schema_tables where table_id=?", convertClientTableIDToDBTableID(conn, AOServProtocol.CURRENT_VERSION, tableID.ordinal()));
                    if(name==null) throw new SQLException("Unable to find table name for table ID: "+tableID);
                    tableNames.put(tableID, name);
                }
                return name;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static Map<String,Map<Integer,Integer>> fromClientTableIDs=new HashMap<String,Map<Integer,Integer>>();

    /**
     * Converts a specific AOServProtocol version table ID to the number used in the database storage.
     */
    public static int convertClientTableIDToDBTableID(
        MasterDatabaseConnection conn,
        String version,
        int clientTableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "convertClientTableIDToDBTableID(MasterDatabaseConnection,String,int)", null);
        try {
            Map<Integer,Integer> tableIDs=fromClientTableIDs.get(version);
            if(tableIDs==null) {
                IntList clientTables=conn.executeIntListQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    "select\n"
                    + "  st.table_id\n"
                    + "from\n"
                    + "  aoserv_protocols client_ap,\n"
                    + "  schema_tables st\n"
                    + "  inner join aoserv_protocols since_version on st.since_version=since_version.version\n"
                    + "  left join aoserv_protocols last_version on st.last_version=last_version.version\n"
                    + "where\n"
                    + "  client_ap.version=?\n"
                    + "  and client_ap.created>=since_version.created\n"
                    + "  and (last_version.created is null or client_ap.created<=last_version.created)\n"
                    + "order by\n"
                    + "  st.table_id",
                    version
                );
                int numTables=clientTables.size();
                tableIDs=new HashMap<Integer,Integer>(numTables);
                for(int c=0;c<numTables;c++) {
                    tableIDs.put(c, clientTables.getInt(c));
                }
                fromClientTableIDs.put(version, tableIDs);
            }
            Integer I=tableIDs.get(clientTableID);
            return I==null?-1:I.intValue();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static Map<String,Map<Integer,Integer>> toClientTableIDs=new HashMap<String,Map<Integer,Integer>>();

    public static int convertDBTableIDToClientTableID(
        MasterDatabaseConnection conn,
        String version,
        int tableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "convertDBTableIDToClientTableID(MasterDatabaseConnection,String,int)", null);
        try {
            Map<Integer,Integer> clientTableIDs=toClientTableIDs.get(version);
            if(clientTableIDs==null) {
                IntList clientTables=conn.executeIntListQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    "select\n"
                    + "  st.table_id\n"
                    + "from\n"
                    + "  aoserv_protocols client_ap,\n"
                    + "  schema_tables st\n"
                    + "  inner join aoserv_protocols since_version on st.since_version=since_version.version\n"
                    + "  left join aoserv_protocols last_version on st.last_version=last_version.version\n"
                    + "where\n"
                    + "  client_ap.version=?\n"
                    + "  and client_ap.created>=since_version.created\n"
                    + "  and (last_version.created is null or client_ap.created<=last_version.created)\n"
                    + "order by\n"
                    + "  st.table_id",
                    version
                );
                int numTables=clientTables.size();
                clientTableIDs=new HashMap<Integer,Integer>(numTables);
                for(int c=0;c<numTables;c++) {
                    clientTableIDs.put(clientTables.getInt(c), c);
                }
                toClientTableIDs.put(version, clientTableIDs);
            }
            Integer I=clientTableIDs.get(tableID);
            int clientTableID=I==null?-1:I.intValue();
            return clientTableID;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Converts the client's AOServProtocol-version-specific table ID to the version used by the master's AOServProtocol version.
     *
     * @return  The <code>SchemaTable.TableID</code> or <code>null</code> if no match.
     */
    public static SchemaTable.TableID convertFromClientTableID(
        MasterDatabaseConnection conn,
        RequestSource source,
        int clientTableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "convertFromClientTableID(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int dbTableID=convertClientTableIDToDBTableID(conn, source.getProtocolVersion(), clientTableID);
            if(dbTableID==-1) return null;
            int tableID = convertDBTableIDToClientTableID(conn, AOServProtocol.CURRENT_VERSION, dbTableID);
            if(tableID==-1) return null;
            return tableIDs[tableID];
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Converts a local (Master AOServProtocol) table ID to a client-version matched table ID.
     */
    public static int convertToClientTableID(
        MasterDatabaseConnection conn,
        RequestSource source,
        SchemaTable.TableID tableID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "convertToClientTableID(MasterDatabaseConnection,RequestSource,SchemaTable.TableID)", null);
        try {
            int dbTableID=convertClientTableIDToDBTableID(conn, AOServProtocol.CURRENT_VERSION, tableID.ordinal());
            if(dbTableID==-1) return -1;
            return convertDBTableIDToClientTableID(conn, source.getProtocolVersion(), dbTableID);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static Map<String,Map<SchemaTable.TableID,Map<String,Integer>>> clientColumnIndexes=new HashMap<String,Map<SchemaTable.TableID,Map<String,Integer>>>();

    public static int getClientColumnIndex(
        MasterDatabaseConnection conn,
        RequestSource source,
        SchemaTable.TableID tableID,
        String columnName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "getClientColumnIndex(MasterDatabaseConnection,RequestSource,SchemaTable.TableID,String)", null);
        try {
            // Get the list of resolved tables for the requested version
            String version=source.getProtocolVersion();
            Map<SchemaTable.TableID,Map<String,Integer>> tables=clientColumnIndexes.get(version);
            if(tables==null) clientColumnIndexes.put(version, tables=new EnumMap<SchemaTable.TableID,Map<String,Integer>>(SchemaTable.TableID.class));
            
            // Find the list of columns for this table
            Map<String,Integer> columns=tables.get(tableID);
            if(columns==null) {
                List<String> clientColumns=conn.executeStringListQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    "select\n"
                    + "  sc.column_name\n"
                    + "from\n"
                    + "  aoserv_protocols client_ap,\n"
                    + "  schema_columns sc\n"
                    + "  inner join aoserv_protocols since_version on sc.since_version=since_version.version\n"
                    + "  left join aoserv_protocols last_version on sc.last_version=last_version.version\n"
                    + "where\n"
                    + "  client_ap.version=?\n"
                    + "  and client_ap.created>=since_version.created\n"
                    + "  and (last_version.created is null or client_ap.created<=last_version.created)\n"
                    + "order by\n"
                    + "  sc.index",
                    version
                );
                int numColumns=clientColumns.size();
                columns=new HashMap<String,Integer>(numColumns);
                for(int c=0;c<numColumns;c++) {
                    columns.put(clientColumns.get(c), Integer.valueOf(c));
                }
                tables.put(tableID, columns);
            }
            
            // Return the column or -1 if not found
            Integer columnIndex=columns.get(columnName);
            return columnIndex==null?-1:columnIndex.intValue();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, TableHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
            if(tableID==SchemaTable.TableID.SCHEMA_TABLES) {
                synchronized(tableNames) {
                    tableNames.clear();
                }
            }
            if(tableID==SchemaTable.TableID.AOSERV_PROTOCOLS || tableID==SchemaTable.TableID.SCHEMA_TABLES) {
                synchronized(fromClientTableIDs) {
                    fromClientTableIDs.clear();
                }
                synchronized(toClientTableIDs) {
                    toClientTableIDs.clear();
                }
            }
            if(tableID==SchemaTable.TableID.AOSERV_PROTOCOLS || tableID==SchemaTable.TableID.SCHEMA_COLUMNS) {
                synchronized(clientColumnIndexes) {
                    clientColumnIndexes.clear();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static IntList getOperatingSystemVersions(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TableHandler.class, "getOperatingSystemVersions(MasterDatabaseConnection,RequestSource)", null);
        try {
            return conn.executeIntListQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                "select distinct\n"
                + "  se.operating_system_version\n"
                + "from\n"
                + "  master_servers ms,\n"
                + "  ao_servers ao,\n"
                + "  servers se,\n"
                + "  operating_system_versions osv\n"
                + "where\n"
                + "  ms.username=?\n"
                + "  and ms.server=ao.server\n"
                + "  and ao.server=se.pkey\n"
                + "  and se.operating_system_version=osv.pkey\n"
                + "  and osv.is_aoserv_daemon_supported",
                source.getUsername()
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}