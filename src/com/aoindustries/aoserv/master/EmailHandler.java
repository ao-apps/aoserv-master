package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>EmailHandler</code> handles all the accesses to the email tables.
 *
 * @author  AO Industries, Inc.
 */
final public class EmailHandler {

    private final static Map<Integer,Boolean> disabledEmailLists=new HashMap<Integer,Boolean>();
    private final static Map<Integer,Boolean> disabledEmailPipes=new HashMap<Integer,Boolean>();
    private final static Map<Integer,Boolean> disabledEmailSmtpRelays=new HashMap<Integer,Boolean>();

    public static boolean canAccessEmailDomain(MasterDatabaseConnection conn, RequestSource source, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "canAccessEmailDomain(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    return ServerHandler.canAccessServer(conn, source, getAOServerForEmailDomain(conn, domain));
                } else {
                    return true;
                }
            } else {
                return PackageHandler.canAccessPackage(conn, source, getPackageForEmailDomain(conn, domain));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailDomain(MasterDatabaseConnection conn, RequestSource source, String action, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailDomain(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailDomain(conn, domain));
                }
            } else {
                PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailDomain(conn, domain));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailSmtpRelay(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailSmtpRelay(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailSmtpRelay(conn, pkey));
                }
            } else {
                PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailSmtpRelay(conn, pkey));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailAddress(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailAddress(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            checkAccessEmailDomain(conn, source, action, getEmailDomainForEmailAddress(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailList(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailList(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, action, getLinuxServerGroupForEmailList(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailListPath(MasterDatabaseConnection conn, RequestSource source, String action, int aoServer, String path) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailListPath(MasterDatabaseConnection,RequestSource,String,int,String)", null);
        try {
            if(!EmailList.isValidRegularPath(path)) {
                // Can also be a path in a majordomo server that they may access
                if(path.startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY+'/')) {
                    int pos=path.indexOf('/', MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.length()+1);
                    if(pos!=-1) {
                        String domain=path.substring(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.length()+1, pos);
                        path=path.substring(pos+1);
                        if(path.startsWith("lists/")) {
                            String listName=path.substring(6);
                            if(MajordomoList.isValidListName(listName)) {
                                int ed=getEmailDomain(conn, aoServer, domain);
                                checkAccessMajordomoServer(conn, source, action, getMajordomoServer(conn, ed));
                                return;
                            }
                        }
                    }
                }
                String message="email_lists.path="+path+" not allowed, '"+action+"'";
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailPipe(MasterDatabaseConnection conn, RequestSource source, String action, int pipe) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "checkAccessEmailPipe(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                    ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailPipe(conn, pipe));
                }
            } else {
                PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailPipe(conn, pipe));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessEmailPipePath(
        MasterDatabaseConnection conn,
        RequestSource source,
        String action,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "checkAccessEmailPipePath(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            throw new SQLException("Method not implemented.");
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessMajordomoServer(MasterDatabaseConnection conn, RequestSource source, String action, int majordomoServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, EmailHandler.class, "checkAccessMajordomoServer(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            checkAccessEmailDomain(conn, source, action, majordomoServer);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public static int addEmailAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String address, 
        int domain
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "addEmailAddress(MasterDatabaseConnection,RequestSource,InvalidateList,String,int)", null);
        try {
            checkAccessEmailDomain(conn, source, "addEmailAddress", domain);

            return addEmailAddress0(conn, invalidateList, address, domain);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static int addEmailAddress0(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String address, 
        int domain
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailAddress0(MasterDatabaseConnection,InvalidateList,String,int)", null);
        try {
            if (!EmailAddress.isValidFormat(address)) throw new SQLException("Invalid email address: " + address);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_addresses_pkey_seq')");

            conn.executeUpdate("insert into email_addresses values(?,?,?)", pkey, address, domain);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_ADDRESSES,
                getBusinessForEmailAddress(conn, pkey),
                getAOServerForEmailAddress(conn, pkey),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addEmailForwarding(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int address, 
        String destination
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailForwarding(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if (!EmailAddress.isValidEmailAddress(destination)) throw new SQLException("Invalid email forwarding destination: " + destination);

            if(destination.toLowerCase().endsWith("@comcast.net")) throw new SQLException(
                "We no longer allow forwarding to comcast accounts.  Comcast blocks mail servers\n"
                + "that forward spam originating from other networks.  Our spam filters are\n"
                + "associated with email inboxes, not forwarding settings.  Our forwarding\n"
                + "configuration assumes the final recipient account will provide spam filters.\n"
                + "Also, our spam filters rely heavily on feedback from the mail client, and\n"
                + "this feedback is not available from forwarded email.  For this reason we\n"
                + "will not provide filters on the forwarded email.\n"
                + "\n"
                + "Please create an email inbox, associate your email address with the inbox and\n"
                + "obtain your email directly from our mail servers over POP3 or IMAP instead of\n"
                + "forwarding to comcast.net.\n"
                + "\n"
                + "Sorry for any inconvenience, but Comcast's unprecedented blocking policy and\n"
                + "our standard installation of SpamAssassin filters are not compatible.\n"
            );

            checkAccessEmailAddress(conn, source, "addEmailForwarding", address);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_forwarding_pkey_seq')");
            conn.executeUpdate("insert into email_forwarding values(?,?,?)", pkey, address, destination);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_FORWARDING,
                getBusinessForEmailAddress(conn, address),
                getAOServerForEmailAddress(conn, address),
                false
            );
            
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addEmailList(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String path,
        int linuxServerAccount,
        int linuxServerGroup
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailList(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,int)", null);
        try {
            checkAccessEmailListPath(conn, source, "addEmailList", LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linuxServerAccount), path);

            // Allow the mail user
            String username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linuxServerAccount);
            if(!username.equals(LinuxAccount.MAIL)) LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addEmailList", linuxServerAccount);
            // Check the group
            LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addEmailList", linuxServerGroup);
            
            return addEmailList0(
                conn,
                invalidateList,
                path,
                linuxServerAccount,
                linuxServerGroup
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    private static int addEmailList0(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String path,
        int linuxServerAccount,
        int linuxServerGroup
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailList0(MasterDatabaseConnection,InvalidateList,String,int,int)", null);
        try {
            if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, linuxServerAccount)) throw new SQLException("Unable to add EmailList, LinuxServerAccount disabled: "+linuxServerAccount);
            String packageName=LinuxAccountHandler.getPackageForLinuxServerGroup(conn, linuxServerGroup);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add EmailList, Package disabled: "+packageName);

            // The server for both account and group must be the same
            int accountAOServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linuxServerAccount);
            int groupAOServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, linuxServerGroup);
            if(accountAOServer!=groupAOServer) throw new SQLException("(linux_server_accounts.pkey="+linuxServerAccount+").ao_server!=(linux_server_groups.pkey="+linuxServerGroup+").ao_server");
            // Must not already have this path on this server
            if(
                conn.executeBooleanQuery(
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      el.pkey\n"
                    + "    from\n"
                    + "      email_lists el,\n"
                    + "      linux_server_groups lsg\n"
                    + "    where\n"
                    + "      el.path=?\n"
                    + "      and el.linux_server_group=lsg.pkey\n"
                    + "      and lsg.ao_server=?\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    path,
                    groupAOServer
                )
            ) throw new SQLException("EmailList path already used: "+path+" on "+groupAOServer);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_lists_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  email_lists\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  null\n"
                + ")",
                pkey,
                path,
                linuxServerAccount,
                linuxServerGroup
            );

            // Create the empty list file
            DaemonHandler.getDaemonConnector(conn, accountAOServer).setEmailListFile(
                path,
                "",
                LinuxAccountHandler.getUIDForLinuxServerAccount(conn, linuxServerAccount),
                LinuxAccountHandler.getGIDForLinuxServerGroup(conn, linuxServerGroup),
                path.startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY)?0644:0640
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_LISTS,
                InvalidateList.allBusinesses,
                accountAOServer,
                false
            );
            
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addEmailListAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int address, 
        int email_list
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "addEmailListAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            checkAccessEmailAddress(conn, source, "addEmailListAddress", address);
            checkAccessEmailList(conn, source, "addEmailListAddress", email_list);

            return addEmailListAddress0(
                conn,
                invalidateList,
                address,
                email_list
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static int addEmailListAddress0(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int address, 
        int email_list
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailListAddress0(MasterDatabaseConnection,InvalidateList,int,int)", null);
        try {
            // The email_domain and the email_list must be on the same server
            int domainAOServer=getAOServerForEmailAddress(conn, address);
            int listServer=getAOServerForEmailList(conn, email_list);
            if(domainAOServer!=listServer) throw new SQLException("List server ("+listServer+")!=Email address server ("+domainAOServer+')');

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_list_addresses_pkey_seq')");
            conn.executeUpdate("insert into email_list_addresses values(?,?,?)", pkey, address, email_list);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_LIST_ADDRESSES,
                getBusinessForEmailAddress(conn, address),
                getAOServerForEmailAddress(conn, address),
                false
            );

            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds an email pipe.
     */
    public static int addEmailPipe(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer,
        String path,
        String packageName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "addEmailPipe(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, "addEmailPipe", aoServer);
            checkAccessEmailPipePath(conn, source, "addEmailPipe", path);
            PackageHandler.checkAccessPackage(conn, source, "addEmailPipe", packageName);
            PackageHandler.checkPackageAccessServer(conn, source, "addEmailPipe", packageName, aoServer);

            return addEmailPipe0(
                conn,
                invalidateList,
                aoServer,
                path,
                packageName
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static int addEmailPipe0(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int aoServer,
        String path,
        String packageName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailPipe0(MasterDatabaseConnection,InvalidateList,int,String,String)", null);
        try {
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add EmailPipe, Package disabled: "+packageName);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_pipes_pkey_seq')");

            conn.executeUpdate("insert into email_pipes values(?,?,?,?,null)", pkey, aoServer, path, packageName);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_PIPES,
                PackageHandler.getBusinessForPackage(conn, packageName),
                aoServer,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addEmailPipeAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int address, 
        int pipe
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "addEmailPipeAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            checkAccessEmailAddress(conn, source, "addEmailPipeAddress", address);
            checkAccessEmailPipe(conn, source, "addEmailPipeAddress", pipe);

            return addEmailPipeAddress0(
                conn,
                invalidateList,
                address,
                pipe
            );
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static int addEmailPipeAddress0(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int address, 
        int pipe
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailPipeAddress0(MasterDatabaseConnection,InvalidateList,int,int)", null);
        try {
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_pipe_addresses_pkey_seq')");
            conn.executeUpdate("insert into email_pipe_addresses values(?,?,?)", pkey, address, pipe);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_PIPE_ADDRESSES,
                getBusinessForEmailAddress(conn, address),
                getAOServerForEmailAddress(conn, address),
                false
            );
            
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addLinuxAccAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int address, 
        int lsa
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addLinuxAccAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            checkAccessEmailAddress(conn, source, "addLinuxAccAddress", address);
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addLinuxAccAddress", lsa);
            String username = LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, lsa);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add email addresses to LinuxAccount named '"+LinuxAccount.MAIL+'\'');
            // TODO: Make sure they are on the same server

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('linux_acc_addresses_pkey_seq')");
            conn.executeUpdate("insert into linux_acc_addresses values(?,?,?)", pkey, address, lsa);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.LINUX_ACC_ADDRESSES,
                getBusinessForEmailAddress(conn, address),
                getAOServerForEmailAddress(conn, address),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addEmailDomain(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String domain,
        int aoServer,
        String packageName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailDomain(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,String)", null);
        try {
            MasterServer.checkAccessHostname(conn, source, "addEmailDomain", domain);
            ServerHandler.checkAccessServer(conn, source, "addEmailDomain", aoServer);
            PackageHandler.checkAccessPackage(conn, source, "addEmailDomain", packageName);
            PackageHandler.checkPackageAccessServer(conn, source, "addEmailDomain", packageName, aoServer);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_domains_pkey_seq')");
            conn.executeUpdate("insert into email_domains values(?,?,?,?)", pkey, domain, aoServer, packageName);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_DOMAINS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                aoServer,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a email SMTP relay.
     */
    public static int addEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String packageName,
        int aoServer,
        String host,
        String type,
        long duration
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addEmailSmtpRelay(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,String,String,long)", null);
        try {
            // Only master users can add relays
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            if(mu==null) throw new SQLException("Only master users may add SMTP relays.");

            PackageHandler.checkAccessPackage(conn, source, "addEmailSmtpRelay", packageName);
            if(aoServer==-1) {
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) throw new SQLException("Only super-users may add global SMTP relays.");
            } else {
                ServerHandler.checkAccessServer(conn, source, "addEmailSmtpRelay", aoServer);
                PackageHandler.checkPackageAccessServer(conn, source, "addEmailSmtpRelay", packageName, aoServer);
            }
            if(!EmailSmtpRelay.isValidHost(host)) throw new SQLException("Invalid host format: "+host);
            if(duration!=EmailSmtpRelay.NO_EXPIRATION && duration<=0) throw new SQLException("Duration must be positive: "+duration);

            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add EmailSmtpRelay, Package disabled: "+packageName);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('email_smtp_relays_pkey_seq')");

            if(aoServer==-1) {
                conn.executeUpdate(
                    "insert into email_smtp_relays values(?,?,null,?,?,now(),now(),0,?,null)",
                    pkey,
                    packageName,
                    host,
                    type,
                    duration==-1?(Timestamp)null:new Timestamp(System.currentTimeMillis()+duration)
                );
            } else {
                conn.executeUpdate(
                    "insert into email_smtp_relays values(?,?,?,?,?,now(),now(),0,?,null)",
                    pkey,
                    packageName,
                    aoServer,
                    host,
                    type,
                    duration==-1?(Timestamp)null:new Timestamp(System.currentTimeMillis()+duration)
                );
            }

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_SMTP_RELAYS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                aoServer,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Only report each error at most once per 12 hours per package.
     */
    private static final long SMTP_STAT_REPORT_INTERVAL=12L*60*60*1000;
    private static final Map<String,Long> smtpStatLastReports=new HashMap<String,Long>();

    public static int addSpamEmailMessage(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int esr,
        String message
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addSpamEmailMessage(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            if(masterUser==null || masterServers.length!=0) throw new SQLException("Only master users may add spam email messages.");

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('spam_email_messages_pkey_seq')");
            conn.executeUpdate(
                "insert into spam_email_messages values(?,?,now(),?)",
                pkey,
                esr,
                message
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.SPAM_EMAIL_MESSAGES,
                InvalidateList.allBusinesses,
                InvalidateList.allServers,
                false
            );

            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addMajordomoList(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int majordomoServer,
        String listName
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addMajordomoList(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(!MajordomoList.isValidListName(listName)) throw new SQLException("Invalide Majordomo list name: "+listName);

            checkAccessMajordomoServer(conn, source, "addMajordomoList", majordomoServer);
            
            String domainName=getDomainForEmailDomain(conn, majordomoServer);
            String msPath=MajordomoServer.MAJORDOMO_SERVER_DIRECTORY+'/'+domainName;
            String listPath=msPath+"/lists/"+listName;
            int aoServer=getAOServerForEmailDomain(conn, majordomoServer);

            // Disabled checks
            String packageName=getPackageForEmailDomain(conn, majordomoServer);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Majordomo list, Package for Majordomo server #"+majordomoServer+" is disabled: "+packageName);

            // Find the email addresses
            int ownerListnameAddress=getOrAddEmailAddress(conn, invalidateList, "owner-"+listName, majordomoServer);
            int listnameOwnerAddress=getOrAddEmailAddress(conn, invalidateList, listName+"-owner", majordomoServer);
            int listnameApprovalAddress=getOrAddEmailAddress(conn, invalidateList, listName+"-approval", majordomoServer);
            
            // Add the email list
            int lsa=getLinuxServerAccountForMajordomoServer(conn, majordomoServer);
            int lsg=getLinuxServerGroupForMajordomoServer(conn, majordomoServer);
            int pkey=addEmailList0(
                conn,
                invalidateList,
                listPath,
                lsa,
                lsg
            );

            // Add the listname email pipe and address
            int listnamePipe=addEmailPipe0(
                conn,
                invalidateList,
                aoServer,
                msPath+"/wrapper resend -l "+listName+' '+listName+"-list@"+domainName,
                packageName
            );
            int listnameAddress=getOrAddEmailAddress(conn, invalidateList, listName, majordomoServer);
            int listnamePipeAddress=addEmailPipeAddress0(conn, invalidateList, listnameAddress, listnamePipe);
            
            // Add the listname-list email list address
            int listnameListAddress=getOrAddEmailAddress(conn, invalidateList, listName+"-list", majordomoServer);
            int listnameListListAddress=addEmailListAddress0(conn, invalidateList, listnameListAddress, pkey);

            // Add the listname-request email pipe and address
            int listnameRequestPipe=addEmailPipe0(
                conn,
                invalidateList,
                aoServer,
                msPath+"/wrapper majordomo -l "+listName,
                packageName
            );
            int listnameRequestAddress=getOrAddEmailAddress(conn, invalidateList, listName+"-request", majordomoServer);
            int listnameRequestPipeAddress=addEmailPipeAddress0(conn, invalidateList, listnameRequestAddress, listnameRequestPipe);

            // Add the majordomo_list
            conn.executeUpdate(
                "insert into majordomo_lists values(?,?,?,?,?,?,?,?,?)",
                pkey,
                majordomoServer,
                listName,
                listnamePipeAddress,
                listnameListListAddress,
                ownerListnameAddress,
                listnameOwnerAddress,
                listnameApprovalAddress,
                listnameRequestPipeAddress
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.MAJORDOMO_LISTS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                aoServer,
                false
            );

            // Create the empty info and intro files
            AOServDaemonConnector daemonConn=DaemonHandler.getDaemonConnector(conn, aoServer);
            int uid=LinuxAccountHandler.getUIDForLinuxServerAccount(conn, lsa);
            int gid=LinuxAccountHandler.getGIDForLinuxServerGroup(conn, lsg);
            daemonConn.setEmailListFile(
                listPath+".info",
                MajordomoList.getDefaultInfoFile(domainName, listName),
                uid,
                gid,
                0664
            );
            daemonConn.setEmailListFile(
                listPath+".intro",
                MajordomoList.getDefaultIntroFile(domainName, listName),
                uid,
                gid,
                0664
            );

            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void addMajordomoServer(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int domain,
        int lsa,
        int lsg,
        String version
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "addMajordomoServer(MasterDatabaseConnection,RequestSource,InvalidateList,int,int,int,String)", null);
        try {
            // Security checks
            checkAccessEmailDomain(conn, source, "addMajordomoServer", domain);
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addMajordomoServer", lsa);
            String lsaUsername=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, lsa);
            if(lsaUsername.equals(LinuxAccount.MAIL)) throw new SQLException("Unable to add MajordomoServer with LinuxServerAccount of '"+lsaUsername+'\'');
            String lsaType=LinuxAccountHandler.getTypeForLinuxServerAccount(conn, lsa);
            if(
                !lsaType.equals(LinuxAccountType.APPLICATION)
                && !lsaType.equals(LinuxAccountType.USER)
            ) throw new SQLException("May only add Majordomo servers using Linux accounts of type '"+LinuxAccountType.APPLICATION+"' or '"+LinuxAccountType.USER+"', trying to use '"+lsaType+'\'');
            LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addMajordomoServer", lsg);
            String lsgName=LinuxAccountHandler.getGroupNameForLinuxServerGroup(conn, lsg);
            if(
                lsgName.equals(LinuxGroup.FTPONLY)
                || lsgName.equals(LinuxGroup.MAIL)
                || lsgName.equals(LinuxGroup.MAILONLY)
            ) throw new SQLException("Unable to add MajordomoServer with LinuxServerGroup of '"+lsgName+'\'');
            String lsgType=LinuxAccountHandler.getTypeForLinuxServerGroup(conn, lsg);
            if(
                !lsgType.equals(LinuxGroupType.APPLICATION)
                && !lsgType.equals(LinuxGroupType.USER)
            ) throw new SQLException("May only add Majordomo servers using Linux groups of type '"+LinuxGroupType.APPLICATION+"' or '"+LinuxGroupType.USER+"', trying to use '"+lsgType+'\'');

            // Data integrity checks
            int domainAOServer=getAOServerForEmailDomain(conn, domain);
            int lsaAOServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa);
            if(domainAOServer!=lsaAOServer) throw new SQLException("((email_domains.pkey="+domain+").ao_server='"+domainAOServer+"')!=((linux_server_accounts.pkey="+lsa+").ao_server='"+lsaAOServer+"')");
            int lsgAOServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, lsg);
            if(domainAOServer!=lsgAOServer) throw new SQLException("((email_domains.pkey="+domain+").ao_server='"+domainAOServer+"')!=((linux_server_groups.pkey="+lsg+").ao_server='"+lsgAOServer+"')");

            // Disabled checks
            String packageName=getPackageForEmailDomain(conn, domain);
            if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Majordomo server: Package for domain #"+domain+" is disabled: "+packageName);
            if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to add Majordomo server: LinuxServerAccount disabled: "+lsa);
            String lgPackageName=LinuxAccountHandler.getPackageForLinuxServerGroup(conn, lsg);
            if(PackageHandler.isPackageDisabled(conn, lgPackageName)) throw new SQLException("Unable to add Majordomo server: Package for LinuxServerGroup #"+lsg+" is disabled: "+lgPackageName);

            // Create the majordomo email pipe
            String domainName=getDomainForEmailDomain(conn, domain);
            String majordomoServerPath=MajordomoServer.MAJORDOMO_SERVER_DIRECTORY+'/'+domainName;
            int majordomoPipe=addEmailPipe0(conn, invalidateList, domainAOServer, majordomoServerPath+"/wrapper majordomo", packageName);
            int majordomoAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_ADDRESS, domain);
            int majordomoPipeAddress=addEmailPipeAddress0(conn, invalidateList, majordomoAddress, majordomoPipe);

            int ownerMajordomoAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.OWNER_MAJORDOMO_ADDRESS, domain);
            int majordomoOwnerAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_OWNER_ADDRESS, domain);

            conn.executeUpdate(
                "insert into\n"
                + "  majordomo_servers\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?\n"
                + ")",
                domain,
                lsa,
                lsg,
                version,
                majordomoPipeAddress,
                ownerMajordomoAddress,
                majordomoOwnerAddress
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.MAJORDOMO_SERVERS,
                PackageHandler.getBusinessForPackage(conn, packageName),
                domainAOServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableEmailList(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "disableEmailList(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isEmailListDisabled(conn, pkey)) throw new SQLException("EmailList is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailList", disableLog, false);
            checkAccessEmailList(conn, source, "disableEmailList", pkey);

            conn.executeUpdate(
                "update email_lists set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_LISTS,
                getBusinessForEmailList(conn, pkey),
                getAOServerForEmailList(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableEmailPipe(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "disableEmailPipe(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isEmailPipeDisabled(conn, pkey)) throw new SQLException("EmailPipe is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailPipe", disableLog, false);
            checkAccessEmailPipe(conn, source, "disableEmailPipe", pkey);

            conn.executeUpdate(
                "update email_pipes set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_PIPES,
                getBusinessForEmailPipe(conn, pkey),
                getAOServerForEmailPipe(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "disableEmailSmtpRelay(MasterDatabaseConnection,RequestSource,InvalidateList,int,int)", null);
        try {
            if(isEmailSmtpRelayDisabled(conn, pkey)) throw new SQLException("EmailSmtpRelay is already disabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailSmtpRelay", disableLog, false);
            checkAccessEmailSmtpRelay(conn, source, "disableEmailSmtpRelay", pkey);

            conn.executeUpdate(
                "update email_smtp_relays set disable_log=? where pkey=?",
                disableLog,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_SMTP_RELAYS,
                getBusinessForEmailSmtpRelay(conn, pkey),
                getAOServerForEmailSmtpRelay(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableEmailList(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "enableEmailList(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForEmailList(conn, pkey);
            if(disableLog==-1) throw new SQLException("EmailList is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailList", disableLog, true);
            checkAccessEmailList(conn, source, "enableEmailList", pkey);
            String pk=getPackageForEmailList(conn, pkey);
            if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailList #"+pkey+", Package not enabled: "+pk);

            conn.executeUpdate(
                "update email_lists set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_LISTS,
                PackageHandler.getBusinessForPackage(conn, pk),
                getAOServerForEmailList(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableEmailPipe(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "enableEmailPipe(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForEmailPipe(conn, pkey);
            if(disableLog==-1) throw new SQLException("EmailPipe is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailPipe", disableLog, true);
            checkAccessEmailPipe(conn, source, "enableEmailPipe", pkey);
            String pk=getPackageForEmailPipe(conn, pkey);
            if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailPipe #"+pkey+", Package not enabled: "+pk);

            conn.executeUpdate(
                "update email_pipes set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_PIPES,
                PackageHandler.getBusinessForPackage(conn, pk),
                getAOServerForEmailPipe(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "enableEmailSmtpRelay(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int disableLog=getDisableLogForEmailSmtpRelay(conn, pkey);
            if(disableLog==-1) throw new SQLException("EmailSmtpRelay is already enabled: "+pkey);
            BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailSmtpRelay", disableLog, true);
            checkAccessEmailSmtpRelay(conn, source, "enableEmailSmtpRelay", pkey);
            String pk=getPackageForEmailSmtpRelay(conn, pkey);
            if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailSmtpRelay #"+pkey+", Package not enabled: "+pk);

            conn.executeUpdate(
                "update email_smtp_relays set disable_log=null where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.EMAIL_SMTP_RELAYS,
                PackageHandler.getBusinessForPackage(conn, pk),
                getAOServerForEmailSmtpRelay(conn, pkey),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getDisableLogForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from email_lists where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForEmailPipe(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getDisableLogForEmailPipe(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from email_pipes where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForEmailSmtpRelay(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getDisableLogForEmailSmtpRelay(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from email_smtp_relays where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getEmailListAddressList(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailListAddressList(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessEmailList(conn, source, "getEmailListAddressList", pkey);
            return DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).getEmailListFile(
                getPathForEmailList(conn, pkey)
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getEmailListsForLinuxServerAccount(
        MasterDatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailListsForLinuxServerAccount(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntListQuery("select pkey from email_lists where linux_server_account=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getEmailListsForPackage(
        MasterDatabaseConnection conn,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailListsForPackage(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                "select\n"
                + "  el.pkey\n"
                + "from\n"
                + "  linux_groups lg,\n"
                + "  linux_server_groups lsg,\n"
                + "  email_lists el\n"
                + "where\n"
                + "  lg.package=?\n"
                + "  and lg.name=lsg.name\n"
                + "  and lsg.pkey=el.linux_server_group",
                name
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getEmailPipesForPackage(
        MasterDatabaseConnection conn,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailPipesForPackage(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery("select pkey from email_pipes where package=?", name);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static long[] getImapFolderSizes(
        MasterDatabaseConnection conn,
        RequestSource source,
        int linux_server_account,
        String[] folderNames
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getImapFolderSizes(MasterDatabaseConnection,RequestSource,int,String[])", null);
        try {
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "getImapFolderSizes", linux_server_account);
            int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linux_server_account);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linux_server_account);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getImapFolderSizes(username, folderNames);
                } catch(IOException err) {
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "linux_server_account="+linux_server_account,
                            "aoServer="+aoServer,
                            "username="+username,
                            "folderNames="+folderNames
                        }
                    );
                    DaemonHandler.flagDaemonAsDown(aoServer);
                }
            }
            long[] sizes=new long[folderNames.length];
            Arrays.fill(sizes, -1);
            return sizes;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setImapFolderSubscribed(
        MasterDatabaseConnection conn,
        RequestSource source,
        int linux_server_account,
        String folderName,
        boolean subscribed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "setImapFolderSubscribed(MasterDatabaseConnection,RequestSource,int,String,boolean)", null);
        try {
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "setImapFolderSubscribed", linux_server_account);
            int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linux_server_account);
            String username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linux_server_account);
            DaemonHandler.getDaemonConnector(conn, aoServer).setImapFolderSubscribed(username, folderName, subscribed);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static InboxAttributes getInboxAttributes(
        MasterDatabaseConnection conn,
        RequestSource source,
        int linux_server_account
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getInboxAttributes(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "getInboxAttributes", linux_server_account);
            int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linux_server_account);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linux_server_account);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getInboxAttributes(username);
                } catch(IOException err) {
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "linux_server_account="+linux_server_account,
                            "aoServer="+aoServer,
                            "username="+username
                        }
                    );
                    DaemonHandler.flagDaemonAsDown(aoServer);
                }
            }
            return null;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getEmailSmtpRelaysForPackage(
        MasterDatabaseConnection conn,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailSmtpRelaysForPackage(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery("select pkey from email_smtp_relays where package=?", name);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getEmailDomain(
        MasterDatabaseConnection conn,
        int aoServer,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailDomain(MasterDatabaseConnection,int,String)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  el.pkey\n"
                + "from\n"
                + "  email_lists el,\n"
                + "  linux_server_groups lsg\n"
                + "where\n"
                + "  el.path=path\n"
                + "  and el.linux_server_group=lsg.pkey\n"
                + "  and lsg.ao_server=?",
                path,
                aoServer
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getEmailDomainForEmailAddress(
        MasterDatabaseConnection conn,
        int emailAddress
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailDomainForEmailAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select domain from email_addresses where pkey=?", emailAddress);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getMajordomoInfoFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getMajordomoInfoFile(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessEmailList(conn, source, "getMajordomoInfoFile", pkey);

            return DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).getEmailListFile(
                getPathForEmailList(conn, pkey)+".info"
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getMajordomoIntroFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getMajordomoIntroFile(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            checkAccessEmailList(conn, source, "getMajordomoIntroFile", pkey);

            return DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).getEmailListFile(
                getPathForEmailList(conn, pkey)+".intro"
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getMajordomoServer(
        MasterDatabaseConnection conn,
        int emailDomain
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getMajordomoServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  pkey\n"
                + "from\n"
                + "  majordomo_servers\n"
                + "where\n"
                + "  domain=?",
                emailDomain
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getSpamEmailMessagesForEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source, 
        CompressedDataOutputStream out,
        boolean provideProgress,
        int esr
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getSpamEmailMessagesForEmailSmtpRelay(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            if(masterUser!=null && masterServers.length==0) MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
                new SpamEmailMessage(),
                "select * from spam_email_messages where email_relay=?",
                esr
            ); else throw new SQLException("Only master users may access spam_email_messages.");
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
            if(tableID==SchemaTable.TableID.EMAIL_LISTS) {
                synchronized(EmailHandler.class) {
                    disabledEmailLists.clear();
                }
            } else if(tableID==SchemaTable.TableID.EMAIL_PIPES) {
                synchronized(EmailHandler.class) {
                    disabledEmailPipes.clear();
                }
            } else if(tableID==SchemaTable.TableID.EMAIL_SMTP_RELAYS) {
                synchronized(EmailHandler.class) {
                    disabledEmailSmtpRelays.clear();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isEmailListDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isEmailListDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(EmailHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledEmailLists.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForEmailList(conn, pkey)!=-1;
		disabledEmailLists.put(I, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isEmailPipeDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isEmailPipeDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(EmailHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledEmailPipes.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForEmailPipe(conn, pkey)!=-1;
		disabledEmailPipes.put(I, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isEmailSmtpRelayDisabled(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isEmailSmtpRelayDisabled(MasterDatabaseConnection,int)", null);
        try {
	    synchronized(EmailHandler.class) {
		Integer I=Integer.valueOf(pkey);
		Boolean O=disabledEmailSmtpRelays.get(I);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForEmailSmtpRelay(conn, pkey)!=-1;
		disabledEmailSmtpRelays.put(I, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Refreshes a email SMTP relay.
     */
    public static void refreshEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        long minDuration
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "refreshEmailSmtpRelay(MasterDatabaseConnection,RequestSource,InvalidateList,int,long)", null);
        try {
            checkAccessEmailSmtpRelay(conn, source, "refreshEmailSmtpRelay", pkey);

            if(isEmailSmtpRelayDisabled(conn, pkey)) throw new SQLException("Unable to refresh EmailSmtpRelay, EmailSmtpRelay disabled: "+pkey);

            String packageName=getPackageForEmailSmtpRelay(conn, pkey);
            String accounting=PackageHandler.getBusinessForPackage(conn, packageName);
            int aoServer=getAOServerForEmailSmtpRelay(conn, pkey);
            Timestamp expiration=conn.executeTimestampQuery("select expiration from email_smtp_relays where pkey=?", pkey);
            long exp=expiration==null?EmailSmtpRelay.NO_EXPIRATION:expiration.getTime();
            long min=minDuration==EmailSmtpRelay.NO_EXPIRATION?EmailSmtpRelay.NO_EXPIRATION:(System.currentTimeMillis()+minDuration);
            conn.executeUpdate(
                "update email_smtp_relays set last_refreshed=now(), refresh_count=refresh_count+1, expiration=? where pkey=?",
                exp==EmailSmtpRelay.NO_EXPIRATION || min==EmailSmtpRelay.NO_EXPIRATION
                ? null
                : new Timestamp(Math.max(exp, min)),
                pkey
            );

            // Delete any old entries
            conn.executeUpdate(
                "delete from email_smtp_relays where package=? and (ao_server is null or ao_server=?) and expiration is not null and now()::date-expiration::date>"+EmailSmtpRelay.HISTORY_DAYS,
                packageName,
                aoServer
            );

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_SMTP_RELAYS, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeBlackholeEmailAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int bea
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeBlackholeEmailAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailAddress(conn, source, "removeBlackholeEmailAddress", bea);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, bea);
            int aoServer=getAOServerForEmailAddress(conn, bea);

            // Delete from the database
            conn.executeUpdate("delete from blackhole_email_addresses where email_address=?", bea);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES,
                accounting,
                aoServer,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int address
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailAddress(conn, source, "removeEmailAddress", address);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, address);
            int aoServer=getAOServerForEmailAddress(conn, address);

            // Delete the objects that depend on this one first
            boolean isBlackhole=conn.executeBooleanQuery("select (select email_address from blackhole_email_addresses where email_address=?) is not null", address);
            if(isBlackhole) conn.executeUpdate("delete from blackhole_email_addresses where email_address=?", address);

            IntList pkeys=conn.executeIntListQuery("select pkey from linux_acc_addresses where email_address=?", address);
            boolean isLinuxAccAddress=pkeys.size()>0;
            if(isLinuxAccAddress) {
                for(int d=0;d<pkeys.size();d++) {
                    int laaPkey=pkeys.getInt(d);
                    conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laaPkey);
                    conn.executeUpdate("delete from linux_acc_addresses where pkey=?", laaPkey);
                }
            }

            boolean isEmailForwarding=conn.executeIntQuery("select count(*) from email_forwarding where email_address=?", address)>0;
            if(isEmailForwarding) conn.executeUpdate("delete from email_forwarding where email_address=?", address);
            boolean isEmailListAddress=conn.executeIntQuery("select count(*) from email_list_addresses where email_address=?", address)>0;
            if(isEmailListAddress) conn.executeUpdate("delete from email_list_addresses where email_address=?", address);
            boolean isEmailPipeAddress=conn.executeIntQuery("select count(*) from email_pipe_addresses where email_address=?", address)>0;
            if(isEmailPipeAddress) conn.executeUpdate("delete from email_pipe_addresses where email_address=?", address);

            // Delete from the database
            conn.executeUpdate("delete from email_addresses where pkey=?", address);

            // Notify all clients of the update
            if(isBlackhole) invalidateList.addTable(conn, SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES, accounting, aoServer, false);
            if(isLinuxAccAddress) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
            if(isEmailForwarding) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
            if(isEmailListAddress) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
            if(isEmailPipeAddress) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailForwarding(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ef
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailForwarding(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int ea=conn.executeIntQuery("select email_address from email_forwarding where pkey=?", ef);
            checkAccessEmailAddress(conn, source, "removeEmailForwarding", ea);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, ea);
            int aoServer=getAOServerForEmailAddress(conn, ea);

            // Delete from the database
            conn.executeUpdate("delete from email_forwarding where pkey=?", ef);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailListAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ela
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailListAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int ea=conn.executeIntQuery("select email_address from email_list_addresses where pkey=?", ela);
            checkAccessEmailAddress(conn, source, "removeEmailListAddress", ea);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, ea);
            int aoServer=getAOServerForEmailAddress(conn, ea);

            // Delete from the database
            conn.executeUpdate("delete from email_list_addresses where pkey=?", ela);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailList(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailList(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailList(conn, source, "removeEmailList", pkey);

            removeEmailList(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailList(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailList(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Get the values for later use
            String accounting=getBusinessForEmailList(conn, pkey);
            int aoServer=getAOServerForEmailList(conn, pkey);
            String path=conn.executeStringQuery("select path from email_lists where pkey=?", pkey);

            // Delete the majordomo_list that is attached to this email list
            if(isMajordomoList(conn, pkey)) {
                // Get the listname_pipe_add and details
                int listnameEPA=conn.executeIntQuery("select listname_pipe_add from majordomo_lists where email_list=?", pkey);
                int listnameEA=conn.executeIntQuery("select email_address from email_pipe_addresses where pkey=?", listnameEPA);
                int listnameEP=conn.executeIntQuery("select email_pipe from email_pipe_addresses where pkey=?", listnameEPA);

                // Get the listname_list_add and details
                int listnameListELA=conn.executeIntQuery("select listname_list_add from majordomo_lists where email_list=?", pkey);
                int listnameListEA=conn.executeIntQuery("select email_address from email_list_addresses where pkey=?", listnameListELA);

                // Get the listname_request_pipe_add and details
                int listnameRequestEPA=conn.executeIntQuery("select listname_request_pipe_add from majordomo_lists where email_list=?", pkey);
                int listnameRequestEA=conn.executeIntQuery("select email_address from email_pipe_addresses where pkey=?", listnameRequestEPA);
                int listnameRequestEP=conn.executeIntQuery("select email_pipe from email_pipe_addresses where pkey=?", listnameRequestEPA);

                // Other direct email addresses
                int ownerListnameEA=conn.executeIntQuery("select owner_listname_add from majordomo_lists where email_list=?", pkey);
                int listnameOwnerEA=conn.executeIntQuery("select listname_owner_add from majordomo_lists where email_list=?", pkey);
                int listnameApprovalEA=conn.executeIntQuery("select listname_approval_add from majordomo_lists where email_list=?", pkey);

                conn.executeUpdate("delete from majordomo_lists where email_list=?", pkey);
                invalidateList.addTable(conn, SchemaTable.TableID.MAJORDOMO_LISTS, accounting, aoServer, false);
                
                // Delete the listname_pipe_add
                conn.executeUpdate("delete from email_pipe_addresses where pkey=?", listnameEPA);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
                if(!isEmailAddressUsed(conn, listnameEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", listnameEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }
                conn.executeUpdate("delete from email_pipes where pkey=?", listnameEP);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);
                
                // Delete the listname_list_add
                conn.executeUpdate("delete from email_list_addresses where pkey=?", listnameListELA);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
                if(!isEmailAddressUsed(conn, listnameListEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", listnameListEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }

                // Delete the listname_pipe_add
                conn.executeUpdate("delete from email_pipe_addresses where pkey=?", listnameRequestEPA);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
                if(!isEmailAddressUsed(conn, listnameRequestEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", listnameRequestEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }
                conn.executeUpdate("delete from email_pipes where pkey=?", listnameRequestEP);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);

                // Other direct email addresses
                if(!isEmailAddressUsed(conn, ownerListnameEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", ownerListnameEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }
                if(!isEmailAddressUsed(conn, listnameOwnerEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", listnameOwnerEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }
                if(!isEmailAddressUsed(conn, listnameApprovalEA)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", listnameApprovalEA);
                    invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
                }
            }

            // Delete the objects that depend on this one first
            IntList addresses=conn.executeIntListQuery("select email_address from email_list_addresses where email_list=?", pkey);
            int size=addresses.size();
            boolean addressesModified=size>0;
            for(int c=0;c<size;c++) {
                int address=addresses.getInt(c);
                conn.executeUpdate("delete from email_list_addresses where email_address=? and email_list=?", address, pkey);
                if(!isEmailAddressUsed(conn, address)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", address);
                }
            }

            // Delete from the database
            conn.executeUpdate("delete from email_lists where pkey=?", pkey);

            // Notify all clients of the update
            if(addressesModified) {
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            }
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LISTS, accounting, aoServer, false);

            // Remove the list file from the server
            DaemonHandler.getDaemonConnector(conn, aoServer).removeEmailList(path);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeLinuxAccAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int laa
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeLinuxAccAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int ea=conn.executeIntQuery("select email_address from linux_acc_addresses where pkey=?", laa);
            checkAccessEmailAddress(conn, source, "removeLinuxAccAddress", ea);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, ea);
            int aoServer=getAOServerForEmailAddress(conn, ea);

            // Delete from the database
            conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laa);
            conn.executeUpdate("delete from linux_acc_addresses where pkey=?", laa);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailPipe(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailPipe(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailPipe(conn, source, "removeEmailPipe", pkey);
            
            removeEmailPipe(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailPipe(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailPipe(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            // Get the values for later use
            String accounting=getBusinessForEmailPipe(conn, pkey);
            int aoServer=getAOServerForEmailPipe(conn, pkey);

            // Delete the objects that depend on this one first
            IntList addresses=conn.executeIntListQuery("select email_address from email_pipe_addresses where email_pipe=?", pkey);
            int size=addresses.size();
            boolean addressesModified=size>0;
            for(int c=0;c<size;c++) {
                int address=addresses.getInt(c);
                conn.executeUpdate("delete from email_pipe_addresses where email_address=? and email_pipe=?", address, pkey);
                if(!isEmailAddressUsed(conn, address)) {
                    conn.executeUpdate("delete from email_addresses where pkey=?", address);
                }
            }

            // Delete from the database
            conn.executeUpdate("delete from email_pipes where pkey=?", pkey);

            // Notify all clients of the update
            if(addressesModified) {
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            }
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailPipeAddress(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int epa
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailPipeAddress(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int ea=conn.executeIntQuery("select email_address from email_pipe_addresses where pkey=?", epa);
            checkAccessEmailAddress(conn, source, "removeEmailPipeAddress", ea);

            // Get stuff for use after the try block
            String accounting=getBusinessForEmailAddress(conn, ea);
            int aoServer=getAOServerForEmailAddress(conn, ea);

            // Delete from the database
            conn.executeUpdate("delete from email_pipe_addresses where pkey=?", epa);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailDomain(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailDomain(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailDomain(conn, source, "removeEmailDomain", pkey);

            removeEmailDomain(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeEmailDomain(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailDomain(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            boolean
                beaMod=false,
                laaMod=false,
                efMod=false,
                elaMod=false,
                epaMod=false
            ;
            String accounting=getBusinessForEmailDomain(conn, pkey);
            int aoServer=getAOServerForEmailDomain(conn, pkey);

            // Remove any majordomo server
            int ms=conn.executeIntQuery("select coalesce((select domain from majordomo_servers where domain=?), -1)", pkey);
            if(ms!=-1) removeMajordomoServer(conn, invalidateList, pkey);

            // Get the list of all email addresses in the domain
            IntList addresses=conn.executeIntListQuery("select pkey from email_addresses where domain=?", pkey);

            int len=addresses.size();
            boolean eaMod=len>0;
            for(int c=0;c<len;c++) {
                int address=addresses.getInt(c);

                if(
                    conn.executeBooleanQuery(
                        "select (select email_address from blackhole_email_addresses where email_address=?) is not null",
                        address
                    )
                ) {
                    conn.executeUpdate("delete from blackhole_email_addresses where email_address=?", address);
                    beaMod=true;
                }

                // Delete any linux_acc_addresses used by this email address
		IntList pkeys=conn.executeIntListQuery("select pkey from linux_acc_addresses where email_address=?", address);
		if(pkeys.size()>0) {
		    for(int d=0;d<pkeys.size();d++) {
			int laaPkey=pkeys.getInt(d);
			conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laaPkey);
			conn.executeUpdate("delete from linux_acc_addresses where pkey=?", laaPkey);
		    }
                    laaMod=true;
                }

                if(
                    conn.executeBooleanQuery(
                        "select (select pkey from email_forwarding where email_address=? limit 1) is not null",
                        address
                    )
                ) {
                    conn.executeUpdate("delete from email_forwarding where email_address=?", address);
                    efMod=true;
                }


                if(
                    conn.executeBooleanQuery(
                        "select (select pkey from email_list_addresses where email_address=? limit 1) is not null",
                        address
                    )
                ) {
                    conn.executeUpdate("delete from email_list_addresses where email_address=?", address);
                    elaMod=true;
                }

                if(
                    conn.executeBooleanQuery(
                        "select (select pkey from email_pipe_addresses where email_address=? limit 1) is not null",
                        address
                    )
                ) {
                    conn.executeUpdate("delete from email_pipe_addresses where email_address=?", address);
                    epaMod=true;
                }

                // Delete from the database
                conn.executeUpdate("delete from email_addresses where pkey=?", address);
            }

            // Remove the domain from the database
            conn.executeUpdate("delete from email_domains where pkey=?", pkey);

            // Notify all clients of the update
            if(beaMod) invalidateList.addTable(conn, SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES, accounting, aoServer, false);
            if(laaMod) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
            if(efMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
            if(elaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
            if(epaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
            if(eaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_DOMAINS, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a email SMTP relay.
     */
    public static void removeEmailSmtpRelay(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "removeEmailSmtpRelay(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessEmailSmtpRelay(conn, source, "removeEmailSmtpRelay", pkey);

            removeEmailSmtpRelay(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Removes a email SMTP relay.
     */
    public static void removeEmailSmtpRelay(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeEmailSmtpRelay(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=getBusinessForEmailSmtpRelay(conn, pkey);
            int aoServer=getAOServerForEmailSmtpRelay(conn, pkey);

            conn.executeUpdate(
                "delete from email_smtp_relays where pkey=?",
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_SMTP_RELAYS, accounting, aoServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeMajordomoServer(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int domain
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, EmailHandler.class, "removeMajordomoServer(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessMajordomoServer(conn, source, "removeMajordomoServer", domain);

            removeMajordomoServer(conn, invalidateList, domain);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void removeMajordomoServer(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int domain
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "removeMajordomoServer(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=getBusinessForEmailDomain(conn, domain);
            int aoServer=getAOServerForEmailDomain(conn, domain);

            // Remove any majordomo lists
            IntList mls=conn.executeIntListQuery("select email_list from majordomo_lists where majordomo_server=?", domain);
            if(mls.size()>0) {
                for(int c=0;c<mls.size();c++) {
                    removeEmailList(conn, invalidateList, mls.getInt(c));
                }
            }

            // Get the majordomo_pipe_address and details
            int epa=conn.executeIntQuery("select majordomo_pipe_address from majordomo_servers where domain=?", domain);
            int ea=conn.executeIntQuery("select email_address from email_pipe_addresses where pkey=?", epa);
            int ep=conn.executeIntQuery("select email_pipe from email_pipe_addresses where pkey=?", epa);

            // Get the other email addresses referenced
            int omEA=conn.executeIntQuery("select owner_majordomo_add from majordomo_servers where domain=?", domain);
            int moEA=conn.executeIntQuery("select majordomo_owner_add from majordomo_servers where domain=?", domain);

            // Remove the domain from the database
            conn.executeUpdate("delete from majordomo_servers where domain=?", domain);
            invalidateList.addTable(conn, SchemaTable.TableID.MAJORDOMO_SERVERS, accounting, aoServer, false);
            
            // Remove the majordomo pipe and address
            conn.executeUpdate("delete from email_pipe_addresses where pkey=?", epa);
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
            if(!isEmailAddressUsed(conn, ea)) {
                conn.executeUpdate("delete from email_addresses where pkey=?", ea);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            }
            conn.executeUpdate("delete from email_pipes where pkey=?", ep);
            invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);
            
            // Remove the referenced email addresses if not used
            if(!isEmailAddressUsed(conn, omEA)) {
                conn.executeUpdate("delete from email_addresses where pkey=?", omEA);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            }
            if(!isEmailAddressUsed(conn, moEA)) {
                conn.executeUpdate("delete from email_addresses where pkey=?", moEA);
                invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setEmailListAddressList(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey,
        String addresses
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "setEmailListAddressList(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            checkAccessEmailList(conn, source, "setEmailListAddressList", pkey);
            DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).setEmailListFile(
                getPathForEmailList(conn, pkey),
                addresses,
                LinuxAccountHandler.getUIDForLinuxServerAccount(conn, getLinuxServerAccountForEmailList(conn, pkey)),
                LinuxAccountHandler.getGIDForLinuxServerGroup(conn, getLinuxServerGroupForEmailList(conn, pkey)),
                isMajordomoList(conn, pkey)?0644:0640
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEmailAddress(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getBusinessForEmailAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from email_addresses ea, email_domains sd, packages pk where ea.domain=sd.pkey and sd.package=pk.name and ea.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getBusinessForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from email_lists el, linux_server_groups lsg, linux_groups lg, packages pk where el.linux_server_group=lsg.pkey and lsg.name=lg.name and lg.package=pk.name and el.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEmailPipe(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getBusinessForEmailPipe(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from email_pipes ep, packages pk where ep.package=pk.name and ep.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEmailDomain(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getBusinessForEmailDomain(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from email_domains sd, packages pk where sd.package=pk.name and sd.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getDomainForEmailDomain(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getDomainForEmailDomain(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select domain from email_domains where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForEmailSmtpRelay(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getBusinessForEmailSmtpRelay(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select pk.accounting from email_smtp_relays esr, packages pk where esr.package=pk.name and esr.pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getEmailAddress(MasterDatabaseConnection conn, String address, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getEmailAddress(MasterDatabaseConnection,String,int)", null);
        try {
            return conn.executeIntQuery(
                "select coalesce((select pkey from email_addresses where address=? and domain=?), -1)",
                address,
                domain
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getOrAddEmailAddress(MasterDatabaseConnection conn, InvalidateList invalidateList, String address, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getOrAddEmailAddress(MasterDatabaseConnection,InvalidateList,String,int)", null);
        try {
            int pkey=getEmailAddress(conn, address, domain);
            if(pkey==-1) pkey=addEmailAddress0(conn, invalidateList, address, domain);
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getLinuxServerAccountForMajordomoServer(MasterDatabaseConnection conn, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getLinuxServerAccountForMajordomoServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select linux_server_account from majordomo_servers where domain=?", domain);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getLinuxServerGroupForMajordomoServer(MasterDatabaseConnection conn, int domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getLinuxServerGroupForMajordomoServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select linux_server_group from majordomo_servers where domain=?", domain);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForEmailDomain(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getPackageForEmailDomain(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select package from email_domains where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getPackageForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(
                "select\n"
                + "  lg.package\n"
                + "from\n"
                + "  email_lists el,\n"
                + "  linux_server_groups lsg,\n"
                + "  linux_groups lg\n"
                + "where\n"
                + "  el.pkey=?\n"
                + "  and el.linux_server_group=lsg.pkey\n"
                + "  and lsg.name=lg.name",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForEmailPipe(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getPackageForEmailPipe(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select package from email_pipes where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPackageForEmailSmtpRelay(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getPackageForEmailSmtpRelay(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select package from email_smtp_relays where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getPathForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getPathForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select path from email_lists where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForEmailAddress(MasterDatabaseConnection conn, int address) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getAOServerForEmailAddress(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ed.ao_server from email_addresses ea, email_domains ed where ea.domain=ed.pkey and ea.pkey=?", address);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getAOServerForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery(
                "select\n"
                + "  lsg.ao_server\n"
                + "from\n"
                + "  email_lists el,\n"
                + "  linux_server_groups lsg\n"
                + "where\n"
                + "  el.pkey=?\n"
                + "  and el.linux_server_group=lsg.pkey",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getLinuxServerAccountForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getLinuxServerAccountForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select linux_server_account from email_lists where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getLinuxServerGroupForEmailList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getLinuxServerGroupForEmailList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select linux_server_group from email_lists where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isEmailAddressUsed(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isEmailAddressUsed(MasterDatabaseConnection,int)", null);
        try {
            return
                conn.executeBooleanQuery("select (select email_address from blackhole_email_addresses where email_address=? limit 1) is not null", pkey)
                || conn.executeBooleanQuery("select (select pkey from email_forwarding where email_address=? limit 1) is not null", pkey)
                || conn.executeBooleanQuery("select (select pkey from email_list_addresses where email_address=? limit 1) is not null", pkey)
                || conn.executeBooleanQuery("select (select pkey from email_pipe_addresses where email_address=? limit 1) is not null", pkey)
                || conn.executeBooleanQuery("select (select pkey from linux_acc_addresses where email_address=? limit 1) is not null", pkey)
                || conn.executeBooleanQuery(
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      ml.email_list\n"
                    + "    from\n"
                    + "      majordomo_lists ml,\n"
                    + "      email_pipe_addresses epa1,\n"
                    + "      email_list_addresses ela,\n"
                    + "      email_pipe_addresses epa2\n"
                    + "    where\n"
                    + "      ml.listname_pipe_add=epa1.pkey\n"
                    + "      and ml.listname_list_add=ela.pkey\n"
                    + "      and ml.listname_request_pipe_add=epa2.pkey\n"
                    + "      and (\n"
                    + "        epa1.email_address=?\n"
                    + "        or ela.email_address=?\n"
                    + "        or ml.owner_listname_add=?\n"
                    + "        or ml.listname_owner_add=?\n"
                    + "        or ml.listname_approval_add=?\n"
                    + "        or epa2.email_address=?\n"
                    + "      )\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    pkey,
                    pkey,
                    pkey,
                    pkey,
                    pkey,
                    pkey
                ) || conn.executeBooleanQuery(
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      ms.domain\n"
                    + "    from\n"
                    + "      majordomo_servers ms,\n"
                    + "      email_pipe_addresses epa\n"
                    + "    where\n"
                    + "      ms.majordomo_pipe_address=epa.pkey\n"
                    + "      and (\n"
                    + "        epa.email_address=?\n"
                    + "        or ms.owner_majordomo_add=?\n"
                    + "        or ms.majordomo_owner_add=?\n"
                    + "      )\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    pkey,
                    pkey,
                    pkey
                )
            ;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForEmailPipe(MasterDatabaseConnection conn, int pipe) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getAOServerForEmailPipe(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from email_pipes where pkey=?", pipe);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForEmailDomain(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getAOServerForEmailDomain(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from email_domains where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForEmailSmtpRelay(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "getAOServerForEmailSmtpRelay(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from email_smtp_relays where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isEmailDomainAvailable(MasterDatabaseConnection conn, RequestSource source, int ao_server, String domain) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isEmailDomainAvailable(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            ServerHandler.checkAccessServer(conn, source, "isEmailDomainAvailable", ao_server);

            return conn.executeBooleanQuery(
                "select (select pkey from email_domains where ao_server=? and domain=?) is null",
                ao_server,
                domain
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static boolean isMajordomoList(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "isMajordomoList(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeBooleanQuery("select (select email_list from majordomo_lists where email_list=?) is not null", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setMajordomoInfoFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey,
        String file
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "setMajordomoInfoFile(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            checkAccessEmailList(conn, source, "setMajordomoInfoFile", pkey);
            DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).setEmailListFile(
                getPathForEmailList(conn, pkey)+".info",
                file,
                LinuxAccountHandler.getUIDForLinuxServerAccount(conn, getLinuxServerAccountForEmailList(conn, pkey)),
                LinuxAccountHandler.getGIDForLinuxServerGroup(conn, getLinuxServerGroupForEmailList(conn, pkey)),
                0664
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setMajordomoIntroFile(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey,
        String file
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, EmailHandler.class, "setMajordomoIntroFile(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            checkAccessEmailList(conn, source, "setMajordomoIntroFile", pkey);
            DaemonHandler.getDaemonConnector(
                conn,
                getAOServerForEmailList(conn, pkey)
            ).setEmailListFile(
                getPathForEmailList(conn, pkey)+".intro",
                file,
                LinuxAccountHandler.getUIDForLinuxServerAccount(conn, getLinuxServerAccountForEmailList(conn, pkey)),
                LinuxAccountHandler.getGIDForLinuxServerGroup(conn, getLinuxServerGroupForEmailList(conn, pkey)),
                0664
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private EmailHandler() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, EmailHandler.class, "<init>()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }
}
