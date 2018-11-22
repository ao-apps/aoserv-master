/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.EmailList;
import com.aoindustries.aoserv.client.EmailSmtpRelay;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxAccountType;
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.LinuxGroupType;
import com.aoindustries.aoserv.client.MajordomoList;
import com.aoindustries.aoserv.client.MajordomoServer;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.SpamEmailMessage;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.Email;
import com.aoindustries.net.HostAddress;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import com.aoindustries.validation.ValidationResult;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>EmailHandler</code> handles all the accesses to the email tables.
 *
 * @author  AO Industries, Inc.
 */
final public class EmailHandler {

	private static final Logger logger = LogFactory.getLogger(EmailHandler.class);

	private EmailHandler() {
	}

	private final static Map<Integer,Boolean> disabledEmailLists=new HashMap<>();
	private final static Map<Integer,Boolean> disabledEmailPipes=new HashMap<>();
	private final static Map<Integer,Boolean> disabledEmailSmtpRelays=new HashMap<>();

	public static boolean canAccessEmailDomain(DatabaseConnection conn, RequestSource source, int domain) throws IOException, SQLException {
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
	}

	public static void checkAccessEmailDomain(DatabaseConnection conn, RequestSource source, String action, int domain) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailDomain(conn, domain));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailDomain(conn, domain));
		}
	}

	public static void checkAccessEmailSmtpRelay(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailSmtpRelay(conn, pkey));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailSmtpRelay(conn, pkey));
		}
	}

	public static void checkAccessEmailAddress(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		checkAccessEmailDomain(conn, source, action, getEmailDomainForEmailAddress(conn, pkey));
	}

	public static void checkAccessEmailList(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, action, getLinuxServerGroupForEmailList(conn, pkey));
	}

	public static void checkAccessEmailListPath(DatabaseConnection conn, RequestSource source, String action, int aoServer, UnixPath path) throws IOException, SQLException {
		if(
			!EmailList.isValidRegularPath(
				path,
				ServerHandler.getOperatingSystemVersionForServer(conn, aoServer)
			)
		) {
			String pathStr = path.toString();
			// Can also be a path in a majordomo server that they may access
			if(pathStr.startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString()+'/')) {
				int pos=pathStr.indexOf('/', MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString().length()+1);
				if(pos!=-1) {
					String domain=pathStr.substring(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString().length()+1, pos);
					pathStr=pathStr.substring(pos+1);
					if(pathStr.startsWith("lists/")) {
						String listName=pathStr.substring(6);
						if(MajordomoList.isValidListName(listName)) {
							int ed=getEmailDomain(conn, aoServer, domain);
							checkAccessMajordomoServer(conn, source, action, getMajordomoServer(conn, ed));
							return;
						}
					}
				}
			}
			String message="email.List.path="+path+" not allowed, '"+action+"'";
			throw new SQLException(message);
		}
	}

	public static void checkAccessEmailPipe(DatabaseConnection conn, RequestSource source, String action, int pipe) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForEmailPipe(conn, pipe));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForEmailPipe(conn, pipe));
		}
	}

	public static void checkAccessEmailPipeCommand(
		DatabaseConnection conn,
		RequestSource source,
		String action,
		String path
	) throws IOException, SQLException {
		throw new SQLException("Method not implemented.");
	}

	public static void checkAccessMajordomoServer(DatabaseConnection conn, RequestSource source, String action, int majordomoServer) throws IOException, SQLException {
		checkAccessEmailDomain(conn, source, action, majordomoServer);
	}

	public static int addEmailAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		String address, 
		int domain
	) throws IOException, SQLException {
		checkAccessEmailDomain(conn, source, "addEmailAddress", domain);

		return addEmailAddress0(conn, invalidateList, address, domain);
	}

	private static int addEmailAddress0(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		String address, 
		int domain
	) throws IOException, SQLException {
		{
			ValidationResult result = Email.validate(
				address,
				getDomainForEmailDomain(conn, domain)
			);
			if(!result.isValid()) throw new SQLException("Invalid email address: " + result);
		}

		int pkey = conn.executeIntUpdate(
			"INSERT INTO email.\"Address\" (address, \"domain\") VALUES (?,?) RETURNING pkey",
			address,
			domain
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_ADDRESSES,
			getBusinessForEmailAddress(conn, pkey),
			getAOServerForEmailAddress(conn, pkey),
			false
		);
		return pkey;
	}

	public static int addEmailForwarding(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int address, 
		Email destination
	) throws IOException, SQLException {
		String destinationStr = destination.toString();
		if(destinationStr.toLowerCase().endsWith("@comcast.net")) throw new SQLException(
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

		int pkey = conn.executeIntUpdate(
			"INSERT INTO email.\"Forwarding\" (email_address, destination) VALUES (?,?) RETURNING pkey",
			address,
			destination
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_FORWARDING,
			getBusinessForEmailAddress(conn, address),
			getAOServerForEmailAddress(conn, address),
			false
		);

		return pkey;
	}

	public static int addEmailList(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		UnixPath path,
		int linuxServerAccount,
		int linuxServerGroup
	) throws IOException, SQLException {
		checkAccessEmailListPath(conn, source, "addEmailList", LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linuxServerAccount), path);

		// Allow the mail user
		UserId username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linuxServerAccount);
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
	}
	private static int addEmailList0(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		UnixPath path,
		int linuxServerAccount,
		int linuxServerGroup
	) throws IOException, SQLException {
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, linuxServerAccount)) throw new SQLException("Unable to add EmailList, LinuxServerAccount disabled: "+linuxServerAccount);
		AccountingCode packageName=LinuxAccountHandler.getPackageForLinuxServerGroup(conn, linuxServerGroup);
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
				+ "      email.\"List\" el,\n"
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

		int pkey = conn.executeIntUpdate(
			"INSERT INTO email.\"List\" (\n"
			+ "  \"path\",\n"
			+ "  linux_server_account,\n"
			+ "  linux_server_group\n"
			+ "values(\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ") RETURNING pkey",
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
			path.toString().startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString()+'/')?0644:0640
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
	}

	public static int addEmailListAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int address, 
		int email_list
	) throws IOException, SQLException {
		checkAccessEmailAddress(conn, source, "addEmailListAddress", address);
		checkAccessEmailList(conn, source, "addEmailListAddress", email_list);

		return addEmailListAddress0(
			conn,
			invalidateList,
			address,
			email_list
		);
	}

	private static int addEmailListAddress0(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int address, 
		int email_list
	) throws IOException, SQLException {
		// The email_domain and the email_list must be on the same server
		int domainAOServer=getAOServerForEmailAddress(conn, address);
		int listServer=getAOServerForEmailList(conn, email_list);
		if(domainAOServer!=listServer) throw new SQLException("List server ("+listServer+")!=Email address server ("+domainAOServer+')');

		int pkey = conn.executeIntUpdate(
			"INSERT INTO email.\"ListAddress\" (email_address, email_list) VALUES (?,?) RETURNING pkey",
			address,
			email_list
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_LIST_ADDRESSES,
			getBusinessForEmailAddress(conn, address),
			getAOServerForEmailAddress(conn, address),
			false
		);

		return pkey;
	}

	/**
	 * Adds an email pipe.
	 */
	public static int addEmailPipe(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		String command,
		AccountingCode packageName
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "addEmailPipe", aoServer);
		checkAccessEmailPipeCommand(conn, source, "addEmailPipe", command);
		PackageHandler.checkAccessPackage(conn, source, "addEmailPipe", packageName);
		PackageHandler.checkPackageAccessServer(conn, source, "addEmailPipe", packageName, aoServer);

		return addEmailPipe0(
			conn,
			invalidateList,
			aoServer,
			command,
			packageName
		);
	}

	private static int addEmailPipe0(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int aoServer,
		String command,
		AccountingCode packageName
	) throws IOException, SQLException {
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add EmailPipe, Package disabled: "+packageName);

		int pkey = conn.executeIntUpdate("INSERT INTO email.\"Pipe\" VALUES (default,?,?,?,null) RETURNING pkey", aoServer, command, packageName);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_PIPES,
			PackageHandler.getBusinessForPackage(conn, packageName),
			aoServer,
			false
		);
		return pkey;
	}

	public static int addEmailPipeAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int address, 
		int pipe
	) throws IOException, SQLException {
		checkAccessEmailAddress(conn, source, "addEmailPipeAddress", address);
		checkAccessEmailPipe(conn, source, "addEmailPipeAddress", pipe);

		return addEmailPipeAddress0(
			conn,
			invalidateList,
			address,
			pipe
		);
	}

	private static int addEmailPipeAddress0(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int address, 
		int pipe
	) throws IOException, SQLException {
		int pkey = conn.executeIntUpdate("INSERT INTO email.\"PipeAddress\" VALUES (default,?,?) RETURNING pkey", address, pipe);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_PIPE_ADDRESSES,
			getBusinessForEmailAddress(conn, address),
			getAOServerForEmailAddress(conn, address),
			false
		);

		return pkey;
	}

	public static int addLinuxAccAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int address, 
		int lsa
	) throws IOException, SQLException {
		checkAccessEmailAddress(conn, source, "addLinuxAccAddress", address);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addLinuxAccAddress", lsa);
		UserId username = LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add email addresses to LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		// TODO: Make sure they are on the same server

		int pkey = conn.executeIntUpdate("INSERT INTO email.\"InboxAddress\" VALUES (default,?,?) RETURNING pkey", address, lsa);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_ACC_ADDRESSES,
			getBusinessForEmailAddress(conn, address),
			getAOServerForEmailAddress(conn, address),
			false
		);
		return pkey;
	}

	public static int addEmailDomain(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		DomainName domain,
		int aoServer,
		AccountingCode packageName
	) throws IOException, SQLException {
		MasterServer.checkAccessHostname(conn, source, "addEmailDomain", domain.toString());
		ServerHandler.checkAccessServer(conn, source, "addEmailDomain", aoServer);
		PackageHandler.checkAccessPackage(conn, source, "addEmailDomain", packageName);
		PackageHandler.checkPackageAccessServer(conn, source, "addEmailDomain", packageName, aoServer);

		int pkey = conn.executeIntUpdate("INSERT INTO email.\"Domain\" VALUES (default,?,?,?) RETURNING pkey", domain, aoServer, packageName);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.EMAIL_DOMAINS,
			PackageHandler.getBusinessForPackage(conn, packageName),
			aoServer,
			false
		);
		return pkey;
	}

	/**
	 * Adds a email SMTP relay.
	 */
	public static int addEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		AccountingCode packageName,
		int aoServer,
		HostAddress host,
		String type,
		long duration
	) throws IOException, SQLException {
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
		if(duration!=-1 && duration<=0) throw new SQLException("Duration must be positive: "+duration);

		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add EmailSmtpRelay, Package disabled: "+packageName);

		int pkey;
		if(aoServer==-1) {
			pkey = conn.executeIntUpdate(
				"INSERT INTO email.\"SmtpRelay\" values(default,?,null,?,?,now(),now(),0,?,null) RETURNING pkey",
				packageName,
				host,
				type,
				duration == -1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(System.currentTimeMillis() + duration)
			);
		} else {
			pkey = conn.executeIntUpdate(
				"INSERT INTO email.\"SmtpRelay\" VALUES (default,?,?,?,?,now(),now(),0,?,null) RETURNING pkey",
				packageName,
				aoServer,
				host,
				type,
				duration == -1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(System.currentTimeMillis() + duration)
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
	}

	/**
	 * Only report each error at most once per 12 hours per package.
	 */
	private static final long SMTP_STAT_REPORT_INTERVAL=12L*60*60*1000;
	private static final Map<String,Long> smtpStatLastReports=new HashMap<>();

	public static int addSpamEmailMessage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int esr,
		String message
	) throws IOException, SQLException {
		UserId username=source.getUsername();
		MasterUser masterUser=MasterServer.getMasterUser(conn, username);
		com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
		if(masterUser==null || masterServers.length!=0) throw new SQLException("Only master users may add spam email messages.");

		int pkey = conn.executeIntUpdate(
			"INSERT INTO spam_email_messages VALUES(default,?,now(),?) RETURNING pkey",
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
	}

	public static int addMajordomoList(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int majordomoServer,
		String listName
	) throws IOException, SQLException {
		if(!MajordomoList.isValidListName(listName)) throw new SQLException("Invalide Majordomo list name: "+listName);

		checkAccessMajordomoServer(conn, source, "addMajordomoList", majordomoServer);

		DomainName domainName=getDomainForEmailDomain(conn, majordomoServer);
		UnixPath msPath;
		UnixPath listPath;
		UnixPath infoPath;
		UnixPath introPath;
		try {
			msPath = UnixPath.valueOf(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY+"/"+domainName);
			listPath = UnixPath.valueOf(msPath+"/lists/"+listName);
			infoPath = UnixPath.valueOf(listPath+".info");
			introPath = UnixPath.valueOf(listPath+".intro");
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		int aoServer=getAOServerForEmailDomain(conn, majordomoServer);

		// Disabled checks
		AccountingCode packageName=getPackageForEmailDomain(conn, majordomoServer);
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
			"insert into email.\"MajordomoList\" values(?,?,?,?,?,?,?,?,?)",
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
			infoPath,
			MajordomoList.getDefaultInfoFile(domainName, listName),
			uid,
			gid,
			0664
		);
		daemonConn.setEmailListFile(
			introPath,
			MajordomoList.getDefaultIntroFile(domainName, listName),
			uid,
			gid,
			0664
		);

		return pkey;
	}

	public static void addMajordomoServer(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int domain,
		int lsa,
		int lsg,
		String version
	) throws IOException, SQLException {
		// Security checks
		checkAccessEmailDomain(conn, source, "addMajordomoServer", domain);
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addMajordomoServer", lsa);
		UserId lsaUsername=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, lsa);
		if(lsaUsername.equals(LinuxAccount.MAIL)) throw new SQLException("Unable to add MajordomoServer with LinuxServerAccount of '"+lsaUsername+'\'');
		String lsaType=LinuxAccountHandler.getTypeForLinuxServerAccount(conn, lsa);
		if(
			!lsaType.equals(LinuxAccountType.APPLICATION)
			&& !lsaType.equals(LinuxAccountType.USER)
		) throw new SQLException("May only add Majordomo servers using Linux accounts of type '"+LinuxAccountType.APPLICATION+"' or '"+LinuxAccountType.USER+"', trying to use '"+lsaType+'\'');
		LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addMajordomoServer", lsg);
		GroupId lsgName=LinuxAccountHandler.getGroupNameForLinuxServerGroup(conn, lsg);
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
		if(domainAOServer!=lsaAOServer) throw new SQLException("((email.Domain.pkey="+domain+").ao_server='"+domainAOServer+"')!=((linux_server_accounts.pkey="+lsa+").ao_server='"+lsaAOServer+"')");
		int lsgAOServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, lsg);
		if(domainAOServer!=lsgAOServer) throw new SQLException("((email.Domain.pkey="+domain+").ao_server='"+domainAOServer+"')!=((linux_server_groups.pkey="+lsg+").ao_server='"+lsgAOServer+"')");

		// Disabled checks
		AccountingCode packageName=getPackageForEmailDomain(conn, domain);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Majordomo server: Package for domain #"+domain+" is disabled: "+packageName);
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to add Majordomo server: LinuxServerAccount disabled: "+lsa);
		AccountingCode lgPackageName=LinuxAccountHandler.getPackageForLinuxServerGroup(conn, lsg);
		if(PackageHandler.isPackageDisabled(conn, lgPackageName)) throw new SQLException("Unable to add Majordomo server: Package for LinuxServerGroup #"+lsg+" is disabled: "+lgPackageName);

		// Create the majordomo email pipe
		DomainName domainName=getDomainForEmailDomain(conn, domain);
		UnixPath majordomoServerPath;
		try {
			majordomoServerPath = UnixPath.valueOf(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY + "/" + domainName);
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		int majordomoPipe=addEmailPipe0(conn, invalidateList, domainAOServer, majordomoServerPath+"/wrapper majordomo", packageName);
		int majordomoAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_ADDRESS, domain);
		int majordomoPipeAddress=addEmailPipeAddress0(conn, invalidateList, majordomoAddress, majordomoPipe);

		int ownerMajordomoAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.OWNER_MAJORDOMO_ADDRESS, domain);
		int majordomoOwnerAddress=getOrAddEmailAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_OWNER_ADDRESS, domain);

		conn.executeUpdate(
			"insert into\n"
			+ "  email.\"MajordomoServer\"\n"
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
	}

	public static void disableEmailList(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isEmailListDisabled(conn, pkey)) throw new SQLException("EmailList is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailList", disableLog, false);
		checkAccessEmailList(conn, source, "disableEmailList", pkey);

		conn.executeUpdate(
			"update email.\"List\" set disable_log=? where pkey=?",
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
	}

	public static void disableEmailPipe(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isEmailPipeDisabled(conn, pkey)) throw new SQLException("EmailPipe is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailPipe", disableLog, false);
		checkAccessEmailPipe(conn, source, "disableEmailPipe", pkey);

		conn.executeUpdate(
			"update email.\"Pipe\" set disable_log=? where pkey=?",
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
	}

	public static void disableEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isEmailSmtpRelayDisabled(conn, pkey)) throw new SQLException("EmailSmtpRelay is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableEmailSmtpRelay", disableLog, false);
		checkAccessEmailSmtpRelay(conn, source, "disableEmailSmtpRelay", pkey);

		conn.executeUpdate(
			"update email.\"SmtpRelay\" set disable_log=? where pkey=?",
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
	}

	public static void enableEmailList(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForEmailList(conn, pkey);
		if(disableLog==-1) throw new SQLException("EmailList is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailList", disableLog, true);
		checkAccessEmailList(conn, source, "enableEmailList", pkey);
		AccountingCode pk=getPackageForEmailList(conn, pkey);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailList #"+pkey+", Package not enabled: "+pk);

		conn.executeUpdate(
			"update email.\"List\" set disable_log=null where pkey=?",
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
	}

	public static void enableEmailPipe(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForEmailPipe(conn, pkey);
		if(disableLog==-1) throw new SQLException("EmailPipe is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailPipe", disableLog, true);
		checkAccessEmailPipe(conn, source, "enableEmailPipe", pkey);
		AccountingCode pk=getPackageForEmailPipe(conn, pkey);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailPipe #"+pkey+", Package not enabled: "+pk);

		conn.executeUpdate(
			"update email.\"Pipe\" set disable_log=null where pkey=?",
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
	}

	public static void enableEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForEmailSmtpRelay(conn, pkey);
		if(disableLog==-1) throw new SQLException("EmailSmtpRelay is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableEmailSmtpRelay", disableLog, true);
		checkAccessEmailSmtpRelay(conn, source, "enableEmailSmtpRelay", pkey);
		AccountingCode pk=getPackageForEmailSmtpRelay(conn, pkey);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable EmailSmtpRelay #"+pkey+", Package not enabled: "+pk);

		conn.executeUpdate(
			"update email.\"SmtpRelay\" set disable_log=null where pkey=?",
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
	}

	public static int getDisableLogForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from email.\"List\" where pkey=?", pkey);
	}

	public static int getDisableLogForEmailPipe(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from email.\"Pipe\" where pkey=?", pkey);
	}

	public static int getDisableLogForEmailSmtpRelay(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from email.\"SmtpRelay\" where pkey=?", pkey);
	}

	public static String getEmailListAddressList(
		DatabaseConnection conn,
		RequestSource source,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "getEmailListAddressList", pkey);
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForEmailList(conn, pkey)
		).getEmailListFile(getPathForEmailList(conn, pkey));
	}

	public static IntList getEmailListsForLinuxServerAccount(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from email.\"List\" where linux_server_account=?", pkey);
	}

	public static IntList getEmailListsForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  el.pkey\n"
			+ "from\n"
			+ "  linux_groups lg,\n"
			+ "  linux_server_groups lsg,\n"
			+ "  email.\"List\" el\n"
			+ "where\n"
			+ "  lg.package=?\n"
			+ "  and lg.name=lsg.name\n"
			+ "  and lsg.pkey=el.linux_server_group",
			name
		);
	}

	public static IntList getEmailPipesForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from email.\"Pipe\" where package=?", name);
	}

	public static long[] getImapFolderSizes(
		DatabaseConnection conn,
		RequestSource source,
		int linux_server_account,
		String[] folderNames
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "getImapFolderSizes", linux_server_account);
		int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linux_server_account);
		if(DaemonHandler.isDaemonAvailable(aoServer)) {
			UserId username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linux_server_account);
			try {
				return DaemonHandler.getDaemonConnector(conn, aoServer).getImapFolderSizes(username, folderNames);
			} catch(IOException err) {
				logger.log(Level.SEVERE, "linux_server_account="+linux_server_account+", aoServer="+aoServer+", username="+username+", folderNames="+folderNames, err);
				DaemonHandler.flagDaemonAsDown(aoServer);
			}
		}
		long[] sizes=new long[folderNames.length];
		Arrays.fill(sizes, -1);
		return sizes;
	}

	public static InboxAttributes getInboxAttributes(
		DatabaseConnection conn,
		RequestSource source,
		int linux_server_account
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "getInboxAttributes", linux_server_account);
		int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, linux_server_account);
		if(DaemonHandler.isDaemonAvailable(aoServer)) {
			UserId username=LinuxAccountHandler.getUsernameForLinuxServerAccount(conn, linux_server_account);
			try {
				return DaemonHandler.getDaemonConnector(conn, aoServer).getInboxAttributes(username);
			} catch(IOException err) {
				logger.log(Level.SEVERE, "linux_server_account="+linux_server_account+", aoServer="+aoServer+", username="+username, err);
				DaemonHandler.flagDaemonAsDown(aoServer);
			}
		}
		return null;
	}

	public static IntList getEmailSmtpRelaysForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from email.\"SmtpRelay\" where package=?", name);
	}

	public static int getEmailDomain(
		DatabaseConnection conn,
		int aoServer,
		String path
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  el.pkey\n"
			+ "from\n"
			+ "  email.\"List\" el,\n"
			+ "  linux_server_groups lsg\n"
			+ "where\n"
			+ "  el.path=path\n"
			+ "  and el.linux_server_group=lsg.pkey\n"
			+ "  and lsg.ao_server=?",
			path,
			aoServer
		);
	}

	public static int getEmailDomainForEmailAddress(
		DatabaseConnection conn,
		int emailAddress
	) throws IOException, SQLException {
		return conn.executeIntQuery("select domain from email.\"Address\" where pkey=?", emailAddress);
	}

	public static String getMajordomoInfoFile(
		DatabaseConnection conn,
		RequestSource source,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "getMajordomoInfoFile", pkey);
		UnixPath infoPath;
		try {
			infoPath = UnixPath.valueOf(getPathForEmailList(conn, pkey)+".info");
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForEmailList(conn, pkey)
		).getEmailListFile(infoPath);
	}

	public static String getMajordomoIntroFile(
		DatabaseConnection conn,
		RequestSource source,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "getMajordomoIntroFile", pkey);
		UnixPath introPath;
		try {
			introPath = UnixPath.valueOf(getPathForEmailList(conn, pkey)+".intro");
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForEmailList(conn, pkey)
		).getEmailListFile(introPath);
	}

	public static int getMajordomoServer(
		DatabaseConnection conn,
		int emailDomain
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  pkey\n"
			+ "from\n"
			+ "  email.\"MajordomoServer\"\n"
			+ "where\n"
			+ "  domain=?",
			emailDomain
		);
	}

	public static void getSpamEmailMessagesForEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source, 
		CompressedDataOutputStream out,
		boolean provideProgress,
		int esr
	) throws IOException, SQLException {
		UserId username=source.getUsername();
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
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
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
	}

	public static boolean isEmailListDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		synchronized(EmailHandler.class) {
			Integer I = pkey;
			Boolean O=disabledEmailLists.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForEmailList(conn, pkey)!=-1;
			disabledEmailLists.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isEmailPipeDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		synchronized(EmailHandler.class) {
			Integer I = pkey;
			Boolean O=disabledEmailPipes.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForEmailPipe(conn, pkey)!=-1;
			disabledEmailPipes.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isEmailSmtpRelayDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		synchronized(EmailHandler.class) {
			Integer I = pkey;
			Boolean O=disabledEmailSmtpRelays.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForEmailSmtpRelay(conn, pkey)!=-1;
			disabledEmailSmtpRelays.put(I, isDisabled);
			return isDisabled;
		}
	}

	/**
	 * Refreshes a email SMTP relay.
	 */
	public static void refreshEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		long minDuration
	) throws IOException, SQLException {
		checkAccessEmailSmtpRelay(conn, source, "refreshEmailSmtpRelay", pkey);

		if(isEmailSmtpRelayDisabled(conn, pkey)) throw new SQLException("Unable to refresh EmailSmtpRelay, EmailSmtpRelay disabled: "+pkey);

		AccountingCode packageName=getPackageForEmailSmtpRelay(conn, pkey);
		AccountingCode accounting = PackageHandler.getBusinessForPackage(conn, packageName);
		int aoServer=getAOServerForEmailSmtpRelay(conn, pkey);
		Timestamp expiration=conn.executeTimestampQuery("select expiration from email.\"SmtpRelay\" where pkey=?", pkey);
		long exp=expiration==null?-1:expiration.getTime();
		long min=minDuration==-1?-1:(System.currentTimeMillis()+minDuration);
		conn.executeUpdate(
			"update email.\"SmtpRelay\" set last_refreshed=now(), refresh_count=refresh_count+1, expiration=? where pkey=?",
			exp==-1 || min==-1
			? null
			: new Timestamp(Math.max(exp, min)),
			pkey
		);

		// Delete any old entries
		conn.executeUpdate(
			"delete from email.\"SmtpRelay\" where package=? and (ao_server is null or ao_server=?) and expiration is not null and now()::date-expiration::date>"+EmailSmtpRelay.HISTORY_DAYS,
			packageName,
			aoServer
		);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_SMTP_RELAYS, accounting, aoServer, false);
	}

	public static void removeBlackholeEmailAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int bea
	) throws IOException, SQLException {
		checkAccessEmailAddress(conn, source, "removeBlackholeEmailAddress", bea);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, bea);
		int aoServer=getAOServerForEmailAddress(conn, bea);

		// Delete from the database
		conn.executeUpdate("delete from email.\"BlackholeAddress\" where email_address=?", bea);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES,
			accounting,
			aoServer,
			false
		);
	}

	public static void removeEmailAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int address
	) throws IOException, SQLException {
		checkAccessEmailAddress(conn, source, "removeEmailAddress", address);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, address);
		int aoServer=getAOServerForEmailAddress(conn, address);

		// Delete the objects that depend on this one first
		boolean isBlackhole=conn.executeBooleanQuery("select (select email_address from email.\"BlackholeAddress\" where email_address=?) is not null", address);
		if(isBlackhole) conn.executeUpdate("delete from email.\"BlackholeAddress\" where email_address=?", address);

		IntList pkeys=conn.executeIntListQuery("select pkey from email.\"InboxAddress\" where email_address=?", address);
		boolean isLinuxAccAddress=pkeys.size()>0;
		if(isLinuxAccAddress) {
			for(int d=0;d<pkeys.size();d++) {
				int laaPkey=pkeys.getInt(d);
				conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laaPkey);
				conn.executeUpdate("delete from email.\"InboxAddress\" where pkey=?", laaPkey);
			}
		}

		boolean isEmailForwarding = conn.executeUpdate("delete from email.\"Forwarding\" where email_address=?", address) > 0;
		boolean isEmailListAddress = conn.executeUpdate("delete from email.\"ListAddress\" where email_address=?", address) > 0;
		boolean isEmailPipeAddress = conn.executeUpdate("delete from email.\"PipeAddress\" where email_address=?", address) > 0;

		// Delete from the database
		conn.executeUpdate("delete from email.\"Address\" where pkey=?", address);

		// Notify all clients of the update
		if(isBlackhole) invalidateList.addTable(conn, SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES, accounting, aoServer, false);
		if(isLinuxAccAddress) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
		if(isEmailForwarding) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
		if(isEmailListAddress) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
		if(isEmailPipeAddress) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
	}

	public static void removeEmailForwarding(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int ef
	) throws IOException, SQLException {
		int ea=conn.executeIntQuery("select email_address from email.\"Forwarding\" where pkey=?", ef);
		checkAccessEmailAddress(conn, source, "removeEmailForwarding", ea);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, ea);
		int aoServer=getAOServerForEmailAddress(conn, ea);

		// Delete from the database
		conn.executeUpdate("delete from email.\"Forwarding\" where pkey=?", ef);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
	}

	public static void removeEmailListAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int ela
	) throws IOException, SQLException {
		int ea=conn.executeIntQuery("select email_address from email.\"ListAddress\" where pkey=?", ela);
		checkAccessEmailAddress(conn, source, "removeEmailListAddress", ea);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, ea);
		int aoServer=getAOServerForEmailAddress(conn, ea);

		// Delete from the database
		conn.executeUpdate("delete from email.\"ListAddress\" where pkey=?", ela);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
	}

	public static void removeEmailList(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "removeEmailList", pkey);

		removeEmailList(conn, invalidateList, pkey);
	}

	public static void removeEmailList(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		// Get the values for later use
		AccountingCode accounting = getBusinessForEmailList(conn, pkey);
		int aoServer=getAOServerForEmailList(conn, pkey);
		UnixPath path = conn.executeObjectQuery(
			ObjectFactories.unixPathFactory,
			"select path from email.\"List\" where pkey=?",
			pkey
		);
		// Delete the majordomo_list that is attached to this email list
		if(isMajordomoList(conn, pkey)) {
			// Get the listname_pipe_add and details
			int listnameEPA=conn.executeIntQuery("select listname_pipe_add from email.\"MajordomoList\" where email_list=?", pkey);
			int listnameEA=conn.executeIntQuery("select email_address from email.\"PipeAddress\" where pkey=?", listnameEPA);
			int listnameEP=conn.executeIntQuery("select email_pipe from email.\"PipeAddress\" where pkey=?", listnameEPA);

			// Get the listname_list_add and details
			int listnameListELA=conn.executeIntQuery("select listname_list_add from email.\"MajordomoList\" where email_list=?", pkey);
			int listnameListEA=conn.executeIntQuery("select email_address from email.\"ListAddress\" where pkey=?", listnameListELA);

			// Get the listname_request_pipe_add and details
			int listnameRequestEPA=conn.executeIntQuery("select listname_request_pipe_add from email.\"MajordomoList\" where email_list=?", pkey);
			int listnameRequestEA=conn.executeIntQuery("select email_address from email.\"PipeAddress\" where pkey=?", listnameRequestEPA);
			int listnameRequestEP=conn.executeIntQuery("select email_pipe from email.\"PipeAddress\" where pkey=?", listnameRequestEPA);

			// Other direct email addresses
			int ownerListnameEA=conn.executeIntQuery("select owner_listname_add from email.\"MajordomoList\" where email_list=?", pkey);
			int listnameOwnerEA=conn.executeIntQuery("select listname_owner_add from email.\"MajordomoList\" where email_list=?", pkey);
			int listnameApprovalEA=conn.executeIntQuery("select listname_approval_add from email.\"MajordomoList\" where email_list=?", pkey);

			conn.executeUpdate("delete from email.\"MajordomoList\" where email_list=?", pkey);
			invalidateList.addTable(conn, SchemaTable.TableID.MAJORDOMO_LISTS, accounting, aoServer, false);

			// Delete the listname_pipe_add
			conn.executeUpdate("delete from email.\"PipeAddress\" where pkey=?", listnameEPA);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
			if(!isEmailAddressUsed(conn, listnameEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", listnameEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}
			conn.executeUpdate("delete from email.\"Pipe\" where pkey=?", listnameEP);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);

			// Delete the listname_list_add
			conn.executeUpdate("delete from email.\"ListAddress\" where pkey=?", listnameListELA);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
			if(!isEmailAddressUsed(conn, listnameListEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", listnameListEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}

			// Delete the listname_pipe_add
			conn.executeUpdate("delete from email.\"PipeAddress\" where pkey=?", listnameRequestEPA);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
			if(!isEmailAddressUsed(conn, listnameRequestEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", listnameRequestEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}
			conn.executeUpdate("delete from email.\"Pipe\" where pkey=?", listnameRequestEP);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);

			// Other direct email addresses
			if(!isEmailAddressUsed(conn, ownerListnameEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", ownerListnameEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}
			if(!isEmailAddressUsed(conn, listnameOwnerEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", listnameOwnerEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}
			if(!isEmailAddressUsed(conn, listnameApprovalEA)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", listnameApprovalEA);
				invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
			}
		}

		// Delete the objects that depend on this one first
		IntList addresses=conn.executeIntListQuery("select email_address from email.\"ListAddress\" where email_list=?", pkey);
		int size=addresses.size();
		boolean addressesModified=size>0;
		for(int c=0;c<size;c++) {
			int address=addresses.getInt(c);
			conn.executeUpdate("delete from email.\"ListAddress\" where email_address=? and email_list=?", address, pkey);
			if(!isEmailAddressUsed(conn, address)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", address);
			}
		}

		// Delete from the database
		conn.executeUpdate("delete from email.\"List\" where pkey=?", pkey);

		// Notify all clients of the update
		if(addressesModified) {
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LISTS, accounting, aoServer, false);

		// Remove the list file from the server
		DaemonHandler.getDaemonConnector(conn, aoServer).removeEmailList(path);
	}

	public static void removeLinuxAccAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int laa
	) throws IOException, SQLException {
		int ea=conn.executeIntQuery("select email_address from email.\"InboxAddress\" where pkey=?", laa);
		checkAccessEmailAddress(conn, source, "removeLinuxAccAddress", ea);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, ea);
		int aoServer=getAOServerForEmailAddress(conn, ea);

		// Delete from the database
		conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laa);
		conn.executeUpdate("delete from email.\"InboxAddress\" where pkey=?", laa);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
	}

	public static void removeEmailPipe(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailPipe(conn, source, "removeEmailPipe", pkey);

		removeEmailPipe(conn, invalidateList, pkey);
	}

	public static void removeEmailPipe(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		// Get the values for later use
		AccountingCode accounting = getBusinessForEmailPipe(conn, pkey);
		int aoServer=getAOServerForEmailPipe(conn, pkey);

		// Delete the objects that depend on this one first
		IntList addresses=conn.executeIntListQuery("select email_address from email.\"PipeAddress\" where email_pipe=?", pkey);
		int size=addresses.size();
		boolean addressesModified=size>0;
		for(int c=0;c<size;c++) {
			int address=addresses.getInt(c);
			conn.executeUpdate("delete from email.\"PipeAddress\" where email_address=? and email_pipe=?", address, pkey);
			if(!isEmailAddressUsed(conn, address)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", address);
			}
		}

		// Delete from the database
		conn.executeUpdate("delete from email.\"Pipe\" where pkey=?", pkey);

		// Notify all clients of the update
		if(addressesModified) {
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);
	}

	public static void removeEmailPipeAddress(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int epa
	) throws IOException, SQLException {
		int ea=conn.executeIntQuery("select email_address from email.\"PipeAddress\" where pkey=?", epa);
		checkAccessEmailAddress(conn, source, "removeEmailPipeAddress", ea);

		// Get stuff for use after the try block
		AccountingCode accounting = getBusinessForEmailAddress(conn, ea);
		int aoServer=getAOServerForEmailAddress(conn, ea);

		// Delete from the database
		conn.executeUpdate("delete from email.\"PipeAddress\" where pkey=?", epa);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
	}

	public static void removeEmailDomain(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailDomain(conn, source, "removeEmailDomain", pkey);

		removeEmailDomain(conn, invalidateList, pkey);
	}

	public static void removeEmailDomain(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		boolean
			beaMod=false,
			laaMod=false,
			efMod=false,
			elaMod=false,
			epaMod=false
		;
		AccountingCode accounting = getBusinessForEmailDomain(conn, pkey);
		int aoServer=getAOServerForEmailDomain(conn, pkey);

		// Remove any majordomo server
		int ms=conn.executeIntQuery("select coalesce((select domain from email.\"MajordomoServer\" where domain=?), -1)", pkey);
		if(ms!=-1) removeMajordomoServer(conn, invalidateList, pkey);

		// Get the list of all email addresses in the domain
		IntList addresses=conn.executeIntListQuery("select pkey from email.\"Address\" where domain=?", pkey);

		int len=addresses.size();
		boolean eaMod=len>0;
		for(int c=0;c<len;c++) {
			int address=addresses.getInt(c);

			if(
				conn.executeBooleanQuery(
					"select (select email_address from email.\"BlackholeAddress\" where email_address=?) is not null",
					address
				)
			) {
				conn.executeUpdate("delete from email.\"BlackholeAddress\" where email_address=?", address);
				beaMod=true;
			}

			// Delete any email.InboxAddress used by this email address
	IntList pkeys=conn.executeIntListQuery("select pkey from email.\"InboxAddress\" where email_address=?", address);
	if(pkeys.size()>0) {
		for(int d=0;d<pkeys.size();d++) {
		int laaPkey=pkeys.getInt(d);
		conn.executeUpdate("update linux_server_accounts set autoresponder_from=null where autoresponder_from=?", laaPkey);
		conn.executeUpdate("delete from email.\"InboxAddress\" where pkey=?", laaPkey);
		}
				laaMod=true;
			}

			if(
				conn.executeBooleanQuery(
					"select (select pkey from email.\"Forwarding\" where email_address=? limit 1) is not null",
					address
				)
			) {
				conn.executeUpdate("delete from email.\"Forwarding\" where email_address=?", address);
				efMod=true;
			}


			if(
				conn.executeBooleanQuery(
					"select (select pkey from email.\"ListAddress\" where email_address=? limit 1) is not null",
					address
				)
			) {
				conn.executeUpdate("delete from email.\"ListAddress\" where email_address=?", address);
				elaMod=true;
			}

			if(
				conn.executeBooleanQuery(
					"select (select pkey from email.\"PipeAddress\" where email_address=? limit 1) is not null",
					address
				)
			) {
				conn.executeUpdate("delete from email.\"PipeAddress\" where email_address=?", address);
				epaMod=true;
			}

			// Delete from the database
			conn.executeUpdate("delete from email.\"Address\" where pkey=?", address);
		}

		// Remove the domain from the database
		conn.executeUpdate("delete from email.\"Domain\" where pkey=?", pkey);

		// Notify all clients of the update
		if(beaMod) invalidateList.addTable(conn, SchemaTable.TableID.BLACKHOLE_EMAIL_ADDRESSES, accounting, aoServer, false);
		if(laaMod) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
		if(efMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_FORWARDING, accounting, aoServer, false);
		if(elaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_LIST_ADDRESSES, accounting, aoServer, false);
		if(epaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
		if(eaMod) invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_DOMAINS, accounting, aoServer, false);
	}

	/**
	 * Removes a email SMTP relay.
	 */
	public static void removeEmailSmtpRelay(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessEmailSmtpRelay(conn, source, "removeEmailSmtpRelay", pkey);

		removeEmailSmtpRelay(conn, invalidateList, pkey);
	}

	/**
	 * Removes a email SMTP relay.
	 */
	public static void removeEmailSmtpRelay(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForEmailSmtpRelay(conn, pkey);
		int aoServer=getAOServerForEmailSmtpRelay(conn, pkey);

		conn.executeUpdate(
			"delete from email.\"SmtpRelay\" where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_SMTP_RELAYS, accounting, aoServer, false);
	}

	public static void removeMajordomoServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int domain
	) throws IOException, SQLException {
		checkAccessMajordomoServer(conn, source, "removeMajordomoServer", domain);

		removeMajordomoServer(conn, invalidateList, domain);
	}

	public static void removeMajordomoServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int domain
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForEmailDomain(conn, domain);
		int aoServer=getAOServerForEmailDomain(conn, domain);

		// Remove any majordomo lists
		IntList mls=conn.executeIntListQuery("select email_list from email.\"MajordomoList\" where majordomo_server=?", domain);
		if(mls.size()>0) {
			for(int c=0;c<mls.size();c++) {
				removeEmailList(conn, invalidateList, mls.getInt(c));
			}
		}

		// Get the majordomo_pipe_address and details
		int epa=conn.executeIntQuery("select majordomo_pipe_address from email.\"MajordomoServer\" where domain=?", domain);
		int ea=conn.executeIntQuery("select email_address from email.\"PipeAddress\" where pkey=?", epa);
		int ep=conn.executeIntQuery("select email_pipe from email.\"PipeAddress\" where pkey=?", epa);

		// Get the other email addresses referenced
		int omEA=conn.executeIntQuery("select owner_majordomo_add from email.\"MajordomoServer\" where domain=?", domain);
		int moEA=conn.executeIntQuery("select majordomo_owner_add from email.\"MajordomoServer\" where domain=?", domain);

		// Remove the domain from the database
		conn.executeUpdate("delete from email.\"MajordomoServer\" where domain=?", domain);
		invalidateList.addTable(conn, SchemaTable.TableID.MAJORDOMO_SERVERS, accounting, aoServer, false);

		// Remove the majordomo pipe and address
		conn.executeUpdate("delete from email.\"PipeAddress\" where pkey=?", epa);
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPE_ADDRESSES, accounting, aoServer, false);
		if(!isEmailAddressUsed(conn, ea)) {
			conn.executeUpdate("delete from email.\"Address\" where pkey=?", ea);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
		conn.executeUpdate("delete from email.\"Pipe\" where pkey=?", ep);
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_PIPES, accounting, aoServer, false);

		// Remove the referenced email addresses if not used
		if(!isEmailAddressUsed(conn, omEA)) {
			conn.executeUpdate("delete from email.\"Address\" where pkey=?", omEA);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
		if(!isEmailAddressUsed(conn, moEA)) {
			conn.executeUpdate("delete from email.\"Address\" where pkey=?", moEA);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
	}

	public static void setEmailListAddressList(
		DatabaseConnection conn,
		RequestSource source,
		int pkey,
		String addresses
	) throws IOException, SQLException {
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
	}

	public static AccountingCode getBusinessForEmailAddress(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from email.\"Address\" ea, email.\"Domain\" sd, billing.\"Package\" pk where ea.domain=sd.pkey and sd.package=pk.name and ea.pkey=?",
			pkey
		);
	}

	public static AccountingCode getBusinessForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from email.\"List\" el, linux_server_groups lsg, linux_groups lg, billing.\"Package\" pk where el.linux_server_group=lsg.pkey and lsg.name=lg.name and lg.package=pk.name and el.pkey=?",
			pkey
		);
	}

	public static AccountingCode getBusinessForEmailPipe(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from email.\"Pipe\" ep, billing.\"Package\" pk where ep.package=pk.name and ep.pkey=?",
			pkey
		);
	}

	public static AccountingCode getBusinessForEmailDomain(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from email.\"Domain\" sd, billing.\"Package\" pk where sd.package=pk.name and sd.pkey=?",
			pkey
		);
	}

	public static DomainName getDomainForEmailDomain(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.domainNameFactory,
			"select domain from email.\"Domain\" where pkey=?",
			pkey
		);
	}

	public static AccountingCode getBusinessForEmailSmtpRelay(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from email.\"SmtpRelay\" esr, billing.\"Package\" pk where esr.package=pk.name and esr.pkey=?",
			pkey
		);
	}

	public static int getEmailAddress(DatabaseConnection conn, String address, int domain) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select coalesce((select pkey from email.\"Address\" where address=? and domain=?), -1)",
			address,
			domain
		);
	}

	public static int getOrAddEmailAddress(DatabaseConnection conn, InvalidateList invalidateList, String address, int domain) throws IOException, SQLException {
		int pkey=getEmailAddress(conn, address, domain);
		if(pkey==-1) pkey=addEmailAddress0(conn, invalidateList, address, domain);
		return pkey;
	}

	public static int getLinuxServerAccountForMajordomoServer(DatabaseConnection conn, int domain) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_account from email.\"MajordomoServer\" where domain=?", domain);
	}

	public static int getLinuxServerGroupForMajordomoServer(DatabaseConnection conn, int domain) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_group from email.\"MajordomoServer\" where domain=?", domain);
	}

	public static AccountingCode getPackageForEmailDomain(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from email.\"Domain\" where pkey=?",
			pkey
		);
	}

	public static AccountingCode getPackageForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  lg.package\n"
			+ "from\n"
			+ "  email.\"List\" el,\n"
			+ "  linux_server_groups lsg,\n"
			+ "  linux_groups lg\n"
			+ "where\n"
			+ "  el.pkey=?\n"
			+ "  and el.linux_server_group=lsg.pkey\n"
			+ "  and lsg.name=lg.name",
			pkey
		);
	}

	public static AccountingCode getPackageForEmailPipe(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from email.\"Pipe\" where pkey=?",
			pkey
		);
	}

	public static AccountingCode getPackageForEmailSmtpRelay(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from email.\"SmtpRelay\" where pkey=?",
			pkey
		);
	}

	public static UnixPath getPathForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.unixPathFactory,
			"select path from email.\"List\" where pkey=?",
			pkey
		);
	}

	public static int getAOServerForEmailAddress(DatabaseConnection conn, int address) throws IOException, SQLException {
		return conn.executeIntQuery("select ed.ao_server from email.\"Address\" ea, email.\"Domain\" ed where ea.domain=ed.pkey and ea.pkey=?", address);
	}

	public static int getAOServerForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  lsg.ao_server\n"
			+ "from\n"
			+ "  email.\"List\" el,\n"
			+ "  linux_server_groups lsg\n"
			+ "where\n"
			+ "  el.pkey=?\n"
			+ "  and el.linux_server_group=lsg.pkey",
			pkey
		);
	}

	public static int getLinuxServerAccountForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_account from email.\"List\" where pkey=?", pkey);
	}

	public static int getLinuxServerGroupForEmailList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_group from email.\"List\" where pkey=?", pkey);
	}

	public static boolean isEmailAddressUsed(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return
			conn.executeBooleanQuery("select (select email_address from email.\"BlackholeAddress\" where email_address=? limit 1) is not null", pkey)
			|| conn.executeBooleanQuery("select (select pkey from email.\"Forwarding\" where email_address=? limit 1) is not null", pkey)
			|| conn.executeBooleanQuery("select (select pkey from email.\"ListAddress\" where email_address=? limit 1) is not null", pkey)
			|| conn.executeBooleanQuery("select (select pkey from email.\"PipeAddress\" where email_address=? limit 1) is not null", pkey)
			|| conn.executeBooleanQuery("select (select pkey from email.\"InboxAddress\" where email_address=? limit 1) is not null", pkey)
			|| conn.executeBooleanQuery(
				"select\n"
				+ "  (\n"
				+ "    select\n"
				+ "      ml.email_list\n"
				+ "    from\n"
				+ "      email.\"MajordomoList\" ml,\n"
				+ "      email.\"PipeAddress\" epa1,\n"
				+ "      email.\"ListAddress\" ela,\n"
				+ "      email.\"PipeAddress\" epa2\n"
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
				+ "      email.\"MajordomoServer\" ms,\n"
				+ "      email.\"PipeAddress\" epa\n"
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
	}

	public static int getAOServerForEmailPipe(DatabaseConnection conn, int pipe) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from email.\"Pipe\" where pkey=?", pipe);
	}

	public static int getAOServerForEmailDomain(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from email.\"Domain\" where pkey=?", pkey);
	}

	public static int getAOServerForEmailSmtpRelay(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from email.\"SmtpRelay\" where pkey=?", pkey);
	}

	public static boolean isEmailDomainAvailable(DatabaseConnection conn, RequestSource source, int ao_server, DomainName domain) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "isEmailDomainAvailable", ao_server);

		return conn.executeBooleanQuery(
			"select (select pkey from email.\"Domain\" where ao_server=? and domain=?) is null",
			ao_server,
			domain
		);
	}

	public static boolean isMajordomoList(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select email_list from email.\"MajordomoList\" where email_list=?) is not null", pkey);
	}

	public static void setMajordomoInfoFile(
		DatabaseConnection conn,
		RequestSource source,
		int pkey,
		String file
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "setMajordomoInfoFile", pkey);
		UnixPath infoPath;
		try {
			infoPath = UnixPath.valueOf(getPathForEmailList(conn, pkey)+".info");
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForEmailList(conn, pkey)
		).setEmailListFile(
			infoPath,
			file,
			LinuxAccountHandler.getUIDForLinuxServerAccount(conn, getLinuxServerAccountForEmailList(conn, pkey)),
			LinuxAccountHandler.getGIDForLinuxServerGroup(conn, getLinuxServerGroupForEmailList(conn, pkey)),
			0664
		);
	}

	public static void setMajordomoIntroFile(
		DatabaseConnection conn,
		RequestSource source,
		int pkey,
		String file
	) throws IOException, SQLException {
		checkAccessEmailList(conn, source, "setMajordomoIntroFile", pkey);
		UnixPath introPath;
		try {
			introPath = UnixPath.valueOf(getPathForEmailList(conn, pkey)+".intro");
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForEmailList(conn, pkey)
		).setEmailListFile(
			introPath,
			file,
			LinuxAccountHandler.getUIDForLinuxServerAccount(conn, getLinuxServerAccountForEmailList(conn, pkey)),
			LinuxAccountHandler.getGIDForLinuxServerGroup(conn, getLinuxServerGroupForEmailList(conn, pkey)),
			0664
		);
	}
}
