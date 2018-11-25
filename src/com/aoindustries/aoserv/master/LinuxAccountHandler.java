/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.EmailSpamAssassinIntegrationMode;
import com.aoindustries.aoserv.client.HttpdSharedTomcat;
import com.aoindustries.aoserv.client.HttpdSite;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxAccountType;
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.LinuxGroupAccount;
import com.aoindustries.aoserv.client.LinuxServerAccount;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.PasswordChecker;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.Shell;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.Gecos;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.lang.ObjectUtils;
import com.aoindustries.util.IntList;
import com.aoindustries.util.InternUtils;
import com.aoindustries.util.Tuple2;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The <code>LinuxAccountHandler</code> handles all the accesses to the Linux tables.
 *
 * @author  AO Industries, Inc.
 */
final public class LinuxAccountHandler {

	private LinuxAccountHandler() {
	}

	/** Matches value in /etc/login.defs on CentOS 7 */
	private static final int
		CENTOS_7_SYS_GID_MIN = 201,
		CENTOS_7_SYS_UID_MIN = 201
	;

	/** Default sudo setting for newly added "aoadmin" system users. */
	private static final String AOADMIN_SUDO = "ALL=(ALL) NOPASSWD: ALL";

	/** Default sudo setting for newly added "aoserv-xen-migration" system users. */
	private static final String AOSERV_XEN_MIGRATION_SUDO = "ALL=(ALL) NOPASSWD: /usr/sbin/xl -t migrate-receive";

	private final static Map<UserId,Boolean> disabledLinuxAccounts=new HashMap<>();
	private final static Map<Integer,Boolean> disabledLinuxServerAccounts=new HashMap<>();

	public static void checkAccessLinuxAccount(DatabaseConnection conn, RequestSource source, String action, UserId username) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				IntList lsas = getLinuxServerAccountsForLinuxAccount(conn, username);
				boolean found = false;
				for(Integer lsa : lsas) {
					if(ServerHandler.canAccessServer(conn, source, getAOServerForLinuxServerAccount(conn, lsa))) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"business_administrator.username="
						+source.getUsername()
						+" is not allowed to access linux_account: action='"
						+action
						+", username="
						+username
					;
					throw new SQLException(message);
				}
			}
		} else {
			UsernameHandler.checkAccessUsername(conn, source, action, username);
		}
	}

	public static void checkAccessLinuxGroup(DatabaseConnection conn, RequestSource source, String action, GroupId name) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				IntList lsgs = getLinuxServerGroupsForLinuxGroup(conn, name);
				boolean found = false;
				for(int lsg : lsgs) {
					if(ServerHandler.canAccessServer(conn, source, getAOServerForLinuxServerGroup(conn, lsg))) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"business_administrator.username="
						+source.getUsername()
						+" is not allowed to access linux_group: action='"
						+action
						+", name="
						+name
					;
					throw new SQLException(message);
				}
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForLinuxGroup(conn, name));
		}
	}

	public static void checkAccessLinuxGroupAccount(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, action, getLinuxAccountForLinuxGroupAccount(conn, pkey));
		checkAccessLinuxGroup(conn, source, action, getLinuxGroupForLinuxGroupAccount(conn, pkey));
	}

	public static boolean canAccessLinuxServerAccount(DatabaseConnection conn, RequestSource source, int account) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				return ServerHandler.canAccessServer(conn, source, getAOServerForLinuxServerAccount(conn, account));
			} else return true;
		} else {
			return UsernameHandler.canAccessUsername(conn, source, getUsernameForLinuxServerAccount(conn, account));
		}
	}

	public static void checkAccessLinuxServerAccount(DatabaseConnection conn, RequestSource source, String action, int account) throws IOException, SQLException {
		if(!canAccessLinuxServerAccount(conn, source, account)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access linux_server_account: action='"
				+action
				+", pkey="
				+account
			;
			throw new SQLException(message);
		}
	}

	public static boolean canAccessLinuxServerGroup(DatabaseConnection conn, RequestSource source, int group) throws IOException, SQLException {
		return
			PackageHandler.canAccessPackage(conn, source, getPackageForLinuxServerGroup(conn, group))
			&& ServerHandler.canAccessServer(conn, source, getAOServerForLinuxServerGroup(conn, group))
		;
	}

	public static void checkAccessLinuxServerGroup(DatabaseConnection conn, RequestSource source, String action, int group) throws IOException, SQLException {
		if(!canAccessLinuxServerGroup(conn, source, group)) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access linux_server_group: action='"
				+action
				+", pkey="
				+group
			;
			throw new SQLException(message);
		}
	}

	/**
	 * Adds a linux account.
	 */
	public static void addLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		GroupId primary_group,
		Gecos name,
		Gecos office_location,
		Gecos office_phone,
		Gecos home_phone,
		String type,
		UnixPath shell,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Make sure the shell is allowed for the type of account being added
		if(!LinuxAccountType.isAllowedShell(type, shell)) throw new SQLException("shell='"+shell+"' not allowed for type='"+type+'\'');

		if(!skipSecurityChecks) {
			UsernameHandler.checkAccessUsername(conn, source, "addLinuxAccount", username);
			if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add LinuxAccount, Username disabled: "+username);
		}

		conn.executeUpdate(
			"insert into linux.\"User\" values(?,?,?,?,?,?,?,now(),null)",
			username,
			name,
			office_location,
			office_phone,
			home_phone,
			type,
			shell
		);
		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, username),
			InvalidateList.allServers,
			false
		);

		addLinuxGroupAccount(
			conn,
			source,
			invalidateList,
			primary_group,
			username,
			true,
			skipSecurityChecks
		);
	}

	public static void addLinuxGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		GroupId groupName,
		AccountingCode packageName,
		String type,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(!skipSecurityChecks) {
			PackageHandler.checkAccessPackage(conn, source, "addLinuxGroup", packageName);
			if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add LinuxGroup, Package disabled: "+packageName);
		}
		if (
			groupName.equals(LinuxGroup.FTPONLY)
			|| groupName.equals(LinuxGroup.MAIL)
			|| groupName.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to add LinuxGroup: "+groupName);

		conn.executeUpdate("insert into linux.\"Group\" values(?,?,?)", groupName, packageName, type);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_GROUPS,
			PackageHandler.getBusinessForPackage(conn, packageName),
			InvalidateList.allServers,
			false
		);
	}

	public static int addLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		GroupId groupName,
		UserId username,
		boolean isPrimary,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(groupName.equals(LinuxGroup.MAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+LinuxGroup.MAIL+'\'');
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for user '"+LinuxAccount.MAIL+'\'');
		if(!skipSecurityChecks) {
			if(
				!groupName.equals(LinuxGroup.FTPONLY)
				&& !groupName.equals(LinuxGroup.MAILONLY)
			) checkAccessLinuxGroup(conn, source, "addLinuxGroupAccount", groupName);
			checkAccessLinuxAccount(conn, source, "addLinuxGroupAccount", username);
			if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add LinuxGroupUser, LinuxAccount disabled: "+username);
		}
		if(groupName.equals(LinuxGroup.FTPONLY)) {
			// Only allowed to have ftponly group when it is a ftponly account
			String type=getTypeForLinuxAccount(conn, username);
			if(!type.equals(LinuxAccountType.FTPONLY)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+LinuxGroup.FTPONLY+"' on non-ftp-only-type LinuxAccount named "+username);
		}
		if(groupName.equals(LinuxGroup.MAILONLY)) {
			// Only allowed to have mail group when it is a "mailonly" account
			String type=getTypeForLinuxAccount(conn, username);
			if(!type.equals(LinuxAccountType.EMAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+LinuxGroup.MAILONLY+"' on non-email-type LinuxAccount named "+username);
		}

		// Do not allow more than 31 groups per account
		int count=conn.executeIntQuery("select count(*) from linux.\"GroupUser\" where username=?", username);
		if(count>=LinuxGroupAccount.MAX_GROUPS) throw new SQLException("Only "+LinuxGroupAccount.MAX_GROUPS+" groups are allowed per user, username="+username+" already has access to "+count+" groups");

		int pkey = conn.executeIntUpdate(
			"INSERT INTO linux.\"GroupUser\" VALUES (default,?,?,?,null) RETURNING pkey",
			groupName,
			username,
			isPrimary
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getCollection(
				UsernameHandler.getBusinessForUsername(conn, username),
				getBusinessForLinuxGroup(conn, groupName)
			),
			getAOServersForLinuxGroupAccount(conn, pkey),
			false
		);
		return pkey;
	}

	public static int addLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		int aoServer,
		UnixPath home,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) {
			throw new SQLException("Not allowed to add LinuxServerAccount for user '"+LinuxAccount.MAIL+'\'');
		}
		if(!skipSecurityChecks) {
			checkAccessLinuxAccount(conn, source, "addLinuxServerAccount", username);
			if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add LinuxServerAccount, LinuxAccount disabled: "+username);
			ServerHandler.checkAccessServer(conn, source, "addLinuxServerAccount", aoServer);
			UsernameHandler.checkUsernameAccessServer(conn, source, "addLinuxServerAccount", username, aoServer);
		}

		// OperatingSystem settings
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		if(osv == -1) throw new SQLException("Operating system version not known for server #" + aoServer);
		UnixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
		UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		if(home.equals(LinuxServerAccount.getDefaultHomeDirectory(username))) {
			// Make sure no conflicting /home/u/username account exists.
			String prefix = home + "/";
			List<String> conflicting = conn.executeStringListQuery(
				"select distinct home from linux.\"UserServer\" where ao_server=? and substring(home from 1 for " + prefix.length() + ")=? order by home",
				aoServer,
				prefix
			);
			if(!conflicting.isEmpty()) throw new SQLException("Found conflicting home directories: " + conflicting);
		} else if(home.equals(LinuxServerAccount.getHashedHomeDirectory(username))) {
			// Make sure no conflicting /home/u account exists.
			String conflictHome = "/home/" + username.toString().charAt(0);
			if(
				conn.executeBooleanQuery(
					"select (select pkey from linux.\"UserServer\" where ao_server=? and home=? limit 1) is not null",
					aoServer,
					conflictHome
				)
			) {
				throw new SQLException("Found conflicting home directory: " + conflictHome);
			}
		} else {
			String homeStr = home.toString();
			// Must be in /www/... or /wwwgroup/... (or newer CentOS 7 equivalent of /var/www and /var/opt/apache-tomcat)
			if(
				!homeStr.startsWith(httpdSitesDir + "/")
				&& !homeStr.startsWith(httpdSharedTomcatsDir + "/")
			) throw new SQLException("Invalid home directory: " + home);

			final String SLASH_WEBAPPS = "/webapps";
			if(homeStr.startsWith(httpdSitesDir + "/")) {
				// May also be in /www/(sitename)/webapps
				String siteName = homeStr.substring(httpdSitesDir.toString().length() + 1);
				if(siteName.endsWith(SLASH_WEBAPPS)) {
					siteName = siteName.substring(0, siteName.length() - SLASH_WEBAPPS.length());
				}
				// May be in /www/(sitename)
				int httpdSite = HttpdHandler.getHttpdSite(conn, aoServer, siteName);
				if(httpdSite != -1) {
					if(!skipSecurityChecks) {
						// Must be able to access an existing site
						HttpdHandler.checkAccessHttpdSite(conn, source, "addLinuxServerAccount", httpdSite);
					}
				} else {
					// Must be a valid site name
					if(!HttpdSite.isValidSiteName(siteName)) {
						throw new SQLException("Invalid site name for www home directory: " + home);
					}
				}
			}

			if(homeStr.startsWith(httpdSharedTomcatsDir + "/")) {
				// May also be in /wwwgroup/(tomcatname)/webapps
				String tomcatName = homeStr.substring(httpdSharedTomcatsDir.toString().length() + 1);
				if(tomcatName.endsWith(SLASH_WEBAPPS)) {
					tomcatName = tomcatName.substring(0, tomcatName.length() - SLASH_WEBAPPS.length());
				}
				// May be in /wwwgroup/(tomcatname)
				int httpdSharedTomcat = HttpdHandler.getHttpdSharedTomcat(conn, aoServer, tomcatName);
				if(httpdSharedTomcat != -1) {
					if(!skipSecurityChecks) {
						// Must be able to access an existing site
						HttpdHandler.checkAccessHttpdSharedTomcat(conn, source, "addLinuxServerAccount", httpdSharedTomcat);
					}
				} else {
					// Must be a valid tomcat name
					if(!HttpdSharedTomcat.isValidSharedTomcatName(tomcatName)) {
						throw new SQLException("Invalid shared tomcat name for wwwgroup home directory: " + home);
					}
				}
			}
		}

		// The primary group for this user must exist on this server
		GroupId primaryGroup=getPrimaryLinuxGroup(conn, username, osv);
		int primaryLSG=getLinuxServerGroup(conn, primaryGroup, aoServer);
		if(primaryLSG<0) throw new SQLException("Unable to find primary Linux group '"+primaryGroup+"' on AOServer #"+aoServer+" for Linux account '"+username+"'");

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, aoServer);
		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  linux.\"UserServer\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  linux.get_next_uid(?),\n"
			+ "  ?,\n"
			+ "  null,\n"
			+ "  null,\n"
			+ "  null,\n"
			+ "  false,\n"
			+ "  null,\n"
			+ "  null,\n"
			+ "  now(),\n"
			+ "  true,\n"
			+ "  " + (username.equals(LinuxAccount.EMAILMON) ? "null::int" : Integer.toString(LinuxServerAccount.DEFAULT_TRASH_EMAIL_RETENTION)) + ",\n"
			+ "  " + (username.equals(LinuxAccount.EMAILMON) ? "null::int" : Integer.toString(LinuxServerAccount.DEFAULT_JUNK_EMAIL_RETENTION)) + ",\n"
			+ "  ?,\n"
			+ "  " + LinuxServerAccount.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE + ",\n"
			+ "  " + (username.equals(LinuxAccount.EMAILMON) ? "null::int" : Integer.toString(LinuxServerAccount.DEFAULT_SPAM_ASSASSIN_DISCARD_SCORE)) + ",\n"
			+ "  null\n" // sudo
			+ ") RETURNING pkey",
			username,
			aoServer,
			aoServer,
			home,
			EmailSpamAssassinIntegrationMode.DEFAULT_SPAMASSASSIN_INTEGRATION_MODE
		);
		// Notify all clients of the update
		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			accounting,
			aoServer,
			true
		);
		// If it is a email type, add the default attachment blocks
		if(!username.equals(LinuxAccount.EMAILMON) && isLinuxAccountEmailType(conn, username)) {
			conn.executeUpdate(
				"insert into\n"
				+ "  email.\"AttachmentBlocks\"\n"
				+ "select\n"
				+ "  default,\n"
				+ "  ?,\n"
				+ "  extension\n"
				+ "from\n"
				+ "  email.\"AttachmentType\"\n"
				+ "where\n"
				+ "  is_default_block",
				pkey
			);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.EMAIL_ATTACHMENT_BLOCKS,
				accounting,
				aoServer,
				false
			);
		}
		return pkey;
	}

	public static int addLinuxServerGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		GroupId groupName,
		int aoServer,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(
			groupName.equals(LinuxGroup.FTPONLY)
			|| groupName.equals(LinuxGroup.MAIL)
			|| groupName.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to add LinuxServerGroup for group '"+groupName+'\'');
		AccountingCode accounting = getBusinessForLinuxGroup(conn, groupName);
		if(!skipSecurityChecks) {
			checkAccessLinuxGroup(conn, source, "addLinuxServerGroup", groupName);
			ServerHandler.checkAccessServer(conn, source, "addLinuxServerGroup", aoServer);
			checkLinuxGroupAccessServer(conn, source, "addLinuxServerGroup", groupName, aoServer);
			BusinessHandler.checkBusinessAccessServer(conn, source, "addLinuxServerGroup", accounting, aoServer);
		}

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, aoServer);
		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  linux.\"GroupServer\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  linux.get_next_gid(?),\n"
			+ "  now()\n"
			+ ") RETURNING pkey",
			groupName,
			aoServer,
			aoServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_GROUPS,
			accounting,
			aoServer,
			true
		);
		return pkey;
	}

	/**
	 * Gets the group name that exists on a server for the given gid
	 * or {@code null} if the gid is not allocated to the server.
	 */
	public static GroupId getGroupNameByGid(
		DatabaseConnection conn,
		int aoServer,
		int gid
	) throws SQLException {
		return conn.executeObjectQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			ObjectFactories.groupIdFactory,
			"select name from linux.\"GroupServer\" where ao_server=? and gid=?",
			aoServer,
			gid
		);
	}

	/**
	 * Gets the username that exists on a server for the given uid
	 * or {@code null} if the uid is not allocated to the server.
	 */
	public static UserId getUsernameByUid(
		DatabaseConnection conn,
		int aoServer,
		int uid
	) throws SQLException {
		return conn.executeObjectQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			ObjectFactories.userIdFactory,
			"select username from linux.\"UserServer\" where ao_server=? and uid=?",
			aoServer,
			uid
		);
	}

	public static int addSystemGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		GroupId groupName,
		int gid
	) throws IOException, SQLException {
		// This must be a master user with access to the server
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu == null) throw new SQLException("Not a master user: " + source.getUsername());
		ServerHandler.checkAccessServer(conn, source, "addSystemGroup", aoServer);
		// The group ID must be in the system group range
		if(gid < 0) throw new SQLException("Invalid gid: " + gid);
		int gidMin = AOServerHandler.getGidMin(conn, aoServer);
		int gidMax = AOServerHandler.getGidMax(conn, aoServer);
		// The group ID must not already exist on this server
		{
			GroupId existing = getGroupNameByGid(conn, aoServer, gid);
			if(existing != null) throw new SQLException("Group #" + gid + " already exists on server #" + aoServer + ": " + existing);
		}
		// Must be one of the expected patterns for the servers operating system version
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		if(
			osv == OperatingSystemVersion.CENTOS_7_X86_64
			&& (
				// Fixed group ids
				   (groupName.equals(LinuxGroup.ROOT)            && gid == 0)
				|| (groupName.equals(LinuxGroup.BIN)             && gid == 1)
				|| (groupName.equals(LinuxGroup.DAEMON)          && gid == 2)
				|| (groupName.equals(LinuxGroup.SYS)             && gid == 3)
				|| (groupName.equals(LinuxGroup.ADM)             && gid == 4)
				|| (groupName.equals(LinuxGroup.TTY)             && gid == 5)
				|| (groupName.equals(LinuxGroup.DISK)            && gid == 6)
				|| (groupName.equals(LinuxGroup.LP)              && gid == 7)
				|| (groupName.equals(LinuxGroup.MEM)             && gid == 8)
				|| (groupName.equals(LinuxGroup.KMEM)            && gid == 9)
				|| (groupName.equals(LinuxGroup.WHEEL)           && gid == 10)
				|| (groupName.equals(LinuxGroup.CDROM)           && gid == 11)
				|| (groupName.equals(LinuxGroup.MAIL)            && gid == 12)
				|| (groupName.equals(LinuxGroup.MAN)             && gid == 15)
				|| (groupName.equals(LinuxGroup.DIALOUT)         && gid == 18)
				|| (groupName.equals(LinuxGroup.FLOPPY)          && gid == 19)
				|| (groupName.equals(LinuxGroup.GAMES)           && gid == 20)
				|| (groupName.equals(LinuxGroup.UTMP)            && gid == 22)
				|| (groupName.equals(LinuxGroup.NAMED)           && gid == 25)
				|| (groupName.equals(LinuxGroup.POSTGRES)        && gid == 26)
				|| (groupName.equals(LinuxGroup.RPCUSER)         && gid == 29)
				|| (groupName.equals(LinuxGroup.MYSQL)           && gid == 31)
				|| (groupName.equals(LinuxGroup.RPC)             && gid == 32)
				|| (groupName.equals(LinuxGroup.TAPE)            && gid == 33)
				|| (groupName.equals(LinuxGroup.UTEMPTER)        && gid == 35)
				|| (groupName.equals(LinuxGroup.VIDEO)           && gid == 39)
				|| (groupName.equals(LinuxGroup.DIP)             && gid == 40)
				|| (groupName.equals(LinuxGroup.MAILNULL)        && gid == 47)
				|| (groupName.equals(LinuxGroup.APACHE)          && gid == 48)
				|| (groupName.equals(LinuxGroup.FTP)             && gid == 50)
				|| (groupName.equals(LinuxGroup.SMMSP)           && gid == 51)
				|| (groupName.equals(LinuxGroup.LOCK)            && gid == 54)
				|| (groupName.equals(LinuxGroup.TSS)             && gid == 59)
				|| (groupName.equals(LinuxGroup.AUDIO)           && gid == 63)
				|| (groupName.equals(LinuxGroup.TCPDUMP)         && gid == 72)
				|| (groupName.equals(LinuxGroup.SSHD)            && gid == 74)
				|| (groupName.equals(LinuxGroup.SASLAUTH)        && gid == 76)
				|| (groupName.equals(LinuxGroup.AWSTATS)         && gid == 78)
				|| (groupName.equals(LinuxGroup.DBUS)            && gid == 81)
				|| (groupName.equals(LinuxGroup.MAILONLY)        && gid == 83)
				|| (groupName.equals(LinuxGroup.SCREEN)          && gid == 84)
				|| (groupName.equals(LinuxGroup.BIRD)            && gid == 95)
				|| (groupName.equals(LinuxGroup.NOBODY)          && gid == 99)
				|| (groupName.equals(LinuxGroup.USERS)           && gid == 100)
				|| (groupName.equals(LinuxGroup.AVAHI_AUTOIPD)   && gid == 170)
				|| (groupName.equals(LinuxGroup.DHCPD)           && gid == 177)
				|| (groupName.equals(LinuxGroup.SYSTEMD_JOURNAL) && gid == 190)
				|| (groupName.equals(LinuxGroup.SYSTEMD_NETWORK) && gid == 192)
				|| (groupName.equals(LinuxGroup.NFSNOBODY)       && gid == 65534)
				|| (
					// System groups in range 201 through gidMin - 1
					gid >= CENTOS_7_SYS_GID_MIN
					&& gid < gidMin
					&& (
						   groupName.equals(LinuxGroup.AOSERV_JILTER)
						|| groupName.equals(LinuxGroup.AOSERV_XEN_MIGRATION)
						|| groupName.equals(LinuxGroup.CGRED)
						|| groupName.equals(LinuxGroup.CHRONY)
						|| groupName.equals(LinuxGroup.CLAMSCAN)
						|| groupName.equals(LinuxGroup.CLAMUPDATE)
						|| groupName.equals(LinuxGroup.INPUT)
						|| groupName.equals(LinuxGroup.MEMCACHED)
						|| groupName.equals(LinuxGroup.NGINX)
						|| groupName.equals(LinuxGroup.POLKITD)
						|| groupName.equals(LinuxGroup.SSH_KEYS)
						|| groupName.equals(LinuxGroup.SYSTEMD_BUS_PROXY)
						|| groupName.equals(LinuxGroup.SYSTEMD_NETWORK)
						|| groupName.equals(LinuxGroup.UNBOUND)
						|| groupName.equals(LinuxGroup.VIRUSGROUP)
					)
				) || (
					// Regular user groups in range gidMin through LinuxGroup.GID_MAX
					gid >= gidMin
					&& gid <= gidMax
					&& groupName.equals(LinuxGroup.AOADMIN)
				)
			)
		) {
			int pkey = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  linux.\"GroupServer\"\n"
				+ "VALUES (\n"
				+ "  default,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  now()\n"
				+ ") RETURNING pkey",
				groupName,
				aoServer,
				gid
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.LINUX_SERVER_GROUPS,
				ServerHandler.getBusinessesForServer(conn, aoServer),
				aoServer,
				true
			);
			return pkey;
		} else {
			throw new SQLException("Unexpected system group: " + groupName + " #" + gid + " on operating system #" + osv);
		}
	}

	static class SystemUser {

		static final int ANY_SYSTEM_UID = -1;
		static final int ANY_USER_UID = -2;

		/**
		 * The set of allowed system group patterns for CentOS 7.
		 */
		private static final Map<UserId,SystemUser> centos7SystemUsers = new HashMap<>();
		private static void addCentos7SystemUser(
			UserId username,
			int uid,
			GroupId groupName,
			String fullName,
			String home,
			UnixPath shell,
			String sudo
		) throws ValidationException {
			if(
				centos7SystemUsers.put(
					username,
					new SystemUser(
						username,
						uid,
						groupName,
						InternUtils.intern(Gecos.valueOf(fullName)), null, null, null,
						UnixPath.valueOf(home).intern(),
						shell,
						sudo
					)
				) != null
			) throw new AssertionError("Duplicate username: " + username);
		}
		static {
			try {
				try {
					addCentos7SystemUser(LinuxAccount.ROOT,                           0, LinuxGroup.ROOT,              "root",                        "/root",            Shell.BASH, null);
					addCentos7SystemUser(LinuxAccount.BIN,                            1, LinuxGroup.BIN,               "bin",                         "/bin",             Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.DAEMON,                         2, LinuxGroup.DAEMON,            "daemon",                      "/sbin",            Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.ADM,                            3, LinuxGroup.ADM,               "adm",                         "/var/adm",         Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.LP,                             4, LinuxGroup.LP,                "lp",                          "/var/spool/lpd",   Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SYNC,                           5, LinuxGroup.ROOT,              "sync",                        "/sbin",            Shell.SYNC, null);
					addCentos7SystemUser(LinuxAccount.SHUTDOWN,                       6, LinuxGroup.ROOT,              "shutdown",                    "/sbin",            Shell.SHUTDOWN, null);
					addCentos7SystemUser(LinuxAccount.HALT,                           7, LinuxGroup.ROOT,              "halt",                        "/sbin",            Shell.HALT, null);
					addCentos7SystemUser(LinuxAccount.MAIL,                           8, LinuxGroup.MAIL,              "mail",                        "/var/spool/mail",  Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.OPERATOR,                      11, LinuxGroup.ROOT,              "operator",                    "/root",            Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.GAMES,                         12, LinuxGroup.USERS,             "games",                       "/usr/games",       Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.FTP,                           14, LinuxGroup.FTP,               "FTP User",                    "/var/ftp",         Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.NAMED,                         25, LinuxGroup.NAMED,             "Named",                       "/var/named",       Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.POSTGRES,                      26, LinuxGroup.POSTGRES,          "PostgreSQL Server",           "/var/lib/pgsql",   Shell.BASH, null);
					addCentos7SystemUser(LinuxAccount.RPCUSER,                       29, LinuxGroup.RPCUSER,           "RPC Service User",            "/var/lib/nfs",     Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.MYSQL,                         31, LinuxGroup.MYSQL,             "MySQL server",                "/var/lib/mysql",   Shell.BASH, null);
					addCentos7SystemUser(LinuxAccount.RPC,                           32, LinuxGroup.RPC,               "Rpcbind Daemon",              "/var/lib/rpcbind", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.MAILNULL,                      47, LinuxGroup.MAILNULL,          null,                          "/var/spool/mqueue", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.APACHE,                        48, LinuxGroup.APACHE,            "Apache",                      "/usr/share/httpd", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SMMSP,                         51, LinuxGroup.SMMSP,             null,                          "/var/spool/mqueue", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.TSS,                           59, LinuxGroup.TSS,               "Account used by the trousers package to sandbox the tcsd daemon", "/dev/null", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.TCPDUMP,                       72, LinuxGroup.TCPDUMP,           null,                          "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SSHD,                          74, LinuxGroup.SSHD,              "Privilege-separated SSH",     "/var/empty/sshd",  Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.CYRUS,                         76, LinuxGroup.MAIL,              "Cyrus IMAP Server",           "/var/lib/imap",    Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.AWSTATS,                       78, LinuxGroup.AWSTATS,           "AWStats Background Log Processing", "/var/opt/awstats", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.DBUS,                          81, LinuxGroup.DBUS,              "System message bus",          "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.BIRD,                          95, LinuxGroup.BIRD,              "BIRD Internet Routing Daemon", "/var/opt/bird",   Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.NOBODY,                        99, LinuxGroup.NOBODY,            "Nobody",                      "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.AVAHI_AUTOIPD,                170, LinuxGroup.AVAHI_AUTOIPD,     "Avahi IPv4LL Stack",          "/var/lib/avahi-autoipd", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.DHCPD,                        177, LinuxGroup.DHCPD,             "DHCP server",                 "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SYSTEMD_NETWORK,              192, LinuxGroup.SYSTEMD_NETWORK,   "systemd Network Management",  "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.NFSNOBODY,                  65534, LinuxGroup.NFSNOBODY,         "Anonymous NFS User",          "/var/lib/nfs",     Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.AOSERV_JILTER,     ANY_SYSTEM_UID, LinuxGroup.AOSERV_JILTER,     "AOServ Jilter",               "/var/opt/aoserv-jilter", Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.CHRONY,            ANY_SYSTEM_UID, LinuxGroup.CHRONY,            null,                          "/var/lib/chrony",  Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.CLAMSCAN,          ANY_SYSTEM_UID, LinuxGroup.CLAMSCAN,          "Clamav scanner user",         "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.CLAMUPDATE,        ANY_SYSTEM_UID, LinuxGroup.CLAMUPDATE,        "Clamav database update user", "/var/lib/clamav",  Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.MEMCACHED,         ANY_SYSTEM_UID, LinuxGroup.MEMCACHED,         "Memcached daemon",            "/run/memcached",   Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.NGINX,             ANY_SYSTEM_UID, LinuxGroup.NGINX,             "Nginx web server",            "/var/lib/nginx",   Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.POLKITD,           ANY_SYSTEM_UID, LinuxGroup.POLKITD,           "User for polkitd",            "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SASLAUTH,          ANY_SYSTEM_UID, LinuxGroup.SASLAUTH,          "Saslauthd user",              "/run/saslauthd",   Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.SYSTEMD_BUS_PROXY, ANY_SYSTEM_UID, LinuxGroup.SYSTEMD_BUS_PROXY, "systemd Bus Proxy",           "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.UNBOUND,           ANY_SYSTEM_UID, LinuxGroup.UNBOUND,           "Unbound DNS resolver",        "/etc/unbound",     Shell.NOLOGIN, null);
					addCentos7SystemUser(LinuxAccount.AOADMIN,           ANY_USER_UID,   LinuxGroup.AOADMIN,           "AO Industries Administrator", "/home/aoadmin",    Shell.BASH, AOADMIN_SUDO);
					addCentos7SystemUser(LinuxAccount.AOSERV_XEN_MIGRATION, ANY_SYSTEM_UID, LinuxGroup.AOSERV_XEN_MIGRATION, "AOServ Xen Migration",  "/var/opt/aoserv-xen-migration", Shell.BASH, AOSERV_XEN_MIGRATION_SUDO);
				} catch(ValidationException e) {
					throw new AssertionError("These hard-coded values are valid", e);
				}
			} catch(Throwable t) {
				t.printStackTrace(System.err);
				if(t instanceof RuntimeException) throw (RuntimeException)t;
				if(t instanceof Error) throw (Error)t;
				throw new RuntimeException(t);
			}
		}

		final UserId username;
		final int uid;
		final GroupId groupName;
		final Gecos fullName;
		final Gecos officeLocation;
		final Gecos officePhone;
		final Gecos homePhone;
		final UnixPath home;
		final UnixPath shell;
		final String sudo;

		SystemUser(
			UserId username,
			int uid,
			GroupId groupName,
			Gecos fullName,
			Gecos officeLocation,
			Gecos officePhone,
			Gecos homePhone,
			UnixPath home,
			UnixPath shell,
			String sudo
		) {
			this.username = username;
			this.uid = uid;
			this.groupName = groupName;
			this.fullName = fullName;
			this.officeLocation = officeLocation;
			this.officePhone = officePhone;
			this.homePhone = homePhone;
			this.home = home;
			this.shell = shell;
			this.sudo = sudo;
		}
	}

	public static int addSystemUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		UserId username,
		int uid,
		int gid,
		Gecos fullName,
		Gecos officeLocation,
		Gecos officePhone,
		Gecos homePhone,
		UnixPath home,
		UnixPath shell
	) throws IOException, SQLException {
		// This must be a master user with access to the server
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu == null) throw new SQLException("Not a master user: " + source.getUsername());
		ServerHandler.checkAccessServer(conn, source, "addSystemUser", aoServer);
		// The user ID must be in the system user range
		if(uid < 0) throw new SQLException("Invalid uid: " + uid);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);
		int uidMax = AOServerHandler.getUidMax(conn, aoServer);
		// The user ID must not already exist on this server
		{
			UserId existing = getUsernameByUid(conn, aoServer, uid);
			if(existing != null) throw new SQLException("User #" + uid + " already exists on server #" + aoServer + ": " + existing);
		}
		// Get the group name for the requested gid
		GroupId groupName = getGroupNameByGid(conn, aoServer, gid);
		if(groupName == null) throw new SQLException("Group #" + gid + " does not exist on server #" + aoServer);
		// Must be one of the expected patterns for the servers operating system version
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		SystemUser systemUser;
		if(
			osv == OperatingSystemVersion.CENTOS_7_X86_64
			&& (systemUser = SystemUser.centos7SystemUsers.get(username)) != null
		) {
			if(systemUser.uid == SystemUser.ANY_SYSTEM_UID) {
				// System users in range 201 through uidMin - 1
				if(uid < CENTOS_7_SYS_UID_MIN || uid >= uidMin) throw new SQLException("Invalid system uid: " + uid);
			} else if(systemUser.uid == SystemUser.ANY_USER_UID) {
				// Regular users in range uidMin through LinuxAccount.UID_MAX
				if(uid < uidMin || uid > uidMax) throw new SQLException("Invalid regular user uid: " + uid);
			} else {
				// UID must match exactly
				if(uid != systemUser.uid) throw new SQLException("Unexpected system uid: " + uid + " != " + systemUser.uid);
			}
			// Check other fields match
			if(!ObjectUtils.equals(groupName,      systemUser.groupName))      throw new SQLException("Unexpected system group: "          + groupName      + " != " + systemUser.groupName);
			if(!ObjectUtils.equals(fullName,       systemUser.fullName))       throw new SQLException("Unexpected system fullName: "       + fullName       + " != " + systemUser.fullName);
			if(!ObjectUtils.equals(officeLocation, systemUser.officeLocation)) throw new SQLException("Unexpected system officeLocation: " + officeLocation + " != " + systemUser.officeLocation);
			if(!ObjectUtils.equals(officePhone,    systemUser.officePhone))    throw new SQLException("Unexpected system officePhone: "    + officePhone    + " != " + systemUser.officePhone);
			if(!ObjectUtils.equals(homePhone,      systemUser.homePhone))      throw new SQLException("Unexpected system homePhone: "      + homePhone      + " != " + systemUser.homePhone);
			if(!ObjectUtils.equals(home,           systemUser.home))           throw new SQLException("Unexpected system home: "           + home           + " != " + systemUser.home);
			if(!ObjectUtils.equals(shell,          systemUser.shell))          throw new SQLException("Unexpected system shell: "          + shell          + " != " + systemUser.shell);
			// Add to database
			int pkey = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  linux.\"UserServer\"\n"
				+ "VALUES (\n"
				+ "  default,\n" // pkey
				+ "  ?,\n" // username
				+ "  ?,\n" // ao_server
				+ "  ?,\n" // uid
				+ "  ?,\n" // home
				+ "  null,\n" // autoresponder_from
				+ "  null,\n" // autoresponder_subject
				+ "  null,\n" // autoresponder_path
				+ "  false,\n" // is_autoresponder_enabled
				+ "  null,\n" // disable_log
				+ "  null,\n" // predisable_password
				+ "  now(),\n" // created
				+ "  true,\n" // use_inbox
				+ "  null,\n" // trash_email_retention
				+ "  null,\n" // junk_email_retention
				+ "  ?,\n" // sa_integration_mode
				+ "  "+LinuxServerAccount.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE+",\n"
				+ "  null,\n" // sa_discard_score
				+ "  ?\n" // sudo
				+ ") RETURNING pkey",
				username,
				aoServer,
				uid,
				home,
				EmailSpamAssassinIntegrationMode.NONE,
				systemUser.sudo
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
				ServerHandler.getBusinessesForServer(conn, aoServer),
				aoServer,
				true
			);
			return pkey;
		} else {
			throw new SQLException("Unexpected system user: " + username + " #" + uid + " on operating system #" + osv);
		}
	}

	/**
	 * Copies the contents of a home directory from one server to another.
	 */
	public static long copyHomeDirectory(
		DatabaseConnection conn,
		RequestSource source,
		int from_lsa,
		int to_server
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "copyHomeDirectory", from_lsa);
		UserId username=getUsernameForLinuxServerAccount(conn, from_lsa);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to copy LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		int from_server=getAOServerForLinuxServerAccount(conn, from_lsa);
		int to_lsa=conn.executeIntQuery(
			"select pkey from linux.\"UserServer\" where username=? and ao_server=?",
			username,
			to_server
		);
		checkAccessLinuxServerAccount(conn, source, "copyHomeDirectory", to_lsa);
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(LinuxAccountType.USER)
			&& !type.equals(LinuxAccountType.EMAIL)
			&& !type.equals(LinuxAccountType.FTPONLY)
		) throw new SQLException("Not allowed to copy LinuxAccounts of type '"+type+"', username="+username);

		long byteCount=DaemonHandler.getDaemonConnector(conn, from_server).copyHomeDirectory(username, DaemonHandler.getDaemonConnector(conn, to_server));
		return byteCount;
	}

	/**
	 * Copies a password from one linux account to another
	 */
	public static void copyLinuxServerAccountPassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int from_lsa,
		int to_lsa
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "copyLinuxServerAccountPassword", from_lsa);
		if(isLinuxServerAccountDisabled(conn, from_lsa)) throw new SQLException("Unable to copy LinuxServerAccount password, from account disabled: "+from_lsa);
		UserId from_username=getUsernameForLinuxServerAccount(conn, from_lsa);
		if(from_username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to copy the password from LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		checkAccessLinuxServerAccount(conn, source, "copyLinuxServerAccountPassword", to_lsa);
		if(isLinuxServerAccountDisabled(conn, to_lsa)) throw new SQLException("Unable to copy LinuxServerAccount password, to account disabled: "+to_lsa);
		UserId to_username=getUsernameForLinuxServerAccount(conn, to_lsa);
		if(to_username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to copy the password to LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		int from_server=getAOServerForLinuxServerAccount(conn, from_lsa);
		int to_server=getAOServerForLinuxServerAccount(conn, to_lsa);

		String from_type=getTypeForLinuxAccount(conn, from_username);
		if(
			!from_type.equals(LinuxAccountType.APPLICATION)
			&& !from_type.equals(LinuxAccountType.USER)
			&& !from_type.equals(LinuxAccountType.EMAIL)
			&& !from_type.equals(LinuxAccountType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords from LinuxAccounts of type '"+from_type+"', username="+from_username);

		String to_type=getTypeForLinuxAccount(conn, to_username);
		if(
			!to_type.equals(LinuxAccountType.APPLICATION)
			&& !to_type.equals(LinuxAccountType.USER)
			&& !to_type.equals(LinuxAccountType.EMAIL)
			&& !to_type.equals(LinuxAccountType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords to LinuxAccounts of type '"+to_type+"', username="+to_username);

		Tuple2<String,Integer> enc_password = DaemonHandler.getDaemonConnector(conn, from_server).getEncryptedLinuxAccountPassword(from_username);
		DaemonHandler.getDaemonConnector(conn, to_server).setEncryptedLinuxAccountPassword(to_username, enc_password.getElement1(), enc_password.getElement2());

		//AccountingCode from_accounting=UsernameHandler.getBusinessForUsername(conn, from_username);
		//AccountingCode to_accounting=UsernameHandler.getBusinessForUsername(conn, to_username);
	}

	public static void disableLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		UserId username
	) throws IOException, SQLException {
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("linux.User is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableLinuxAccount", disableLog, false);
		checkAccessLinuxAccount(conn, source, "disableLinuxAccount", username);
		IntList lsas=getLinuxServerAccountsForLinuxAccount(conn, username);
		for(int c=0;c<lsas.size();c++) {
			int lsa=lsas.getInt(c);
			if(!isLinuxServerAccountDisabled(conn, lsa)) {
				throw new SQLException("Cannot disable LinuxAccount '"+username+"': LinuxServerAccount not disabled: "+lsa);
			}
		}

		conn.executeUpdate(
			"update linux.\"User\" set disable_log=? where username=?",
			disableLog,
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	public static void disableLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isLinuxServerAccountDisabled(conn, pkey)) throw new SQLException("linux.UserServer is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableLinuxServerAccount", disableLog, false);
		checkAccessLinuxServerAccount(conn, source, "disableLinuxServerAccount", pkey);

		int aoServer = getAOServerForLinuxServerAccount(conn, pkey);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);

		// The UID must be a user UID
		int uid=getUIDForLinuxServerAccount(conn, pkey);
		if(uid < uidMin) throw new SQLException("Not allowed to disable a system LinuxServerAccount: pkey="+pkey+", uid="+uid);

		IntList crs=CvsHandler.getCvsRepositoriesForLinuxServerAccount(conn, pkey);
		for(int c=0;c<crs.size();c++) {
			int cr=crs.getInt(c);
			if(!CvsHandler.isCvsRepositoryDisabled(conn, cr)) {
				throw new SQLException("Cannot disable LinuxServerAccount #"+pkey+": CvsRepository not disabled: "+cr);
			}
		}
		IntList hsts=HttpdHandler.getHttpdSharedTomcatsForLinuxServerAccount(conn, pkey);
		for(int c=0;c<hsts.size();c++) {
			int hst=hsts.getInt(c);
			if(!HttpdHandler.isHttpdSharedTomcatDisabled(conn, hst)) {
				throw new SQLException("Cannot disable LinuxServerAccount #"+pkey+": HttpdSharedTomcat not disabled: "+hst);
			}
		}
		IntList hss=HttpdHandler.getHttpdSitesForLinuxServerAccount(conn, pkey);
		for(int c=0;c<hss.size();c++) {
			int hs=hss.getInt(c);
			if(!HttpdHandler.isHttpdSiteDisabled(conn, hs)) {
				throw new SQLException("Cannot disable LinuxServerAccount #"+pkey+": HttpdSite not disabled: "+hs);
			}
		}
		IntList els=EmailHandler.getEmailListsForLinuxServerAccount(conn, pkey);
		for(int c=0;c<els.size();c++) {
			int el=els.getInt(c);
			if(!EmailHandler.isEmailListDisabled(conn, el)) {
				throw new SQLException("Cannot disable LinuxServerAccount #"+pkey+": EmailList not disabled: "+el);
			}
		}

		conn.executeUpdate(
			"update linux.\"UserServer\" set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			aoServer,
			false
		);
	}

	public static void enableLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForLinuxAccount(conn, username);
		if(disableLog==-1) throw new SQLException("linux.User is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableLinuxAccount", disableLog, true);
		checkAccessLinuxAccount(conn, source, "enableLinuxAccount", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable LinuxAccount '"+username+"', Username not enabled: "+username);

		conn.executeUpdate(
			"update linux.\"User\" set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	public static void enableLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForLinuxServerAccount(conn, pkey);
		if(disableLog==-1) throw new SQLException("linux.UserServer is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableLinuxServerAccount", disableLog, true);
		checkAccessLinuxServerAccount(conn, source, "enableLinuxServerAccount", pkey);
		UserId la=getUsernameForLinuxServerAccount(conn, pkey);
		if(isLinuxAccountDisabled(conn, la)) throw new SQLException("Unable to enable LinuxServerAccount #"+pkey+", LinuxAccount not enabled: "+la);

		conn.executeUpdate(
			"update linux.\"UserServer\" set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, la),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	/**
	 * Gets the contents of an autoresponder.
	 */
	public static String getAutoresponderContent(DatabaseConnection conn, RequestSource source, int lsa) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "getAutoresponderContent", lsa);
		UserId username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to get the autoresponder content for LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		UnixPath path = conn.executeObjectQuery(
			ObjectFactories.unixPathFactory,
			"select autoresponder_path from linux.\"UserServer\" where pkey=?",
			lsa
		);
		String content;
		if(path == null) {
			content="";
		} else {
			int aoServer = getAOServerForLinuxServerAccount(conn, lsa);
			content = DaemonHandler.getDaemonConnector(conn, aoServer).getAutoresponderContent(path);
		}
		return content;
	}

	/**
	 * Gets the contents of a user cron table.
	 */
	public static String getCronTable(DatabaseConnection conn, RequestSource source, int lsa) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "getCronTable", lsa);
		UserId username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to get the cron table for LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(LinuxAccountType.USER)
		) throw new SQLException("Not allowed to get the cron table for LinuxAccounts of type '"+type+"', username="+username);
		int aoServer=getAOServerForLinuxServerAccount(conn, lsa);

		return DaemonHandler.getDaemonConnector(conn, aoServer).getCronTable(username);
	}

	public static int getDisableLogForLinuxAccount(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from linux.\"User\" where username=?", username);
	}

	public static int getDisableLogForLinuxServerAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from linux.\"UserServer\" where pkey=?", pkey);
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.LINUX_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledLinuxAccounts.clear();
			}
		} else if(tableID==SchemaTable.TableID.LINUX_SERVER_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledLinuxServerAccounts.clear();
			}
		}
	}

	public static boolean isLinuxAccount(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      username\n"
			+ "    from\n"
			+ "      linux.\"User\"\n"
			+ "    where\n"
			+ "      username=?\n"
			+ "    limit 1\n"
			+ "  ) is not null",
			username
		);
	}

	public static boolean isLinuxAccountDisabled(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Boolean O=disabledLinuxAccounts.get(username);
			if(O != null) return O;
			boolean isDisabled=getDisableLogForLinuxAccount(conn, username)!=-1;
			disabledLinuxAccounts.put(username, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isLinuxAccountEmailType(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  lat.is_email\n"
			+ "from\n"
			+ "  linux.\"User\" la,\n"
			+ "  linux.\"UserType\" lat\n"
			+ "where\n"
			+ "  la.username=?\n"
			+ "  and la.type=lat.name",
			username
		);
	}

	public static boolean isLinuxServerAccountDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Integer I=pkey;
			Boolean O=disabledLinuxServerAccounts.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForLinuxServerAccount(conn, pkey)!=-1;
			disabledLinuxServerAccounts.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isLinuxGroupNameAvailable(DatabaseConnection conn, GroupId groupname) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select name from linux.\"Group\" where name=?) is null", groupname);
	}

	public static int getLinuxServerGroup(DatabaseConnection conn, GroupId group, int aoServer) throws IOException, SQLException {
		int pkey=conn.executeIntQuery("select coalesce((select pkey from linux.\"GroupServer\" where name=? and ao_server=?), -1)", group, aoServer);
		if(pkey==-1) throw new SQLException("Unable to find LinuxServerGroup "+group+" on "+aoServer);
		return pkey;
	}

	public static GroupId getPrimaryLinuxGroup(DatabaseConnection conn, UserId username, int operatingSystemVersion) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.groupIdFactory,
			"select\n"
			+ "  \"group\"\n"
			+ "from\n"
			+ "  linux.\"GroupUser\"\n"
			+ "where\n"
			+ "  username=?\n"
			+ "  and is_primary\n"
			+ "  and (\n"
			+ "    operating_system_version is null\n"
			+ "    or operating_system_version=?\n"
		    + ")",
			username,
			operatingSystemVersion
		);
	}

	public static boolean isLinuxServerAccountPasswordSet(
		DatabaseConnection conn,
		RequestSource source, 
		int account
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "isLinuxServerAccountPasswordSet", account);
		UserId username=getUsernameForLinuxServerAccount(conn, account);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to check if a password is set for LinuxServerAccount '"+LinuxAccount.MAIL+'\'');

		int aoServer=getAOServerForLinuxServerAccount(conn, account);
		String crypted = DaemonHandler.getDaemonConnector(conn, aoServer).getEncryptedLinuxAccountPassword(username).getElement1();
		return crypted.length() >= 2 && !LinuxAccount.NO_PASSWORD_CONFIG_VALUE.equals(crypted);
	}

	public static int isLinuxServerAccountProcmailManual(
		DatabaseConnection conn,
		RequestSource source, 
		int account
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "isLinuxServerAccountProcmailManual", account);

		int aoServer=getAOServerForLinuxServerAccount(conn, account);
		if(DaemonHandler.isDaemonAvailable(aoServer)) {
			try {
				return DaemonHandler.getDaemonConnector(conn, aoServer).isProcmailManual(account) ? AOServProtocol.TRUE : AOServProtocol.FALSE;
			} catch(IOException err) {
				DaemonHandler.flagDaemonAsDown(aoServer);
				return AOServProtocol.SERVER_DOWN;
			}
		} else {
			return AOServProtocol.SERVER_DOWN;
		}
	}

	public static void removeLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "removeLinuxAccount", username);

		removeLinuxAccount(conn, invalidateList, username);
	}

	public static void removeLinuxAccount(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		UserId username
	) throws IOException, SQLException {
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove LinuxAccount with username '"+LinuxAccount.MAIL+'\'');

		// Detach the linux account from its autoresponder address
		IntList aoServers=getAOServersForLinuxAccount(conn, username);
		for(int c=0;c<aoServers.size();c++) {
			int aoServer=aoServers.getInt(c);
			conn.executeUpdate("update linux.\"UserServer\" set autoresponder_from=null where username=? and ao_server=?", username, aoServer);
		}
		// Delete any FTP guest user info attached to this account
		boolean ftpModified = conn.executeUpdate("delete from ftp.\"GuestUser\" where username=?", username) > 0;
		// Delete the account from all servers
		// Get the values for later use
		for(int c=0;c<aoServers.size();c++) {
			int aoServer=aoServers.getInt(c);
			int pkey=conn.executeIntQuery("select pkey from linux.\"UserServer\" where username=? and ao_server=?", username, aoServer);
			removeLinuxServerAccount(conn, invalidateList, pkey);
		}
		// Delete the group relations for this account
		boolean groupAccountModified = conn.executeUpdate("delete from linux.\"GroupUser\" where username=?", username) > 0;
		// Delete from the database
		conn.executeUpdate("delete from linux.\"User\" where username=?", username);

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);

		if(ftpModified) invalidateList.addTable(conn, SchemaTable.TableID.FTP_GUEST_USERS, accounting, aoServers, false);
		if(groupAccountModified) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, accounting, aoServers, false);
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void removeLinuxGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		GroupId name
	) throws IOException, SQLException {
		checkAccessLinuxGroup(conn, source, "removeLinuxGroup", name);

		removeLinuxGroup(conn, invalidateList, name);
	}

	public static void removeLinuxGroup(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		GroupId name
	) throws IOException, SQLException {
		if(
			name.equals(LinuxGroup.FTPONLY)
			|| name.equals(LinuxGroup.MAIL)
			|| name.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to remove LinuxGroup named '"+name+"'");

		// Must not be the primary group for any LinuxAccount
		int primaryCount=conn.executeIntQuery("select count(*) from linux.\"GroupUser\" where \"group\"=? and is_primary", name);
		if(primaryCount>0) throw new SQLException("linux_group.name="+name+" is the primary group for "+primaryCount+" Linux "+(primaryCount==1?"account":"accounts"));
		// Get the values for later use
		AccountingCode accounting = getBusinessForLinuxGroup(conn, name);
		IntList aoServers=getAOServersForLinuxGroup(conn, name);
		for(int c=0;c<aoServers.size();c++) {
			int aoServer=aoServers.getInt(c);
			conn.executeUpdate("delete from linux.\"GroupServer\" where name=? and ao_server=?", name, aoServer);
		}
		// Delete the group relations for this group
		boolean groupAccountsModified=conn.executeIntQuery("select count(*) from linux.\"GroupUser\" where \"group\"=? limit 1", name)>0;
		if(groupAccountsModified) conn.executeUpdate("delete from linux.\"GroupUser\" where \"group\"=?", name);
		// Delete from the database
		conn.executeUpdate("delete from linux.\"Group\" where name=?", name);

		// Notify all clients of the update
		if(aoServers.size()>0) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_SERVER_GROUPS, accounting, aoServers, false);
		if(groupAccountsModified) invalidateList.addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, accounting, aoServers, false);
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_GROUPS, accounting, aoServers, false);
	}

	public static void removeLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessLinuxGroupAccount(conn, source, "removeLinuxGroupAccount", pkey);

		// Must not be a primary group
		boolean isPrimary=conn.executeBooleanQuery("select is_primary from linux.\"GroupUser\" where pkey=?", pkey);
		if(isPrimary) throw new SQLException("linux.GroupUser.pkey="+pkey+" is a primary group");

		// Must be needingful not by HttpdTomcatSharedSite to be tying to HttpdSharedTomcat please
		int useCount = conn.executeIntQuery(
			"select count(*) from linux.\"GroupUser\" lga, "+
					"linux.\"UserServer\" lsa, "+
					"web.\"SharedTomcat\" hst, "+
					"web.\"SharedTomcatSite\" htss, "+
					"web.\"Site\" hs "+
						"where lga.username = lsa.username and "+
						"lsa.pkey           = hst.linux_server_account and "+
						"htss.tomcat_site   = hs.pkey and "+
						"lga.\"group\"      = hs.linux_group and "+
						"hst.pkey           = htss.httpd_shared_tomcat and "+
						"lga.pkey = ?",
			pkey
		);
		if (useCount==0) {
			useCount = conn.executeIntQuery(
				"select count(*) from linux.\"GroupUser\" lga, "+
						"linux.\"GroupServer\" lsg, "+
						"web.\"SharedTomcat\" hst, "+
						"web.\"SharedTomcatSite\" htss, "+
						"web.\"Site\" hs "+
							"where lga.\"group\" = lsg.name and "+
							"lsg.pkey            = hst.linux_server_group and "+
							"htss.tomcat_site    = hs.pkey and "+
							"lga.username        = hs.linux_account and "+
							"hst.pkey            = htss.httpd_shared_tomcat and "+
							"lga.pkey = ?",
				pkey
			);
		}
		if (useCount>0) throw new SQLException("linux_group_account("+pkey+") has been used by "+useCount+" web.SharedTomcatSite.");

		// Get the values for later use
		List<AccountingCode> accountings=getBusinessesForLinuxGroupAccount(conn, pkey);
		IntList aoServers=getAOServersForLinuxGroupAccount(conn, pkey);
		// Delete the group relations for this group
		conn.executeUpdate("delete from linux.\"GroupUser\" where pkey=?", pkey);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, accountings, aoServers, false);
	}

	public static void removeUnusedAlternateLinuxGroupAccount(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		GroupId group,
		UserId username
	) throws IOException, SQLException {
		int pkey=conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        lga.pkey\n"
			+ "      from\n"
			+ "        linux.\"GroupUser\" lga\n"
			+ "      where\n"
			+ "        lga.\"group\"=?\n"
			+ "        and lga.username=?\n"
			+ "        and not lga.is_primary\n"
			+ "        and (\n"
			+ "          select\n"
			+ "            htss.tomcat_site\n"
			+ "          from\n"
			+ "            linux.\"UserServer\" lsa,\n"
			+ "            web.\"SharedTomcat\" hst,\n"
			+ "            web.\"SharedTomcatSite\" htss,\n"
			+ "            web.\"Site\" hs\n"
			+ "          where\n"
			+ "            lga.username=lsa.username\n"
			+ "            and lsa.pkey=hst.linux_server_account\n"
			+ "            and hst.pkey=htss.httpd_shared_tomcat\n"
			+ "            and htss.tomcat_site=hs.pkey\n"
			+ "            and hs.linux_group=lga.\"group\"\n"
			+ "          limit 1\n"
			+ "        ) is null\n"
			+ "        and (\n"
			+ "          select\n"
			+ "            htss.tomcat_site\n"
			+ "          from\n"
			+ "            linux.\"GroupServer\" lsg,\n"
			+ "            web.\"SharedTomcat\" hst,\n"
			+ "            web.\"SharedTomcatSite\" htss,\n"
			+ "            web.\"Site\" hs\n"
			+ "          where\n"
			+ "            lga.\"group\"=lsg.name\n"
			+ "            and lsg.pkey=hst.linux_server_group\n"
			+ "            and hst.pkey=htss.httpd_shared_tomcat\n"
			+ "            and htss.tomcat_site=hs.pkey\n"
			+ "            and hs.linux_account=lga.username\n"
			+ "          limit 1\n"
			+ "        ) is null\n"
			+ "    ),\n"
			+ "    -1\n"
			+ "  )",
			group,
			username
		);
		if(pkey!=-1) {
			// Get the values for later use
			List<AccountingCode> accountings=getBusinessesForLinuxGroupAccount(conn, pkey);
			IntList aoServers=getAOServersForLinuxGroupAccount(conn, pkey);
			conn.executeUpdate("delete from linux.\"GroupUser\" where pkey=?", pkey);

			// Notify all clients of the update
			invalidateList.addTable(conn, SchemaTable.TableID.LINUX_GROUP_ACCOUNTS, accountings, aoServers, false);
		}
	}

	public static void removeLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int account
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "removeLinuxServerAccount", account);

		removeLinuxServerAccount(conn, invalidateList, account);
	}

	public static void removeLinuxServerAccount(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int account
	) throws IOException, SQLException {
		UserId username=getUsernameForLinuxServerAccount(conn, account);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove LinuxServerAccount for user '"+LinuxAccount.MAIL+'\'');

		int aoServer = getAOServerForLinuxServerAccount(conn, account);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);

		// The UID must be a user UID
		int uid=getUIDForLinuxServerAccount(conn, account);
		if(uid < uidMin) throw new SQLException("Not allowed to remove a system LinuxServerAccount: pkey="+account+", uid="+uid);

		// Must not contain a CVS repository
		String home=conn.executeStringQuery("select home from linux.\"UserServer\" where pkey=?", account);
		int count=conn.executeIntQuery(
			"select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  cvs_repositories cr\n"
			+ "where\n"
			+ "  linux_server_account=?\n"
			+ "  and (\n"
			+ "    path=?\n"
			+ "    or substring(path from 1 for "+(home.length()+1)+")=?\n"
			+ "  )",
			account,
			home,
			home+'/'
		);
		if(count>0) throw new SQLException("Home directory on "+aoServer+" contains "+count+" CVS "+(count==1?"repository":"repositories")+": "+home);

		// Delete the email configurations that depend on this account
		IntList addresses=conn.executeIntListQuery("select email_address from email.\"InboxAddress\" where linux_server_account=?", account);
		int size=addresses.size();
		boolean addressesModified=size>0;
		for(int c=0;c<size;c++) {
			int address=addresses.getInt(c);
			conn.executeUpdate("delete from email.\"InboxAddress\" where email_address=?", address);
			if(!EmailHandler.isEmailAddressUsed(conn, address)) {
				conn.executeUpdate("delete from email.\"Address\" where pkey=?", address);
			}
		}

		AccountingCode accounting = getBusinessForLinuxServerAccount(conn, account);

		// Delete the attachment blocks
		conn.executeUpdate("delete from email.\"AttachmentBlocks\" where linux_server_account=?", account);
		invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ATTACHMENT_BLOCKS, accounting, aoServer, false);

		// Delete the account from the server
		conn.executeUpdate("delete from linux.\"UserServer\" where pkey=?", account);
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_SERVER_ACCOUNTS, accounting, aoServer, true);

		// Notify all clients of the update
		if(addressesModified) {
			invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
		}
	}

	public static void removeLinuxServerGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int group
	) throws IOException, SQLException {
		checkAccessLinuxServerGroup(conn, source, "removeLinuxServerGroup", group);

		removeLinuxServerGroup(conn, invalidateList, group);
	}

	public static void removeLinuxServerGroup(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int group
	) throws IOException, SQLException {
		GroupId groupName=getGroupNameForLinuxServerGroup(conn, group);
		if(
			groupName.equals(LinuxGroup.FTPONLY)
			|| groupName.equals(LinuxGroup.MAIL)
			|| groupName.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to remove LinuxServerGroup for group '"+groupName+"'");

		// Get the server this group is on
		AccountingCode accounting = getBusinessForLinuxServerGroup(conn, group);
		int aoServer=getAOServerForLinuxServerGroup(conn, group);
		// Must not be the primary group for any LinuxServerAccount on the same server
		int primaryCount=conn.executeIntQuery(
			"select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg\n"
			+ "  inner join linux.\"GroupUser\" lga on lsg.name=lga.\"group\"\n"
			+ "  inner join linux.\"UserServer\" lsa on lga.username=lsa.username\n"
			+ "  inner join servers se on lsg.ao_server=se.pkey\n"
			+ "where\n"
			+ "  lsg.pkey=?\n"
			+ "  and lga.is_primary\n"
			+ "  and (\n"
			+ "    lga.operating_system_version is null\n"
			+ "    or lga.operating_system_version=se.operating_system_version\n"
			+ "  )\n"
			+ "  and lsg.ao_server=lsa.ao_server",
			group
		);

		if(primaryCount>0) throw new SQLException("linux_server_group.pkey="+group+" is the primary group for "+primaryCount+" Linux server "+(primaryCount==1?"account":"accounts")+" on "+aoServer);
		// Delete from the database
		conn.executeUpdate("delete from linux.\"GroupServer\" where pkey=?", group);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_SERVER_GROUPS, accounting, aoServer, true);
	}

	public static void setAutoresponder(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int from,
		String subject,
		String content,
		boolean enabled
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "setAutoresponder", pkey);
		if(isLinuxServerAccountDisabled(conn, pkey)) throw new SQLException("Unable to set autoresponder, LinuxServerAccount disabled: "+pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set autoresponder for user '"+LinuxAccount.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(LinuxAccountType.EMAIL)
			&& !type.equals(LinuxAccountType.USER)
		) throw new SQLException("Not allowed to set autoresponder for this type of account: "+type);

		// The from must be on this account
		if(from!=-1) {
			int fromLSA=conn.executeIntQuery("select linux_server_account from email.\"InboxAddress\" where pkey=?", from);
			if(fromLSA!=pkey) throw new SQLException("((linux_acc_address.pkey="+from+").linux_server_account="+fromLSA+")!=((linux_server_account.pkey="+pkey+").username="+username+")");
		}

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		int aoServer=getAOServerForLinuxServerAccount(conn, pkey);
		UnixPath path;
		if(content==null && !enabled) {
			path = null;
		} else {
			path = conn.executeObjectQuery(
				ObjectFactories.unixPathFactory,
				"select coalesce(autoresponder_path, home || '/.autorespond.txt') from linux.\"UserServer\" where pkey=?",
				pkey
			);
		}
		int uid;
		int gid;
		if(!enabled) {
			uid=-1;
			gid=-1;
		} else {
			uid = getUIDForLinuxServerAccount(conn, pkey);
			gid = conn.executeIntQuery(
				"select\n"
				+ "  lsg.gid\n"
				+ "from\n"
				+ "  linux.\"UserServer\" lsa\n"
				+ "  inner join linux.\"GroupUser\" lga on lsa.username=lga.username\n"
				+ "  inner join linux.\"GroupServer\" lsg on lga.\"group\"=lsg.name\n"
				+ "  inner join servers se on lsa.ao_server=se.pkey\n"
				+ "where\n"
				+ "  lsa.pkey=?\n"
				+ "  and lga.is_primary\n"
				+ "  and (\n"
				+ "    lga.operating_system_version is null\n"
				+ "    or lga.operating_system_version=se.operating_system_version\n"
				+ "  )\n"
				+ "  and lsa.ao_server=lsg.ao_server",
				pkey
			);
		}
		try (
			PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
				"update\n"
				+ "  linux.\"UserServer\"\n"
				+ "set\n"
				+ "  autoresponder_from=?,\n"
				+ "  autoresponder_subject=?,\n"
				+ "  autoresponder_path=?,\n"
				+ "  is_autoresponder_enabled=?\n"
				+ "where\n"
				+ "  pkey=?"
			)
		) {
			try {
				if(from==-1) pstmt.setNull(1, Types.INTEGER);
				else pstmt.setInt(1, from);
				pstmt.setString(2, subject);
				pstmt.setString(3, ObjectUtils.toString(path));
				pstmt.setBoolean(4, enabled);
				pstmt.setInt(5, pkey);
				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

		// Store the content on the server
		if(path!=null) DaemonHandler.getDaemonConnector(
			conn,
			aoServer
		).setAutoresponderContent(
			path,
			content==null?"":content,
			uid,
			gid
		);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_SERVER_ACCOUNTS, accounting, aoServer, false);
	}

	/**
	 * Gets the contents of a user cron table.
	 */
	public static void setCronTable(
		DatabaseConnection conn,
		RequestSource source,
		int lsa,
		String cronTable
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "setCronTable", lsa);
		if(isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to set cron table, LinuxServerAccount disabled: "+lsa);
		UserId username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the cron table for LinuxAccount named '"+LinuxAccount.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(LinuxAccountType.USER)
		) throw new SQLException("Not allowed to set the cron table for LinuxAccounts of type '"+type+"', username="+username);
		int aoServer=getAOServerForLinuxServerAccount(conn, lsa);

		DaemonHandler.getDaemonConnector(conn, aoServer).setCronTable(username, cronTable);
	}

	public static void setLinuxAccountHomePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountHomePhone", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set home phone number, LinuxAccount disabled: "+username);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set home phone number for user '"+LinuxAccount.MAIL+'\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set home_phone=? where username=?", phone, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountName(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		Gecos name
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountName", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set full name, LinuxAccount disabled: "+username);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set LinuxAccountName for user '"+LinuxAccount.MAIL+'\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set name=? where username=?", name, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountOfficeLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		Gecos location
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficeLocation", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set office location, LinuxAccount disabled: "+username);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set office location for user '"+LinuxAccount.MAIL+'\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set office_location=? where username=?", location, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountOfficePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficePhone", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set office phone number, LinuxAccount disabled: "+username);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set office phone number for user '"+LinuxAccount.MAIL+'\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set office_phone=? where username=?", phone, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountShell(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		UserId username,
		UnixPath shell
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficeShell", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set shell, LinuxAccount disabled: "+username);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set shell for account named '"+LinuxAccount.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(!LinuxAccountType.isAllowedShell(type, shell)) throw new SQLException("Shell '"+shell+"' not allowed for Linux accounts with the type '"+type+'\'');

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set shell=? where username=?", shell, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, SchemaTable.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxServerAccountPassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		String password
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "setLinuxServerAccountPassword", AOServPermission.Permission.set_linux_server_account_password);
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountPassword", pkey);
		if(isLinuxServerAccountDisabled(conn, pkey)) throw new SQLException("Unable to set LinuxServerAccount password, account disabled: "+pkey);

		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set password for LinuxServerAccount named '"+LinuxAccount.MAIL+"': "+pkey);
		String type=conn.executeStringQuery("select type from linux.\"User\" where username=?", username);

		// Make sure passwords can be set before doing a strength check
		if(!LinuxAccountType.canSetPassword(type)) throw new SQLException("Passwords may not be set for LinuxAccountType="+type);

		if(password!=null && password.length()>0) {
			// Perform the password check here, too.
			List<PasswordChecker.Result> results = LinuxAccount.checkPassword(username, type, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		AccountingCode accounting = UsernameHandler.getBusinessForUsername(conn, username);
		int aoServer=getAOServerForLinuxServerAccount(conn, pkey);
		try {
			DaemonHandler.getDaemonConnector(conn, aoServer).setLinuxServerAccountPassword(username, password);
		} catch(IOException | SQLException err) {
			System.err.println("Unable to set linux account password for "+username+" on "+aoServer);
			throw err;
		}

		// Update the linux.Server table for emailmon and ftpmon
		/*if(username.equals(LinuxAccount.EMAILMON)) {
			conn.executeUpdate("update linux."Server" set emailmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
			invalidateList.addTable(conn, SchemaTable.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
		} else if(username.equals(LinuxAccount.FTPMON)) {
			conn.executeUpdate("update linux."Server" set ftpmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
			invalidateList.addTable(conn, SchemaTable.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
		}*/
	}

	public static void setLinuxServerAccountPredisablePassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int lsa,
		String password
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountPredisablePassword", lsa);
		if(password==null) {
			if(isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to clear LinuxServerAccount predisable password, account disabled: "+lsa);
		} else {
			if(!isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to set LinuxServerAccount predisable password, account not disabled: "+lsa);
		}

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set predisable_password=? where pkey=?",
			password,
			lsa
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, lsa),
			getAOServerForLinuxServerAccount(conn, lsa),
			false
		);
	}

	public static void setLinuxServerAccountJunkEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountJunkEmailRetention", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the junk email retention for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set junk_email_retention=null where pkey=?",
				pkey
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set junk_email_retention=? where pkey=?",
				days,
				pkey
			);
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinIntegrationMode(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		String mode
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinIntegrationMode", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the spam assassin integration mode for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set sa_integration_mode=? where pkey=?",
			mode,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinRequiredScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		float required_score
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinRequiredScore", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the spam assassin required score for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set sa_required_score=? where pkey=?",
			required_score,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinDiscardScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int discard_score
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinDiscardScore", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the spam assassin discard score for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		if(discard_score==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set sa_discard_score=null where pkey=?",
				pkey
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set sa_discard_score=? where pkey=?",
				discard_score,
				pkey
			);
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	public static void setLinuxServerAccountTrashEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountTrashEmailRetention", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the trash email retention for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set trash_email_retention=null where pkey=?",
				pkey
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set trash_email_retention=? where pkey=?",
				days,
				pkey
			);
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	public static void setLinuxServerAccountUseInbox(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean useInbox
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountUseInbox", pkey);
		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set the use_inbox flag for LinuxAccount named '"+LinuxAccount.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set use_inbox=? where pkey=?",
			useInbox,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, pkey),
			getAOServerForLinuxServerAccount(conn, pkey),
			false
		);
	}

	/**
	 * Waits for any pending or processing account rebuild to complete.
	 */
	public static void waitForLinuxAccountRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForLinuxAccountRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForLinuxAccountRebuild();
	}

	static boolean canLinuxGroupAccessServer(DatabaseConnection conn, RequestSource source, GroupId groupName, int aoServer) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      lg.name\n"
			+ "    from\n"
			+ "      linux.\"Group\" lg,\n"
			+ "      billing.\"Package\" pk,\n"
			+ "      business_servers bs\n"
			+ "    where\n"
			+ "      lg.name=?\n"
			+ "      and lg.package=pk.name\n"
			+ "      and pk.accounting=bs.accounting\n"
			+ "      and bs.server=?\n"
			+ "    limit 1\n"
			+ "  )\n"
			+ "  is not null\n",
			groupName,
			aoServer
		);
	}

	static void checkLinuxGroupAccessServer(DatabaseConnection conn, RequestSource source, String action, GroupId groupName, int server) throws IOException, SQLException {
		if(!canLinuxGroupAccessServer(conn, source, groupName, server)) {
			String message=
				"groupName="
				+groupName
				+" is not allowed to access server.pkey="
				+server
				+": action='"
				+action
				+"'"
			;
			throw new SQLException(message);
		}
	}

	public static AccountingCode getBusinessForLinuxGroup(DatabaseConnection conn, GroupId name) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select pk.accounting from linux.\"Group\" lg, billing.\"Package\" pk where lg.package=pk.name and lg.name=?",
			name
		);
	}

	public static List<AccountingCode> getBusinessesForLinuxGroupAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectCollectionQuery(
			new ArrayList<AccountingCode>(),
			ObjectFactories.accountingCodeFactory,
		   "select\n"
			+ "  pk1.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga1\n"
			+ "  inner join linux.\"Group\" lg1 on lga1.\"group\"=lg1.name\n"
			+ "  inner join billing.\"Package\" pk1 on lg1.package=pk1.name\n"
			+ "where\n"
			+ "  lga1.pkey=?\n"
			+ "union select\n"
			+ "  pk2.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga2\n"
			+ "  inner join account.\"Username\" un2 on lga2.username=un2.username\n"
			+ "  inner join billing.\"Package\" pk2 on un2.package=pk2.name\n"
			+ "where\n"
			+ "  lga2.pkey=?",
			pkey,
			pkey
		);
	}

	public static AccountingCode getBusinessForLinuxServerAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  lsa.pkey=?\n"
			+ "  and lsa.username=un.username\n"
			+ "  and un.package=pk.name",
			pkey
		);
	}

	public static AccountingCode getBusinessForLinuxServerGroup(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  lsg.pkey=?\n"
			+ "  and lsg.name=lg.name\n"
			+ "  and lg.package=pk.name",
			pkey
		);
	}

	public static GroupId getGroupNameForLinuxServerGroup(DatabaseConnection conn, int lsgPKey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.groupIdFactory,
			"select name from linux.\"GroupServer\" where pkey=?",
			lsgPKey
		);
	}

	public static int getAOServerForLinuxServerAccount(DatabaseConnection conn, int account) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from linux.\"UserServer\" where pkey=?", account);
	}

	public static int getAOServerForLinuxServerGroup(DatabaseConnection conn, int group) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from linux.\"GroupServer\" where pkey=?", group);
	}

	public static IntList getAOServersForLinuxAccount(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeIntListQuery("select ao_server from linux.\"UserServer\" where username=?", username);
	}

	public static IntList getAOServersForLinuxGroup(DatabaseConnection conn, GroupId name) throws IOException, SQLException {
		return conn.executeIntListQuery("select ao_server from linux.\"GroupServer\" where name=?", name);
	}

	public static IntList getAOServersForLinuxGroupAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  lsg.ao_server\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga\n"
			+ "  inner join linux.\"GroupServer\" lsg on lga.\"group\"=lsg.name\n"
			+ "  inner join linux.\"UserServer\" lsa on lga.username=lsa.username\n"
			+ "  inner join servers se on lsg.ao_server=se.pkey\n"
			+ "where\n"
			+ "  lga.pkey=?\n"
			+ "  and lsg.ao_server=lsa.ao_server\n"
			+ "  and (\n"
			+ "    lga.operating_system_version is null\n"
			+ "    or lga.operating_system_version=se.operating_system_version\n"
			+ "  )",
			pkey
		);
	}

	public static String getTypeForLinuxAccount(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeStringQuery("select type from linux.\"User\" where username=?", username);
	}

	public static String getTypeForLinuxServerAccount(DatabaseConnection conn, int account) throws IOException, SQLException {
		return conn.executeStringQuery(
			"select\n"
			+ "  la.type\n"
			+ "from\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  linux.\"User\" la\n"
			+ "where\n"
			+ "  lsa.pkey=?\n"
			+ "  and lsa.username=la.username",
			account
		);
   }

	public static String getTypeForLinuxServerGroup(DatabaseConnection conn, int lsg) throws IOException, SQLException {
		return conn.executeStringQuery(
			"select\n"
			+ "  lg.type\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  lsg.pkey=?\n"
			+ "  and lsg.name=lg.name",
			lsg
		);
	}

	public static int getLinuxServerAccount(DatabaseConnection conn, UserId username, int aoServer) throws IOException, SQLException {
		int pkey=conn.executeIntQuery(
			"select coalesce(\n"
			+ "  (\n"
			+ "    select\n"
			+ "      pkey\n"
			+ "    from\n"
			+ "      linux.\"UserServer\"\n"
			+ "    where\n"
			+ "      username=?\n"
			+ "      and ao_server=?\n"
			+ "  ), -1\n"
			+ ")",
			username,
			aoServer
		);
		if(pkey==-1) throw new SQLException("Unable to find LinuxServerAccount for "+username+" on "+aoServer);
		return pkey;
	}

	public static IntList getLinuxServerAccountsForLinuxAccount(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from linux.\"UserServer\" where username=?", username);
	}

	public static IntList getLinuxServerGroupsForLinuxGroup(DatabaseConnection conn, GroupId name) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from linux.\"GroupServer\" where name=?", name);
	}

	public static AccountingCode getPackageForLinuxGroup(DatabaseConnection conn, GroupId name) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  package\n"
			+ "from\n"
			+ "  linux.\"Group\"\n"
			+ "where\n"
			+ "  name=?",
			name
		);
	}

	public static AccountingCode getPackageForLinuxServerGroup(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  lg.package\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  lsg.pkey=?\n"
			+ "  and lsg.name=lg.name",
			pkey
		);
	}

	public static int getGIDForLinuxServerGroup(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select gid from linux.\"GroupServer\" where pkey=?", pkey);
	}

	public static int getUIDForLinuxServerAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select uid from linux.\"UserServer\" where pkey=?", pkey);
	}

	public static UserId getLinuxAccountForLinuxGroupAccount(DatabaseConnection conn, int lga) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.userIdFactory,
			"select username from linux.\"GroupUser\" where pkey=?",
			lga
		);
	}

	public static GroupId getLinuxGroupForLinuxGroupAccount(DatabaseConnection conn, int lga) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.groupIdFactory,
			"select \"group\" from linux.\"GroupUser\" where pkey=?",
			lga
		);
	}

	public static UserId getUsernameForLinuxServerAccount(DatabaseConnection conn, int account) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.userIdFactory,
			"select username from linux.\"UserServer\" where pkey=?",
			account
		);
	}

	public static boolean comparePassword(
		DatabaseConnection conn,
		RequestSource source, 
		int pkey, 
		String password
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "comparePassword", pkey);
		if(isLinuxServerAccountDisabled(conn, pkey)) throw new SQLException("Unable to compare password, LinuxServerAccount disabled: "+pkey);

		UserId username=getUsernameForLinuxServerAccount(conn, pkey);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to compare password for LinuxServerAccount named '"+LinuxAccount.MAIL+"': "+pkey);
		String type=conn.executeStringQuery("select type from linux.\"User\" where username=?", username);

		// Make sure passwords can be set before doing a comparison
		if(!LinuxAccountType.canSetPassword(type)) throw new SQLException("Passwords may not be compared for LinuxAccountType="+type);

		// Perform the password comparison
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForLinuxServerAccount(conn, pkey)
		).compareLinuxAccountPassword(username, password);
	}

	public static void setPrimaryLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessLinuxGroupAccount(conn, source, "setPrimaryLinuxGroupAccount", pkey);
		UserId username = conn.executeObjectQuery(
			ObjectFactories.userIdFactory,
			"select username from linux.\"GroupUser\" where pkey=?",
			pkey
		);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set primary LinuxGroupUser, User disabled: "+username);
		GroupId group = conn.executeObjectQuery(
			ObjectFactories.groupIdFactory,
			"select \"group\" from linux.\"GroupUser\" where pkey=?",
			pkey
		);

		conn.executeUpdate(
			"update linux.\"GroupUser\" set is_primary=true where pkey=?",
			pkey
		);
		conn.executeUpdate(
			"update linux.\"GroupUser\" set is_primary=false where is_primary and pkey!=? and username=?",
			pkey,
			username
		);
		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getCollection(UsernameHandler.getBusinessForUsername(conn, username), getBusinessForLinuxGroup(conn, group)),
			getAOServersForLinuxGroupAccount(conn, pkey),
			false
		);
	}
}
