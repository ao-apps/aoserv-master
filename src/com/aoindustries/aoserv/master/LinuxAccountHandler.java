/*
 * Copyright 2001-2013, 2015, 2016, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.distribution.OperatingSystemVersion;
import com.aoindustries.aoserv.client.email.SpamAssassinMode;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.GroupUser;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.linux.Shell;
import com.aoindustries.aoserv.client.linux.User;
import com.aoindustries.aoserv.client.linux.User.Gecos;
import com.aoindustries.aoserv.client.linux.UserServer;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.password.PasswordChecker;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.Site;
import com.aoindustries.aoserv.client.web.tomcat.SharedTomcat;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
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

	private final static Map<com.aoindustries.aoserv.client.linux.User.Name,Boolean> disabledLinuxAccounts=new HashMap<>();
	private final static Map<Integer,Boolean> disabledLinuxServerAccounts=new HashMap<>();

	public static void checkAccessLinuxAccount(DatabaseConnection conn, RequestSource source, String action, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
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

	public static void checkAccessLinuxGroup(DatabaseConnection conn, RequestSource source, String action, Group.Name name) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
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

	public static void checkAccessLinuxGroupAccount(DatabaseConnection conn, RequestSource source, String action, int id) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, action, getLinuxAccountForLinuxGroupAccount(conn, id));
		checkAccessLinuxGroup(conn, source, action, getLinuxGroupForLinuxGroupAccount(conn, id));
	}

	public static boolean canAccessLinuxServerAccount(DatabaseConnection conn, RequestSource source, int account) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getUsername()).length!=0) {
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
				+", id="
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
				+", id="
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
		com.aoindustries.aoserv.client.linux.User.Name username,
		Group.Name primary_group,
		Gecos name,
		Gecos office_location,
		Gecos office_phone,
		Gecos home_phone,
		String type,
		PosixPath shell,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to add User named '"+User.MAIL+'\'');

		// Make sure the shell is allowed for the type of account being added
		if(!UserType.isAllowedShell(type, shell)) throw new SQLException("shell='"+shell+"' not allowed for type='"+type+'\'');

		if(!skipSecurityChecks) {
			UsernameHandler.checkAccessUsername(conn, source, "addLinuxAccount", username);
			if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to add User, Username disabled: "+username);
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
			Table.TableID.LINUX_ACCOUNTS,
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
		Group.Name groupName,
		Account.Name packageName,
		String type,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(!skipSecurityChecks) {
			PackageHandler.checkAccessPackage(conn, source, "addLinuxGroup", packageName);
			if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Group, Package disabled: "+packageName);
		}
		if (
			groupName.equals(Group.FTPONLY)
			|| groupName.equals(Group.MAIL)
			|| groupName.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to add Group: "+groupName);

		conn.executeUpdate("insert into linux.\"Group\" values(?,?,?)", groupName, packageName, type);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_GROUPS,
			PackageHandler.getBusinessForPackage(conn, packageName),
			InvalidateList.allServers,
			false
		);
	}

	public static int addLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name groupName,
		com.aoindustries.aoserv.client.linux.User.Name username,
		boolean isPrimary,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(groupName.equals(Group.MAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+Group.MAIL+'\'');
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for user '"+User.MAIL+'\'');
		if(!skipSecurityChecks) {
			if(
				!groupName.equals(Group.FTPONLY)
				&& !groupName.equals(Group.MAILONLY)
			) checkAccessLinuxGroup(conn, source, "addLinuxGroupAccount", groupName);
			checkAccessLinuxAccount(conn, source, "addLinuxGroupAccount", username);
			if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add LinuxGroupUser, User disabled: "+username);
		}
		if(groupName.equals(Group.FTPONLY)) {
			// Only allowed to have ftponly group when it is a ftponly account
			String type=getTypeForLinuxAccount(conn, username);
			if(!type.equals(UserType.FTPONLY)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+Group.FTPONLY+"' on non-ftp-only-type User named "+username);
		}
		if(groupName.equals(Group.MAILONLY)) {
			// Only allowed to have mail group when it is a "mailonly" account
			String type=getTypeForLinuxAccount(conn, username);
			if(!type.equals(UserType.EMAIL)) throw new SQLException("Not allowed to add LinuxGroupUser for group '"+Group.MAILONLY+"' on non-email-type User named "+username);
		}

		// Do not allow more than 31 groups per account
		int count=conn.executeIntQuery("select count(*) from linux.\"GroupUser\" where \"user\"=?", username);
		if(count >= GroupUser.MAX_GROUPS) throw new SQLException("Only "+GroupUser.MAX_GROUPS+" groups are allowed per user, username="+username+" already has access to "+count+" groups");

		int id = conn.executeIntUpdate(
			"INSERT INTO linux.\"GroupUser\" VALUES (default,?,?,?,null) RETURNING id",
			groupName,
			username,
			isPrimary
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getCollection(
				UsernameHandler.getBusinessForUsername(conn, username),
				getBusinessForLinuxGroup(conn, groupName)
			),
			getAOServersForLinuxGroupAccount(conn, id),
			false
		);
		return id;
	}

	public static int addLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		int aoServer,
		PosixPath home,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(username.equals(User.MAIL)) {
			throw new SQLException("Not allowed to add UserServer for user '"+User.MAIL+'\'');
		}
		if(!skipSecurityChecks) {
			checkAccessLinuxAccount(conn, source, "addLinuxServerAccount", username);
			if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add UserServer, User disabled: "+username);
			ServerHandler.checkAccessServer(conn, source, "addLinuxServerAccount", aoServer);
			UsernameHandler.checkUsernameAccessServer(conn, source, "addLinuxServerAccount", username, aoServer);
		}

		// OperatingSystem settings
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		if(osv == -1) throw new SQLException("Operating system version not known for server #" + aoServer);
		PosixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
		PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		if(home.equals(UserServer.getDefaultHomeDirectory(username))) {
			// Make sure no conflicting /home/u/username account exists.
			String prefix = home + "/";
			List<String> conflicting = conn.executeStringListQuery(
				"select distinct home from linux.\"UserServer\" where ao_server=? and substring(home from 1 for " + prefix.length() + ")=? order by home",
				aoServer,
				prefix
			);
			if(!conflicting.isEmpty()) throw new SQLException("Found conflicting home directories: " + conflicting);
		} else if(home.equals(UserServer.getHashedHomeDirectory(username))) {
			// Make sure no conflicting /home/u account exists.
			String conflictHome = "/home/" + username.toString().charAt(0);
			if(
				conn.executeBooleanQuery(
					"select (select id from linux.\"UserServer\" where ao_server=? and home=? limit 1) is not null",
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
					if(!Site.isValidSiteName(siteName)) {
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
					if(!SharedTomcat.isValidSharedTomcatName(tomcatName)) {
						throw new SQLException("Invalid shared tomcat name for wwwgroup home directory: " + home);
					}
				}
			}
		}

		// The primary group for this user must exist on this server
		Group.Name primaryGroup=getPrimaryLinuxGroup(conn, username, osv);
		int primaryLSG=getLinuxServerGroup(conn, primaryGroup, aoServer);
		if(primaryLSG<0) throw new SQLException("Unable to find primary Linux group '"+primaryGroup+"' on Server #"+aoServer+" for Linux account '"+username+"'");

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, aoServer);
		int id = conn.executeIntUpdate(
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
			+ "  " + (username.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_TRASH_EMAIL_RETENTION)) + ",\n"
			+ "  " + (username.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_JUNK_EMAIL_RETENTION)) + ",\n"
			+ "  ?,\n"
			+ "  " + UserServer.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE + ",\n"
			+ "  " + (username.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_SPAM_ASSASSIN_DISCARD_SCORE)) + ",\n"
			+ "  null\n" // sudo
			+ ") RETURNING id",
			username,
			aoServer,
			aoServer,
			home,
			SpamAssassinMode.DEFAULT_SPAMASSASSIN_INTEGRATION_MODE
		);
		// Notify all clients of the update
		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			accounting,
			aoServer,
			true
		);
		// If it is a email type, add the default attachment blocks
		if(!username.equals(User.EMAILMON) && isLinuxAccountEmailType(conn, username)) {
			conn.executeUpdate(
				"insert into email.\"AttachmentBlock\" (\n"
				+ "  linux_server_account,\n"
				+ "  extension\n"
				+ ") select\n"
				+ "  ?,\n"
				+ "  extension\n"
				+ "from\n"
				+ "  email.\"AttachmentType\"\n"
				+ "where\n"
				+ "  is_default_block",
				id
			);
			invalidateList.addTable(
				conn,
				Table.TableID.EMAIL_ATTACHMENT_BLOCKS,
				accounting,
				aoServer,
				false
			);
		}
		return id;
	}

	public static int addLinuxServerGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name groupName,
		int aoServer,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(
			groupName.equals(Group.FTPONLY)
			|| groupName.equals(Group.MAIL)
			|| groupName.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to add GroupServer for group '"+groupName+'\'');
		Account.Name accounting = getBusinessForLinuxGroup(conn, groupName);
		if(!skipSecurityChecks) {
			checkAccessLinuxGroup(conn, source, "addLinuxServerGroup", groupName);
			ServerHandler.checkAccessServer(conn, source, "addLinuxServerGroup", aoServer);
			checkLinuxGroupAccessServer(conn, source, "addLinuxServerGroup", groupName, aoServer);
			BusinessHandler.checkBusinessAccessServer(conn, source, "addLinuxServerGroup", accounting, aoServer);
		}

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, aoServer);
		int id = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  linux.\"GroupServer\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  linux.get_next_gid(?),\n"
			+ "  now()\n"
			+ ") RETURNING id",
			groupName,
			aoServer,
			aoServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_GROUPS,
			accounting,
			aoServer,
			true
		);
		return id;
	}

	/**
	 * Gets the group name that exists on a server for the given gid
	 * or {@code null} if the gid is not allocated to the server.
	 */
	public static Group.Name getGroupNameByGid(
		DatabaseConnection conn,
		int aoServer,
		int gid
	) throws SQLException {
		return conn.executeObjectQuery(Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			ObjectFactories.groupNameFactory,
			"select name from linux.\"GroupServer\" where ao_server=? and gid=?",
			aoServer,
			gid
		);
	}

	/**
	 * Gets the username that exists on a server for the given uid
	 * or {@code null} if the uid is not allocated to the server.
	 */
	public static com.aoindustries.aoserv.client.linux.User.Name getUsernameByUid(
		DatabaseConnection conn,
		int aoServer,
		int uid
	) throws SQLException {
		return conn.executeObjectQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			ObjectFactories.linuxUserNameFactory,
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
		Group.Name groupName,
		int gid
	) throws IOException, SQLException {
		// This must be a master user with access to the server
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu == null) throw new SQLException("Not a master user: " + source.getUsername());
		ServerHandler.checkAccessServer(conn, source, "addSystemGroup", aoServer);
		// The group ID must be in the system group range
		if(gid < 0) throw new SQLException("Invalid gid: " + gid);
		int gidMin = AOServerHandler.getGidMin(conn, aoServer);
		int gidMax = AOServerHandler.getGidMax(conn, aoServer);
		// The group ID must not already exist on this server
		{
			Group.Name existing = getGroupNameByGid(conn, aoServer, gid);
			if(existing != null) throw new SQLException("Group #" + gid + " already exists on server #" + aoServer + ": " + existing);
		}
		// Must be one of the expected patterns for the servers operating system version
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		if(
			osv == OperatingSystemVersion.CENTOS_7_X86_64
			&& (
				// Fixed group ids
				   (groupName.equals(Group.ROOT)            && gid == 0)
				|| (groupName.equals(Group.BIN)             && gid == 1)
				|| (groupName.equals(Group.DAEMON)          && gid == 2)
				|| (groupName.equals(Group.SYS)             && gid == 3)
				|| (groupName.equals(Group.ADM)             && gid == 4)
				|| (groupName.equals(Group.TTY)             && gid == 5)
				|| (groupName.equals(Group.DISK)            && gid == 6)
				|| (groupName.equals(Group.LP)              && gid == 7)
				|| (groupName.equals(Group.MEM)             && gid == 8)
				|| (groupName.equals(Group.KMEM)            && gid == 9)
				|| (groupName.equals(Group.WHEEL)           && gid == 10)
				|| (groupName.equals(Group.CDROM)           && gid == 11)
				|| (groupName.equals(Group.MAIL)            && gid == 12)
				|| (groupName.equals(Group.MAN)             && gid == 15)
				|| (groupName.equals(Group.DIALOUT)         && gid == 18)
				|| (groupName.equals(Group.FLOPPY)          && gid == 19)
				|| (groupName.equals(Group.GAMES)           && gid == 20)
				|| (groupName.equals(Group.UTMP)            && gid == 22)
				|| (groupName.equals(Group.NAMED)           && gid == 25)
				|| (groupName.equals(Group.POSTGRES)        && gid == 26)
				|| (groupName.equals(Group.RPCUSER)         && gid == 29)
				|| (groupName.equals(Group.MYSQL)           && gid == 31)
				|| (groupName.equals(Group.RPC)             && gid == 32)
				|| (groupName.equals(Group.TAPE)            && gid == 33)
				|| (groupName.equals(Group.UTEMPTER)        && gid == 35)
				|| (groupName.equals(Group.VIDEO)           && gid == 39)
				|| (groupName.equals(Group.DIP)             && gid == 40)
				|| (groupName.equals(Group.MAILNULL)        && gid == 47)
				|| (groupName.equals(Group.APACHE)          && gid == 48)
				|| (groupName.equals(Group.FTP)             && gid == 50)
				|| (groupName.equals(Group.SMMSP)           && gid == 51)
				|| (groupName.equals(Group.LOCK)            && gid == 54)
				|| (groupName.equals(Group.TSS)             && gid == 59)
				|| (groupName.equals(Group.AUDIO)           && gid == 63)
				|| (groupName.equals(Group.TCPDUMP)         && gid == 72)
				|| (groupName.equals(Group.SSHD)            && gid == 74)
				|| (groupName.equals(Group.SASLAUTH)        && gid == 76)
				|| (groupName.equals(Group.AWSTATS)         && gid == 78)
				|| (groupName.equals(Group.DBUS)            && gid == 81)
				|| (groupName.equals(Group.MAILONLY)        && gid == 83)
				|| (groupName.equals(Group.SCREEN)          && gid == 84)
				|| (groupName.equals(Group.BIRD)            && gid == 95)
				|| (groupName.equals(Group.NOBODY)          && gid == 99)
				|| (groupName.equals(Group.USERS)           && gid == 100)
				|| (groupName.equals(Group.AVAHI_AUTOIPD)   && gid == 170)
				|| (groupName.equals(Group.DHCPD)           && gid == 177)
				|| (groupName.equals(Group.SYSTEMD_JOURNAL) && gid == 190)
				|| (groupName.equals(Group.SYSTEMD_NETWORK) && gid == 192)
				|| (groupName.equals(Group.NFSNOBODY)       && gid == 65534)
				|| (
					// System groups in range 201 through gidMin - 1
					gid >= CENTOS_7_SYS_GID_MIN
					&& gid < gidMin
					&& (
						   groupName.equals(Group.AOSERV_JILTER)
						|| groupName.equals(Group.AOSERV_XEN_MIGRATION)
						|| groupName.equals(Group.CGRED)
						|| groupName.equals(Group.CHRONY)
						|| groupName.equals(Group.CLAMSCAN)
						|| groupName.equals(Group.CLAMUPDATE)
						|| groupName.equals(Group.INPUT)
						|| groupName.equals(Group.MEMCACHED)
						|| groupName.equals(Group.NGINX)
						|| groupName.equals(Group.POLKITD)
						|| groupName.equals(Group.SSH_KEYS)
						|| groupName.equals(Group.SYSTEMD_BUS_PROXY)
						|| groupName.equals(Group.SYSTEMD_NETWORK)
						|| groupName.equals(Group.UNBOUND)
						|| groupName.equals(Group.VIRUSGROUP)
					)
				) || (
					// Regular user groups in range gidMin through Group.GID_MAX
					gid >= gidMin
					&& gid <= gidMax
					&& groupName.equals(Group.AOADMIN)
				)
			)
		) {
			int id = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  linux.\"GroupServer\"\n"
				+ "VALUES (\n"
				+ "  default,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  now()\n"
				+ ") RETURNING id",
				groupName,
				aoServer,
				gid
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				Table.TableID.LINUX_SERVER_GROUPS,
				ServerHandler.getBusinessesForServer(conn, aoServer),
				aoServer,
				true
			);
			return id;
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
		private static final Map<com.aoindustries.aoserv.client.linux.User.Name,SystemUser> centos7SystemUsers = new HashMap<>();
		private static void addCentos7SystemUser(
			com.aoindustries.aoserv.client.linux.User.Name username,
			int uid,
			Group.Name groupName,
			String fullName,
			String home,
			PosixPath shell,
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
						PosixPath.valueOf(home).intern(),
						shell,
						sudo
					)
				) != null
			) throw new AssertionError("Duplicate username: " + username);
		}
		static {
			try {
				try {
					addCentos7SystemUser(User.ROOT,                           0, Group.ROOT,              "root",                        "/root",            Shell.BASH, null);
					addCentos7SystemUser(User.BIN,                            1, Group.BIN,               "bin",                         "/bin",             Shell.NOLOGIN, null);
					addCentos7SystemUser(User.DAEMON,                         2, Group.DAEMON,            "daemon",                      "/sbin",            Shell.NOLOGIN, null);
					addCentos7SystemUser(User.ADM,                            3, Group.ADM,               "adm",                         "/var/adm",         Shell.NOLOGIN, null);
					addCentos7SystemUser(User.LP,                             4, Group.LP,                "lp",                          "/var/spool/lpd",   Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SYNC,                           5, Group.ROOT,              "sync",                        "/sbin",            Shell.SYNC, null);
					addCentos7SystemUser(User.SHUTDOWN,                       6, Group.ROOT,              "shutdown",                    "/sbin",            Shell.SHUTDOWN, null);
					addCentos7SystemUser(User.HALT,                           7, Group.ROOT,              "halt",                        "/sbin",            Shell.HALT, null);
					addCentos7SystemUser(User.MAIL,                           8, Group.MAIL,              "mail",                        "/var/spool/mail",  Shell.NOLOGIN, null);
					addCentos7SystemUser(User.OPERATOR,                      11, Group.ROOT,              "operator",                    "/root",            Shell.NOLOGIN, null);
					addCentos7SystemUser(User.GAMES,                         12, Group.USERS,             "games",                       "/usr/games",       Shell.NOLOGIN, null);
					addCentos7SystemUser(User.FTP,                           14, Group.FTP,               "FTP User",                    "/var/ftp",         Shell.NOLOGIN, null);
					addCentos7SystemUser(User.NAMED,                         25, Group.NAMED,             "Named",                       "/var/named",       Shell.NOLOGIN, null);
					addCentos7SystemUser(User.POSTGRES,                      26, Group.POSTGRES,          "PostgreSQL Server",           "/var/lib/pgsql",   Shell.BASH, null);
					addCentos7SystemUser(User.RPCUSER,                       29, Group.RPCUSER,           "RPC Service User",            "/var/lib/nfs",     Shell.NOLOGIN, null);
					addCentos7SystemUser(User.MYSQL,                         31, Group.MYSQL,             "MySQL server",                "/var/lib/mysql",   Shell.BASH, null);
					addCentos7SystemUser(User.RPC,                           32, Group.RPC,               "Rpcbind Daemon",              "/var/lib/rpcbind", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.MAILNULL,                      47, Group.MAILNULL,          null,                          "/var/spool/mqueue", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.APACHE,                        48, Group.APACHE,            "Apache",                      "/usr/share/httpd", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SMMSP,                         51, Group.SMMSP,             null,                          "/var/spool/mqueue", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.TSS,                           59, Group.TSS,               "Account used by the trousers package to sandbox the tcsd daemon", "/dev/null", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.TCPDUMP,                       72, Group.TCPDUMP,           null,                          "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SSHD,                          74, Group.SSHD,              "Privilege-separated SSH",     "/var/empty/sshd",  Shell.NOLOGIN, null);
					addCentos7SystemUser(User.CYRUS,                         76, Group.MAIL,              "Cyrus IMAP Server",           "/var/lib/imap",    Shell.NOLOGIN, null);
					addCentos7SystemUser(User.AWSTATS,                       78, Group.AWSTATS,           "AWStats Background Log Processing", "/var/opt/awstats", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.DBUS,                          81, Group.DBUS,              "System message bus",          "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.BIRD,                          95, Group.BIRD,              "BIRD Internet Routing Daemon", "/var/opt/bird",   Shell.NOLOGIN, null);
					addCentos7SystemUser(User.NOBODY,                        99, Group.NOBODY,            "Nobody",                      "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.AVAHI_AUTOIPD,                170, Group.AVAHI_AUTOIPD,     "Avahi IPv4LL Stack",          "/var/lib/avahi-autoipd", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.DHCPD,                        177, Group.DHCPD,             "DHCP server",                 "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SYSTEMD_NETWORK,              192, Group.SYSTEMD_NETWORK,   "systemd Network Management",  "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.NFSNOBODY,                  65534, Group.NFSNOBODY,         "Anonymous NFS User",          "/var/lib/nfs",     Shell.NOLOGIN, null);
					addCentos7SystemUser(User.AOSERV_JILTER,     ANY_SYSTEM_UID, Group.AOSERV_JILTER,     "AOServ Jilter",               "/var/opt/aoserv-jilter", Shell.NOLOGIN, null);
					addCentos7SystemUser(User.CHRONY,            ANY_SYSTEM_UID, Group.CHRONY,            null,                          "/var/lib/chrony",  Shell.NOLOGIN, null);
					addCentos7SystemUser(User.CLAMSCAN,          ANY_SYSTEM_UID, Group.CLAMSCAN,          "Clamav scanner user",         "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.CLAMUPDATE,        ANY_SYSTEM_UID, Group.CLAMUPDATE,        "Clamav database update user", "/var/lib/clamav",  Shell.NOLOGIN, null);
					addCentos7SystemUser(User.MEMCACHED,         ANY_SYSTEM_UID, Group.MEMCACHED,         "Memcached daemon",            "/run/memcached",   Shell.NOLOGIN, null);
					addCentos7SystemUser(User.NGINX,             ANY_SYSTEM_UID, Group.NGINX,             "Nginx web server",            "/var/lib/nginx",   Shell.NOLOGIN, null);
					addCentos7SystemUser(User.POLKITD,           ANY_SYSTEM_UID, Group.POLKITD,           "User for polkitd",            "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SASLAUTH,          ANY_SYSTEM_UID, Group.SASLAUTH,          "Saslauthd user",              "/run/saslauthd",   Shell.NOLOGIN, null);
					addCentos7SystemUser(User.SYSTEMD_BUS_PROXY, ANY_SYSTEM_UID, Group.SYSTEMD_BUS_PROXY, "systemd Bus Proxy",           "/",                Shell.NOLOGIN, null);
					addCentos7SystemUser(User.UNBOUND,           ANY_SYSTEM_UID, Group.UNBOUND,           "Unbound DNS resolver",        "/etc/unbound",     Shell.NOLOGIN, null);
					addCentos7SystemUser(User.AOADMIN,           ANY_USER_UID,   Group.AOADMIN,           "AO Industries Administrator", "/home/aoadmin",    Shell.BASH, AOADMIN_SUDO);
					addCentos7SystemUser(User.AOSERV_XEN_MIGRATION, ANY_SYSTEM_UID, Group.AOSERV_XEN_MIGRATION, "AOServ Xen Migration",  "/var/opt/aoserv-xen-migration", Shell.BASH, AOSERV_XEN_MIGRATION_SUDO);
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

		final com.aoindustries.aoserv.client.linux.User.Name username;
		final int uid;
		final Group.Name groupName;
		final Gecos fullName;
		final Gecos officeLocation;
		final Gecos officePhone;
		final Gecos homePhone;
		final PosixPath home;
		final PosixPath shell;
		final String sudo;

		SystemUser(
			com.aoindustries.aoserv.client.linux.User.Name username,
			int uid,
			Group.Name groupName,
			Gecos fullName,
			Gecos officeLocation,
			Gecos officePhone,
			Gecos homePhone,
			PosixPath home,
			PosixPath shell,
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
		com.aoindustries.aoserv.client.linux.User.Name username,
		int uid,
		int gid,
		Gecos fullName,
		Gecos officeLocation,
		Gecos officePhone,
		Gecos homePhone,
		PosixPath home,
		PosixPath shell
	) throws IOException, SQLException {
		// This must be a master user with access to the server
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getUsername());
		if(mu == null) throw new SQLException("Not a master user: " + source.getUsername());
		ServerHandler.checkAccessServer(conn, source, "addSystemUser", aoServer);
		// The user ID must be in the system user range
		if(uid < 0) throw new SQLException("Invalid uid: " + uid);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);
		int uidMax = AOServerHandler.getUidMax(conn, aoServer);
		// The user ID must not already exist on this server
		{
			com.aoindustries.aoserv.client.linux.User.Name existing = getUsernameByUid(conn, aoServer, uid);
			if(existing != null) throw new SQLException("User #" + uid + " already exists on server #" + aoServer + ": " + existing);
		}
		// Get the group name for the requested gid
		Group.Name groupName = getGroupNameByGid(conn, aoServer, gid);
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
				// Regular users in range uidMin through User.UID_MAX
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
			int id = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  linux.\"UserServer\"\n"
				+ "VALUES (\n"
				+ "  default,\n" // id
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
				+ "  "+UserServer.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE+",\n"
				+ "  null,\n" // sa_discard_score
				+ "  ?\n" // sudo
				+ ") RETURNING id",
				username,
				aoServer,
				uid,
				home,
				SpamAssassinMode.NONE,
				systemUser.sudo
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				Table.TableID.LINUX_SERVER_ACCOUNTS,
				ServerHandler.getBusinessesForServer(conn, aoServer),
				aoServer,
				true
			);
			return id;
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
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, from_lsa);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to copy User named '"+User.MAIL+'\'');
		int from_server=getAOServerForLinuxServerAccount(conn, from_lsa);
		int to_lsa=conn.executeIntQuery(
			"select id from linux.\"UserServer\" where username=? and ao_server=?",
			username,
			to_server
		);
		checkAccessLinuxServerAccount(conn, source, "copyHomeDirectory", to_lsa);
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(UserType.USER)
			&& !type.equals(UserType.EMAIL)
			&& !type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy LinuxAccounts of type '"+type+"', username="+username);

		AOServDaemonConnector fromDaemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
		AOServDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
		conn.releaseConnection();
		long byteCount = fromDaemonConnector.copyHomeDirectory(username, toDaemonConnector);
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
		if(isLinuxServerAccountDisabled(conn, from_lsa)) throw new SQLException("Unable to copy UserServer password, from account disabled: "+from_lsa);
		com.aoindustries.aoserv.client.linux.User.Name from_username=getUsernameForLinuxServerAccount(conn, from_lsa);
		if(from_username.equals(User.MAIL)) throw new SQLException("Not allowed to copy the password from User named '"+User.MAIL+'\'');
		checkAccessLinuxServerAccount(conn, source, "copyLinuxServerAccountPassword", to_lsa);
		if(isLinuxServerAccountDisabled(conn, to_lsa)) throw new SQLException("Unable to copy UserServer password, to account disabled: "+to_lsa);
		com.aoindustries.aoserv.client.linux.User.Name to_username=getUsernameForLinuxServerAccount(conn, to_lsa);
		if(to_username.equals(User.MAIL)) throw new SQLException("Not allowed to copy the password to User named '"+User.MAIL+'\'');

		int from_server=getAOServerForLinuxServerAccount(conn, from_lsa);
		int to_server=getAOServerForLinuxServerAccount(conn, to_lsa);

		String from_type=getTypeForLinuxAccount(conn, from_username);
		if(
			!from_type.equals(UserType.APPLICATION)
			&& !from_type.equals(UserType.USER)
			&& !from_type.equals(UserType.EMAIL)
			&& !from_type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords from LinuxAccounts of type '"+from_type+"', username="+from_username);

		String to_type=getTypeForLinuxAccount(conn, to_username);
		if(
			!to_type.equals(UserType.APPLICATION)
			&& !to_type.equals(UserType.USER)
			&& !to_type.equals(UserType.EMAIL)
			&& !to_type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords to LinuxAccounts of type '"+to_type+"', username="+to_username);

		AOServDaemonConnector fromDemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
		AOServDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
		conn.releaseConnection();
		Tuple2<String,Integer> enc_password = fromDemonConnector.getEncryptedLinuxAccountPassword(from_username);
		toDaemonConnector.setEncryptedLinuxAccountPassword(to_username, enc_password.getElement1(), enc_password.getElement2());

		//Account.Name from_accounting=UsernameHandler.getBusinessForUsername(conn, from_username);
		//Account.Name to_accounting=UsernameHandler.getBusinessForUsername(conn, to_username);
	}

	public static void disableLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		com.aoindustries.aoserv.client.linux.User.Name username
	) throws IOException, SQLException {
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("linux.User is already disabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableLinuxAccount", disableLog, false);
		checkAccessLinuxAccount(conn, source, "disableLinuxAccount", username);
		IntList lsas=getLinuxServerAccountsForLinuxAccount(conn, username);
		for(int c=0;c<lsas.size();c++) {
			int lsa=lsas.getInt(c);
			if(!isLinuxServerAccountDisabled(conn, lsa)) {
				throw new SQLException("Cannot disable User '"+username+"': UserServer not disabled: "+lsa);
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
			Table.TableID.LINUX_ACCOUNTS,
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
		int id
	) throws IOException, SQLException {
		if(isLinuxServerAccountDisabled(conn, id)) throw new SQLException("linux.UserServer is already disabled: "+id);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableLinuxServerAccount", disableLog, false);
		checkAccessLinuxServerAccount(conn, source, "disableLinuxServerAccount", id);

		int aoServer = getAOServerForLinuxServerAccount(conn, id);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);

		// The UID must be a user UID
		int uid=getUIDForLinuxServerAccount(conn, id);
		if(uid < uidMin) throw new SQLException("Not allowed to disable a system UserServer: id="+id+", uid="+uid);

		IntList crs=CvsHandler.getCvsRepositoriesForLinuxServerAccount(conn, id);
		for(int c=0;c<crs.size();c++) {
			int cr=crs.getInt(c);
			if(!CvsHandler.isCvsRepositoryDisabled(conn, cr)) {
				throw new SQLException("Cannot disable UserServer #"+id+": CvsRepository not disabled: "+cr);
			}
		}
		IntList hsts=HttpdHandler.getHttpdSharedTomcatsForLinuxServerAccount(conn, id);
		for(int c=0;c<hsts.size();c++) {
			int hst=hsts.getInt(c);
			if(!HttpdHandler.isHttpdSharedTomcatDisabled(conn, hst)) {
				throw new SQLException("Cannot disable UserServer #"+id+": SharedTomcat not disabled: "+hst);
			}
		}
		IntList hss=HttpdHandler.getHttpdSitesForLinuxServerAccount(conn, id);
		for(int c=0;c<hss.size();c++) {
			int hs=hss.getInt(c);
			if(!HttpdHandler.isHttpdSiteDisabled(conn, hs)) {
				throw new SQLException("Cannot disable UserServer #"+id+": Site not disabled: "+hs);
			}
		}
		IntList els=EmailHandler.getEmailListsForLinuxServerAccount(conn, id);
		for(int c=0;c<els.size();c++) {
			int el=els.getInt(c);
			if(!EmailHandler.isEmailListDisabled(conn, el)) {
				throw new SQLException("Cannot disable UserServer #"+id+": List not disabled: "+el);
			}
		}

		conn.executeUpdate(
			"update linux.\"UserServer\" set disable_log=? where id=?",
			disableLog,
			id
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			aoServer,
			false
		);
	}

	public static void enableLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username
	) throws IOException, SQLException {
		int disableLog=getDisableLogForLinuxAccount(conn, username);
		if(disableLog==-1) throw new SQLException("linux.User is already enabled: "+username);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableLinuxAccount", disableLog, true);
		checkAccessLinuxAccount(conn, source, "enableLinuxAccount", username);
		if(UsernameHandler.isUsernameDisabled(conn, username)) throw new SQLException("Unable to enable User '"+username+"', Username not enabled: "+username);

		conn.executeUpdate(
			"update linux.\"User\" set disable_log=null where username=?",
			username
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, username),
			UsernameHandler.getServersForUsername(conn, username),
			false
		);
	}

	public static void enableLinuxServerAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		int disableLog=getDisableLogForLinuxServerAccount(conn, id);
		if(disableLog==-1) throw new SQLException("linux.UserServer is already enabled: "+id);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableLinuxServerAccount", disableLog, true);
		checkAccessLinuxServerAccount(conn, source, "enableLinuxServerAccount", id);
		com.aoindustries.aoserv.client.linux.User.Name la=getUsernameForLinuxServerAccount(conn, id);
		if(isLinuxAccountDisabled(conn, la)) throw new SQLException("Unable to enable UserServer #"+id+", User not enabled: "+la);

		conn.executeUpdate(
			"update linux.\"UserServer\" set disable_log=null where id=?",
			id
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			UsernameHandler.getBusinessForUsername(conn, la),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	/**
	 * Gets the contents of an autoresponder.
	 */
	public static String getAutoresponderContent(DatabaseConnection conn, RequestSource source, int lsa) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "getAutoresponderContent", lsa);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to get the autoresponder content for User named '"+User.MAIL+'\'');
		PosixPath path = conn.executeObjectQuery(ObjectFactories.posixPathFactory,
			"select autoresponder_path from linux.\"UserServer\" where id=?",
			lsa
		);
		String content;
		if(path == null) {
			content="";
		} else {
			int aoServer = getAOServerForLinuxServerAccount(conn, lsa);
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
			conn.releaseConnection();
			content = daemonConnector.getAutoresponderContent(path);
		}
		return content;
	}

	/**
	 * Gets the contents of a user cron table.
	 */
	public static String getCronTable(DatabaseConnection conn, RequestSource source, int lsa) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "getCronTable", lsa);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to get the cron table for User named '"+User.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(UserType.USER)
		) throw new SQLException("Not allowed to get the cron table for LinuxAccounts of type '"+type+"', username="+username);
		int aoServer=getAOServerForLinuxServerAccount(conn, lsa);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		return daemonConnector.getCronTable(username);
	}

	public static int getDisableLogForLinuxAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from linux.\"User\" where username=?", username);
	}

	public static int getDisableLogForLinuxServerAccount(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from linux.\"UserServer\" where id=?", id);
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.LINUX_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledLinuxAccounts.clear();
			}
		} else if(tableID==Table.TableID.LINUX_SERVER_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledLinuxServerAccounts.clear();
			}
		}
	}

	public static boolean isLinuxAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
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

	public static boolean isLinuxAccountDisabled(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Boolean O=disabledLinuxAccounts.get(username);
			if(O != null) return O;
			boolean isDisabled=getDisableLogForLinuxAccount(conn, username)!=-1;
			disabledLinuxAccounts.put(username, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isLinuxAccountEmailType(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
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

	public static boolean isLinuxServerAccountDisabled(DatabaseConnection conn, int id) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Integer I=id;
			Boolean O=disabledLinuxServerAccounts.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForLinuxServerAccount(conn, id)!=-1;
			disabledLinuxServerAccounts.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isLinuxGroupNameAvailable(DatabaseConnection conn, Group.Name groupname) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select name from linux.\"Group\" where name=?) is null", groupname);
	}

	public static int getLinuxServerGroup(DatabaseConnection conn, Group.Name group, int aoServer) throws IOException, SQLException {
		int id=conn.executeIntQuery("select coalesce((select id from linux.\"GroupServer\" where name=? and ao_server=?), -1)", group, aoServer);
		if(id==-1) throw new SQLException("Unable to find GroupServer "+group+" on "+aoServer);
		return id;
	}

	public static Group.Name getPrimaryLinuxGroup(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username, int operatingSystemVersion) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.groupNameFactory,
			"select\n"
			+ "  \"group\"\n"
			+ "from\n"
			+ "  linux.\"GroupUser\"\n"
			+ "where\n"
			+ "  \"user\"=?\n"
			+ "  and \"isPrimary\"\n"
			+ "  and (\n"
			+ "    \"operatingSystemVersion\" is null\n"
			+ "    or \"operatingSystemVersion\"=?\n"
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
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, account);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to check if a password is set for UserServer '"+User.MAIL+'\'');

		int aoServer=getAOServerForLinuxServerAccount(conn, account);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		String crypted = daemonConnector.getEncryptedLinuxAccountPassword(username).getElement1();
		return crypted.length() >= 2 && !User.NO_PASSWORD_CONFIG_VALUE.equals(crypted);
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
				AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
				conn.releaseConnection();
				return daemonConnector.isProcmailManual(account) ? AoservProtocol.TRUE : AoservProtocol.FALSE;
			} catch(IOException err) {
				DaemonHandler.flagDaemonAsDown(aoServer);
				return AoservProtocol.SERVER_DOWN;
			}
		} else {
			return AoservProtocol.SERVER_DOWN;
		}
	}

	public static void removeLinuxAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "removeLinuxAccount", username);

		removeLinuxAccount(conn, invalidateList, username);
	}

	public static void removeLinuxAccount(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username
	) throws IOException, SQLException {
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to remove User with username '"+User.MAIL+'\'');

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
			int id=conn.executeIntQuery("select id from linux.\"UserServer\" where username=? and ao_server=?", username, aoServer);
			removeLinuxServerAccount(conn, invalidateList, id);
		}
		// Delete the group relations for this account
		boolean groupAccountModified = conn.executeUpdate("delete from linux.\"GroupUser\" where \"user\"=?", username) > 0;
		// Delete from the database
		conn.executeUpdate("delete from linux.\"User\" where username=?", username);

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);

		if(ftpModified) invalidateList.addTable(conn, Table.TableID.FTP_GUEST_USERS, accounting, aoServers, false);
		if(groupAccountModified) invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounting, aoServers, false);
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void removeLinuxGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name name
	) throws IOException, SQLException {
		checkAccessLinuxGroup(conn, source, "removeLinuxGroup", name);

		removeLinuxGroup(conn, invalidateList, name);
	}

	public static void removeLinuxGroup(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		Group.Name name
	) throws IOException, SQLException {
		if(
			name.equals(Group.FTPONLY)
			|| name.equals(Group.MAIL)
			|| name.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to remove Group named '"+name+"'");

		// Must not be the primary group for any User
		int primaryCount=conn.executeIntQuery("select count(*) from linux.\"GroupUser\" where \"group\"=? and \"isPrimary\"", name);
		if(primaryCount>0) throw new SQLException("linux_group.name="+name+" is the primary group for "+primaryCount+" Linux "+(primaryCount==1?"account":"accounts"));
		// Get the values for later use
		Account.Name accounting = getBusinessForLinuxGroup(conn, name);
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
		if(aoServers.size()>0) invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_GROUPS, accounting, aoServers, false);
		if(groupAccountsModified) invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounting, aoServers, false);
		invalidateList.addTable(conn, Table.TableID.LINUX_GROUPS, accounting, aoServers, false);
	}

	public static void removeLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		checkAccessLinuxGroupAccount(conn, source, "removeLinuxGroupAccount", id);

		// Must not be a primary group
		boolean isPrimary=conn.executeBooleanQuery("select \"isPrimary\" from linux.\"GroupUser\" where id=?", id);
		if(isPrimary) throw new SQLException("linux.GroupUser.id="+id+" is a primary group");

		// Must be needingful not by SharedTomcatSite to be tying to SharedTomcat please
		int useCount = conn.executeIntQuery(
			"select count(*) from linux.\"GroupUser\" lga, "+
					"linux.\"UserServer\" lsa, "+
					"\"web.tomcat\".\"SharedTomcat\" hst, "+
					"\"web.tomcat\".\"SharedTomcatSite\" htss, "+
					"web.\"Site\" hs "+
						"where lga.\"user\" = lsa.username and "+
						"lsa.id             = hst.linux_server_account and "+
						"htss.tomcat_site   = hs.id and "+
						"lga.\"group\"      = hs.linux_group and "+
						"hst.id             = htss.httpd_shared_tomcat and "+
						"lga.id = ?",
			id
		);
		if (useCount==0) {
			useCount = conn.executeIntQuery(
				"select count(*) from linux.\"GroupUser\" lga, "+
						"linux.\"GroupServer\" lsg, "+
						"\"web.tomcat\".\"SharedTomcat\" hst, "+
						"\"web.tomcat\".\"SharedTomcatSite\" htss, "+
						"web.\"Site\" hs "+
							"where lga.\"group\" = lsg.name and "+
							"lsg.id              = hst.linux_server_group and "+
							"htss.tomcat_site    = hs.id and "+
							"lga.\"user\"        = hs.linux_account and "+
							"hst.id              = htss.httpd_shared_tomcat and "+
							"lga.id = ?",
				id
			);
		}
		if (useCount>0) throw new SQLException("linux_group_account("+id+") has been used by "+useCount+" web.tomcat.SharedTomcatSite.");

		// Get the values for later use
		List<Account.Name> accountings=getBusinessesForLinuxGroupAccount(conn, id);
		IntList aoServers=getAOServersForLinuxGroupAccount(conn, id);
		// Delete the group relations for this group
		conn.executeUpdate("delete from linux.\"GroupUser\" where id=?", id);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accountings, aoServers, false);
	}

	public static void removeUnusedAlternateLinuxGroupAccount(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		Group.Name group,
		com.aoindustries.aoserv.client.linux.User.Name username
	) throws IOException, SQLException {
		int id=conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        lga.id\n"
			+ "      from\n"
			+ "        linux.\"GroupUser\" lga\n"
			+ "      where\n"
			+ "        lga.\"group\"=?\n"
			+ "        and lga.\"user\"=?\n"
			+ "        and not lga.\"isPrimary\"\n"
			+ "        and (\n"
			+ "          select\n"
			+ "            htss.tomcat_site\n"
			+ "          from\n"
			+ "            linux.\"UserServer\" lsa,\n"
			+ "            \"web.tomcat\".\"SharedTomcat\" hst,\n"
			+ "            \"web.tomcat\".\"SharedTomcatSite\" htss,\n"
			+ "            web.\"Site\" hs\n"
			+ "          where\n"
			+ "            lga.\"user\"=lsa.username\n"
			+ "            and lsa.id=hst.linux_server_account\n"
			+ "            and hst.id=htss.httpd_shared_tomcat\n"
			+ "            and htss.tomcat_site=hs.id\n"
			+ "            and hs.linux_group=lga.\"group\"\n"
			+ "          limit 1\n"
			+ "        ) is null\n"
			+ "        and (\n"
			+ "          select\n"
			+ "            htss.tomcat_site\n"
			+ "          from\n"
			+ "            linux.\"GroupServer\" lsg,\n"
			+ "            \"web.tomcat\".\"SharedTomcat\" hst,\n"
			+ "            \"web.tomcat\".\"SharedTomcatSite\" htss,\n"
			+ "            web.\"Site\" hs\n"
			+ "          where\n"
			+ "            lga.\"group\"=lsg.name\n"
			+ "            and lsg.id=hst.linux_server_group\n"
			+ "            and hst.id=htss.httpd_shared_tomcat\n"
			+ "            and htss.tomcat_site=hs.id\n"
			+ "            and hs.linux_account=lga.\"user\"\n"
			+ "          limit 1\n"
			+ "        ) is null\n"
			+ "    ),\n"
			+ "    -1\n"
			+ "  )",
			group,
			username
		);
		if(id!=-1) {
			// Get the values for later use
			List<Account.Name> accountings=getBusinessesForLinuxGroupAccount(conn, id);
			IntList aoServers=getAOServersForLinuxGroupAccount(conn, id);
			conn.executeUpdate("delete from linux.\"GroupUser\" where id=?", id);

			// Notify all clients of the update
			invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accountings, aoServers, false);
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
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, account);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to remove UserServer for user '"+User.MAIL+'\'');

		int aoServer = getAOServerForLinuxServerAccount(conn, account);
		int uidMin = AOServerHandler.getUidMin(conn, aoServer);

		// The UID must be a user UID
		int uid=getUIDForLinuxServerAccount(conn, account);
		if(uid < uidMin) throw new SQLException("Not allowed to remove a system UserServer: id="+account+", uid="+uid);

		// Must not contain a CVS repository
		String home=conn.executeStringQuery("select home from linux.\"UserServer\" where id=?", account);
		int count=conn.executeIntQuery(
			"select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  scm.\"CvsRepository\" cr\n"
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
				conn.executeUpdate("delete from email.\"Address\" where id=?", address);
			}
		}

		Account.Name accounting = getBusinessForLinuxServerAccount(conn, account);

		// Delete the attachment blocks
		if(conn.executeUpdate("delete from email.\"AttachmentBlock\" where linux_server_account=?", account) > 0) {
			invalidateList.addTable(conn, Table.TableID.EMAIL_ATTACHMENT_BLOCKS, accounting, aoServer, false);
		}

		// Delete the account from the server
		conn.executeUpdate("delete from linux.\"UserServer\" where id=?", account);
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_ACCOUNTS, accounting, aoServer, true);

		// Notify all clients of the update
		if(addressesModified) {
			invalidateList.addTable(conn, Table.TableID.LINUX_ACC_ADDRESSES, accounting, aoServer, false);
			invalidateList.addTable(conn, Table.TableID.EMAIL_ADDRESSES, accounting, aoServer, false);
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
		Group.Name groupName=getGroupNameForLinuxServerGroup(conn, group);
		if(
			groupName.equals(Group.FTPONLY)
			|| groupName.equals(Group.MAIL)
			|| groupName.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to remove GroupServer for group '"+groupName+"'");

		// Get the server this group is on
		Account.Name accounting = getBusinessForLinuxServerGroup(conn, group);
		int aoServer=getAOServerForLinuxServerGroup(conn, group);
		// Must not be the primary group for any UserServer on the same server
		int primaryCount=conn.executeIntQuery(
			"select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg\n"
			+ "  inner join linux.\"GroupUser\" lga on lsg.name=lga.\"group\"\n"
			+ "  inner join linux.\"UserServer\" lsa on lga.\"user\"=lsa.username\n"
			+ "  inner join net.\"Host\" se on lsg.ao_server=se.id\n"
			+ "where\n"
			+ "  lsg.id=?\n"
			+ "  and lga.\"isPrimary\"\n"
			+ "  and (\n"
			+ "    lga.\"operatingSystemVersion\" is null\n"
			+ "    or lga.\"operatingSystemVersion\" = se.operating_system_version\n"
			+ "  )\n"
			+ "  and lsg.ao_server=lsa.ao_server",
			group
		);

		if(primaryCount>0) throw new SQLException("linux_server_group.id="+group+" is the primary group for "+primaryCount+" Linux server "+(primaryCount==1?"account":"accounts")+" on "+aoServer);
		// Delete from the database
		conn.executeUpdate("delete from linux.\"GroupServer\" where id=?", group);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_GROUPS, accounting, aoServer, true);
	}

	public static void setAutoresponder(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		int from,
		String subject,
		String content,
		boolean enabled
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "setAutoresponder", id);
		if(isLinuxServerAccountDisabled(conn, id)) throw new SQLException("Unable to set autoresponder, UserServer disabled: "+id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set autoresponder for user '"+User.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(UserType.EMAIL)
			&& !type.equals(UserType.USER)
		) throw new SQLException("Not allowed to set autoresponder for this type of account: "+type);

		// The from must be on this account
		if(from!=-1) {
			int fromLSA=conn.executeIntQuery("select linux_server_account from email.\"InboxAddress\" where id=?", from);
			if(fromLSA!=id) throw new SQLException("((linux_acc_address.id="+from+").linux_server_account="+fromLSA+")!=((linux_server_account.id="+id+").username="+username+")");
		}

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		int aoServer=getAOServerForLinuxServerAccount(conn, id);
		PosixPath path;
		if(content==null && !enabled) {
			path = null;
		} else {
			path = conn.executeObjectQuery(ObjectFactories.posixPathFactory,
				"select coalesce(autoresponder_path, home || '/.autorespond.txt') from linux.\"UserServer\" where id=?",
				id
			);
		}
		int uid;
		int gid;
		if(!enabled) {
			uid=-1;
			gid=-1;
		} else {
			uid = getUIDForLinuxServerAccount(conn, id);
			gid = conn.executeIntQuery(
				"select\n"
				+ "  lsg.gid\n"
				+ "from\n"
				+ "  linux.\"UserServer\" lsa\n"
				+ "  inner join linux.\"GroupUser\" lga on lsa.username=lga.\"user\"\n"
				+ "  inner join linux.\"GroupServer\" lsg on lga.\"group\"=lsg.name\n"
				+ "  inner join net.\"Host\" se on lsa.ao_server=se.id\n"
				+ "where\n"
				+ "  lsa.id=?\n"
				+ "  and lga.\"isPrimary\"\n"
				+ "  and (\n"
				+ "    lga.\"operatingSystemVersion\" is null\n"
				+ "    or lga.\"operatingSystemVersion\" = se.operating_system_version\n"
				+ "  )\n"
				+ "  and lsa.ao_server=lsg.ao_server",
				id
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
				+ "  id=?"
			)
		) {
			try {
				if(from==-1) pstmt.setNull(1, Types.INTEGER);
				else pstmt.setInt(1, from);
				pstmt.setString(2, subject);
				pstmt.setString(3, ObjectUtils.toString(path));
				pstmt.setBoolean(4, enabled);
				pstmt.setInt(5, id);
				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

		// Store the content on the server
		if(path!=null) {
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
			conn.releaseConnection();
			daemonConnector.setAutoresponderContent(path, content==null?"":content, uid, gid);
		}

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_ACCOUNTS, accounting, aoServer, false);
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
		if(isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to set cron table, UserServer disabled: "+lsa);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, lsa);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the cron table for User named '"+User.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(
			!type.equals(UserType.USER)
		) throw new SQLException("Not allowed to set the cron table for LinuxAccounts of type '"+type+"', username="+username);
		int aoServer=getAOServerForLinuxServerAccount(conn, lsa);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.setCronTable(username, cronTable);
	}

	public static void setLinuxAccountHomePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountHomePhone", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set home phone number, User disabled: "+username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set home phone number for user '"+User.MAIL+'\'');

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set home_phone=? where username=?", phone, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountName(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		Gecos name
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountName", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set full name, User disabled: "+username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set LinuxAccountName for user '"+User.MAIL+'\'');

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set name=? where username=?", name, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountOfficeLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		Gecos location
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficeLocation", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set office location, User disabled: "+username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set office location for user '"+User.MAIL+'\'');

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set office_location=? where username=?", location, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountOfficePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficePhone", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set office phone number, User disabled: "+username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set office phone number for user '"+User.MAIL+'\'');

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set office_phone=? where username=?", phone, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxAccountShell(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name username,
		PosixPath shell
	) throws IOException, SQLException {
		checkAccessLinuxAccount(conn, source, "setLinuxAccountOfficeShell", username);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set shell, User disabled: "+username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set shell for account named '"+User.MAIL+'\'');
		String type=getTypeForLinuxAccount(conn, username);
		if(!UserType.isAllowedShell(type, shell)) throw new SQLException("Shell '"+shell+"' not allowed for Linux accounts with the type '"+type+'\'');

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		IntList aoServers=getAOServersForLinuxAccount(conn, username);

		conn.executeUpdate("update linux.\"User\" set shell=? where username=?", shell, username);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, accounting, aoServers, false);
	}

	public static void setLinuxServerAccountPassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String password
	) throws IOException, SQLException {
		BusinessHandler.checkPermission(conn, source, "setLinuxServerAccountPassword", Permission.Name.set_linux_server_account_password);
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountPassword", id);
		if(isLinuxServerAccountDisabled(conn, id)) throw new SQLException("Unable to set UserServer password, account disabled: "+id);

		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set password for UserServer named '"+User.MAIL+"': "+id);
		String type=conn.executeStringQuery("select type from linux.\"User\" where username=?", username);

		// Make sure passwords can be set before doing a strength check
		if(!UserType.canSetPassword(type)) throw new SQLException("Passwords may not be set for UserType="+type);

		if(password!=null && password.length()>0) {
			// Perform the password check here, too.
			List<PasswordChecker.Result> results = User.checkPassword(username, type, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		Account.Name accounting = UsernameHandler.getBusinessForUsername(conn, username);
		int aoServer=getAOServerForLinuxServerAccount(conn, id);
		try {
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
			conn.releaseConnection();
			daemonConnector.setLinuxServerAccountPassword(username, password);
		} catch(IOException | SQLException err) {
			System.err.println("Unable to set linux account password for "+username+" on "+aoServer);
			throw err;
		}

		// Update the linux.Server table for emailmon and ftpmon
		/*if(username.equals(User.EMAILMON)) {
			conn.executeUpdate("update linux.\"Server\" set emailmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
			invalidateList.addTable(conn, Table.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
		} else if(username.equals(User.FTPMON)) {
			conn.executeUpdate("update linux.\"Server\" set ftpmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
			invalidateList.addTable(conn, Table.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
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
			if(isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to clear UserServer predisable password, account disabled: "+lsa);
		} else {
			if(!isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to set UserServer predisable password, account not disabled: "+lsa);
		}

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set predisable_password=? where id=?",
			password,
			lsa
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, lsa),
			getAOServerForLinuxServerAccount(conn, lsa),
			false
		);
	}

	public static void setLinuxServerAccountJunkEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountJunkEmailRetention", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the junk email retention for User named '"+User.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set junk_email_retention=null where id=?",
				id
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set junk_email_retention=? where id=?",
				days,
				id
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinIntegrationMode(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		String mode
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinIntegrationMode", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin integration mode for User named '"+User.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set sa_integration_mode=? where id=?",
			mode,
			id
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinRequiredScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		float required_score
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinRequiredScore", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin required score for User named '"+User.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set sa_required_score=? where id=?",
			required_score,
			id
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	public static void setLinuxServerAccountSpamAssassinDiscardScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		int discard_score
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountSpamAssassinDiscardScore", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin discard score for User named '"+User.MAIL+'\'');

		// Update the database
		if(discard_score==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set sa_discard_score=null where id=?",
				id
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set sa_discard_score=? where id=?",
				discard_score,
				id
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	public static void setLinuxServerAccountTrashEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountTrashEmailRetention", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the trash email retention for User named '"+User.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.executeUpdate(
				"update linux.\"UserServer\" set trash_email_retention=null where id=?",
				id
			);
		} else {
			conn.executeUpdate(
				"update linux.\"UserServer\" set trash_email_retention=? where id=?",
				days,
				id
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
			false
		);
	}

	public static void setLinuxServerAccountUseInbox(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id,
		boolean useInbox
	) throws IOException, SQLException {
		// Security checks
		checkAccessLinuxServerAccount(conn, source, "setLinuxServerAccountUseInbox", id);
		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to set the use_inbox flag for User named '"+User.MAIL+'\'');

		// Update the database
		conn.executeUpdate(
			"update linux.\"UserServer\" set use_inbox=? where id=?",
			useInbox,
			id
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getBusinessForLinuxServerAccount(conn, id),
			getAOServerForLinuxServerAccount(conn, id),
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
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, aoServer);
		conn.releaseConnection();
		daemonConnector.waitForLinuxAccountRebuild();
	}

	static boolean canLinuxGroupAccessServer(DatabaseConnection conn, RequestSource source, Group.Name groupName, int aoServer) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      lg.name\n"
			+ "    from\n"
			+ "      linux.\"Group\" lg,\n"
			+ "      billing.\"Package\" pk,\n"
			+ "      account.\"AccountHost\" bs\n"
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

	static void checkLinuxGroupAccessServer(DatabaseConnection conn, RequestSource source, String action, Group.Name groupName, int server) throws IOException, SQLException {
		if(!canLinuxGroupAccessServer(conn, source, groupName, server)) {
			String message=
				"groupName="
				+groupName
				+" is not allowed to access server.id="
				+server
				+": action='"
				+action
				+"'"
			;
			throw new SQLException(message);
		}
	}

	public static Account.Name getBusinessForLinuxGroup(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select pk.accounting from linux.\"Group\" lg, billing.\"Package\" pk where lg.package=pk.name and lg.name=?",
			name
		);
	}

	public static List<Account.Name> getBusinessesForLinuxGroupAccount(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectCollectionQuery(new ArrayList<Account.Name>(),
			ObjectFactories.accountNameFactory,
		   "select\n"
			+ "  pk1.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga1\n"
			+ "  inner join linux.\"Group\" lg1 on lga1.\"group\"=lg1.name\n"
			+ "  inner join billing.\"Package\" pk1 on lg1.package=pk1.name\n"
			+ "where\n"
			+ "  lga1.id=?\n"
			+ "union select\n"
			+ "  pk2.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga2\n"
			+ "  inner join account.\"User\" un2 on lga2.\"user\" = un2.username\n"
			+ "  inner join billing.\"Package\" pk2 on un2.package=pk2.name\n"
			+ "where\n"
			+ "  lga2.id=?",
			id,
			id
		);
	}

	public static Account.Name getBusinessForLinuxServerAccount(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  lsa.id=?\n"
			+ "  and lsa.username=un.username\n"
			+ "  and un.package=pk.name",
			id
		);
	}

	public static Account.Name getBusinessForLinuxServerGroup(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  lsg.id=?\n"
			+ "  and lsg.name=lg.name\n"
			+ "  and lg.package=pk.name",
			id
		);
	}

	public static Group.Name getGroupNameForLinuxServerGroup(DatabaseConnection conn, int lsgPKey) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.groupNameFactory,
			"select name from linux.\"GroupServer\" where id=?",
			lsgPKey
		);
	}

	public static int getAOServerForLinuxServerAccount(DatabaseConnection conn, int account) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from linux.\"UserServer\" where id=?", account);
	}

	public static int getAOServerForLinuxServerGroup(DatabaseConnection conn, int group) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from linux.\"GroupServer\" where id=?", group);
	}

	public static IntList getAOServersForLinuxAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
		return conn.executeIntListQuery("select ao_server from linux.\"UserServer\" where username=?", username);
	}

	public static IntList getAOServersForLinuxGroup(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.executeIntListQuery("select ao_server from linux.\"GroupServer\" where name=?", name);
	}

	public static IntList getAOServersForLinuxGroupAccount(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  lsg.ao_server\n"
			+ "from\n"
			+ "  linux.\"GroupUser\" lga\n"
			+ "  inner join linux.\"GroupServer\" lsg on lga.\"group\"=lsg.name\n"
			+ "  inner join linux.\"UserServer\" lsa on lga.\"user\"=lsa.username\n"
			+ "  inner join net.\"Host\" se on lsg.ao_server=se.id\n"
			+ "where\n"
			+ "  lga.id=?\n"
			+ "  and lsg.ao_server=lsa.ao_server\n"
			+ "  and (\n"
			+ "    lga.\"operatingSystemVersion\" is null\n"
			+ "    or lga.\"operatingSystemVersion\" = se.operating_system_version\n"
			+ "  )",
			id
		);
	}

	public static String getTypeForLinuxAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
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
			+ "  lsa.id=?\n"
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
			+ "  lsg.id=?\n"
			+ "  and lsg.name=lg.name",
			lsg
		);
	}

	public static int getLinuxServerAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username, int aoServer) throws IOException, SQLException {
		int id=conn.executeIntQuery(
			"select coalesce(\n"
			+ "  (\n"
			+ "    select\n"
			+ "      id\n"
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
		if(id==-1) throw new SQLException("Unable to find UserServer for "+username+" on "+aoServer);
		return id;
	}

	public static IntList getLinuxServerAccountsForLinuxAccount(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
		return conn.executeIntListQuery("select id from linux.\"UserServer\" where username=?", username);
	}

	public static IntList getLinuxServerGroupsForLinuxGroup(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.executeIntListQuery("select id from linux.\"GroupServer\" where name=?", name);
	}

	public static Account.Name getPackageForLinuxGroup(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select\n"
			+ "  package\n"
			+ "from\n"
			+ "  linux.\"Group\"\n"
			+ "where\n"
			+ "  name=?",
			name
		);
	}

	public static Account.Name getPackageForLinuxServerGroup(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.accountNameFactory,
			"select\n"
			+ "  lg.package\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  lsg.id=?\n"
			+ "  and lsg.name=lg.name",
			id
		);
	}

	public static int getGIDForLinuxServerGroup(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select gid from linux.\"GroupServer\" where id=?", id);
	}

	public static int getUIDForLinuxServerAccount(DatabaseConnection conn, int id) throws IOException, SQLException {
		return conn.executeIntQuery("select uid from linux.\"UserServer\" where id=?", id);
	}

	public static com.aoindustries.aoserv.client.linux.User.Name getLinuxAccountForLinuxGroupAccount(DatabaseConnection conn, int lga) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.linuxUserNameFactory,
			"select \"user\" from linux.\"GroupUser\" where id=?",
			lga
		);
	}

	public static Group.Name getLinuxGroupForLinuxGroupAccount(DatabaseConnection conn, int lga) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.groupNameFactory,
			"select \"group\" from linux.\"GroupUser\" where id=?",
			lga
		);
	}

	public static com.aoindustries.aoserv.client.linux.User.Name getUsernameForLinuxServerAccount(DatabaseConnection conn, int account) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.linuxUserNameFactory,
			"select username from linux.\"UserServer\" where id=?",
			account
		);
	}

	public static boolean comparePassword(
		DatabaseConnection conn,
		RequestSource source, 
		int id, 
		String password
	) throws IOException, SQLException {
		checkAccessLinuxServerAccount(conn, source, "comparePassword", id);
		if(isLinuxServerAccountDisabled(conn, id)) throw new SQLException("Unable to compare password, UserServer disabled: "+id);

		com.aoindustries.aoserv.client.linux.User.Name username=getUsernameForLinuxServerAccount(conn, id);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to compare password for UserServer named '"+User.MAIL+"': "+id);
		String type=conn.executeStringQuery("select type from linux.\"User\" where username=?", username);

		// Make sure passwords can be set before doing a comparison
		if(!UserType.canSetPassword(type)) throw new SQLException("Passwords may not be compared for UserType="+type);

		// Perform the password comparison
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForLinuxServerAccount(conn, id)
		);
		conn.releaseConnection();
		return daemonConnector.compareLinuxAccountPassword(username, password);
	}

	public static void setPrimaryLinuxGroupAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int id
	) throws IOException, SQLException {
		checkAccessLinuxGroupAccount(conn, source, "setPrimaryLinuxGroupAccount", id);
		com.aoindustries.aoserv.client.linux.User.Name username = conn.executeObjectQuery(
			ObjectFactories.linuxUserNameFactory,
			"select \"user\" from linux.\"GroupUser\" where id=?",
			id
		);
		if(isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to set primary LinuxGroupUser, User disabled: "+username);
		Group.Name group = conn.executeObjectQuery(ObjectFactories.groupNameFactory,
			"select \"group\" from linux.\"GroupUser\" where id=?",
			id
		);

		conn.executeUpdate(
			"update linux.\"GroupUser\" set \"isPrimary\" = true where id = ?",
			id
		);
		conn.executeUpdate(
			"update linux.\"GroupUser\" set \"isPrimary\" = false where \"isPrimary\" and id != ? and \"user\" = ?",
			id,
			username
		);
		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getCollection(UsernameHandler.getBusinessForUsername(conn, username), getBusinessForLinuxGroup(conn, group)),
			getAOServersForLinuxGroupAccount(conn, id),
			false
		);
	}
}
