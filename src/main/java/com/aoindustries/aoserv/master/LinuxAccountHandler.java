/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.collections.IntList;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.util.Tuple2;
import com.aoapps.lang.Throwables;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoapps.lang.util.InternUtils;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.sql.Connections;
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
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
	private static final String AOADMIN_SUDO = "ALL=(ALL) NOPASSWD:ALL";

	/** Amazon EC2 cloud-init */
	private static final String CENTOS_SUDO = "ALL=(ALL) NOPASSWD:ALL";

	/** Default sudo setting for newly added "aoserv-xen-migration" system users. */
	private static final String AOSERV_XEN_MIGRATION_SUDO = "ALL=(ALL) NOPASSWD: /usr/sbin/xl -t migrate-receive";

	private final static Map<com.aoindustries.aoserv.client.linux.User.Name, Boolean> disabledUsers=new HashMap<>();
	private final static Map<Integer, Boolean> disabledUserServers=new HashMap<>();

	public static void checkAccessUser(DatabaseConnection conn, RequestSource source, String action, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				IntList lsas = getUserServersForUser(conn, user);
				boolean found = false;
				for(Integer lsa : lsas) {
					if(NetHostHandler.canAccessHost(conn, source, getServerForUserServer(conn, lsa))) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"currentAdministrator="
						+source.getCurrentAdministrator()
						+" is not allowed to access linux_account: action='"
						+action
						+", username="
						+user
					;
					throw new SQLException(message);
				}
			}
		} else {
			AccountUserHandler.checkAccessUser(conn, source, action, user);
		}
	}

	public static void checkAccessGroup(DatabaseConnection conn, RequestSource source, String action, Group.Name group) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				IntList lsgs = getGroupServersForGroup(conn, group);
				boolean found = false;
				for(int lsg : lsgs) {
					if(NetHostHandler.canAccessHost(conn, source, getServerForGroupServer(conn, lsg))) {
						found=true;
						break;
					}
				}
				if(!found) {
					String message=
						"currentAdministrator="
						+source.getCurrentAdministrator()
						+" is not allowed to access linux_group: action='"
						+action
						+", name="
						+group
					;
					throw new SQLException(message);
				}
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForGroup(conn, group));
		}
	}

	public static void checkAccessGroupUser(DatabaseConnection conn, RequestSource source, String action, int groupUser) throws IOException, SQLException {
		checkAccessUser(conn, source, action, getUserForGroupUser(conn, groupUser));
		checkAccessGroup(conn, source, action, getGroupForGroupUser(conn, groupUser));
	}

	public static boolean canAccessUserServer(DatabaseConnection conn, RequestSource source, int userServer) throws IOException, SQLException {
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				return NetHostHandler.canAccessHost(conn, source, getServerForUserServer(conn, userServer));
			} else return true;
		} else {
			return AccountUserHandler.canAccessUser(conn, source, getUserForUserServer(conn, userServer));
		}
	}

	public static void checkAccessUserServer(DatabaseConnection conn, RequestSource source, String action, int userServer) throws IOException, SQLException {
		if(!canAccessUserServer(conn, source, userServer)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access linux_server_account: action='"
				+action
				+", id="
				+userServer
			;
			throw new SQLException(message);
		}
	}

	public static boolean canAccessGroupServer(DatabaseConnection conn, RequestSource source, int groupServer) throws IOException, SQLException {
		return
			PackageHandler.canAccessPackage(conn, source, getPackageForGroupServer(conn, groupServer))
			&& NetHostHandler.canAccessHost(conn, source, getServerForGroupServer(conn, groupServer))
		;
	}

	public static void checkAccessGroupServer(DatabaseConnection conn, RequestSource source, String action, int groupServer) throws IOException, SQLException {
		if(!canAccessGroupServer(conn, source, groupServer)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access linux_server_group: action='"
				+action
				+", id="
				+groupServer
			;
			throw new SQLException(message);
		}
	}

	/**
	 * Adds a linux account.
	 */
	public static void addUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		Group.Name primaryGroup,
		Gecos name,
		Gecos office_location,
		Gecos office_phone,
		Gecos home_phone,
		String type,
		PosixPath shell,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to add User named '"+User.MAIL+'\'');

		// Make sure the shell is allowed for the type of account being added
		if(!UserType.isAllowedShell(type, shell)) throw new SQLException("shell='"+shell+"' not allowed for type='"+type+'\'');

		if(!skipSecurityChecks) {
			AccountUserHandler.checkAccessUser(conn, source, "addUser", user);
			if(AccountUserHandler.isUserDisabled(conn, user)) throw new SQLException("Unable to add User, Username disabled: "+user);
		}

		conn.update(
			"insert into linux.\"User\" values(?,?,?,?,?,?,?,now(),null)",
			user,
			name,
			office_location,
			office_phone,
			home_phone,
			type,
			shell
		);
		// Notify all clients of the update
		invalidateList.addTable(conn,
			Table.TableID.LINUX_ACCOUNTS,
			AccountUserHandler.getAccountForUser(conn, user),
			InvalidateList.allHosts,
			false
		);

		addGroupUser(
			conn,
			source,
			invalidateList,
			primaryGroup,
			user,
			true,
			skipSecurityChecks
		);
	}

	public static void addGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name name,
		Account.Name packageName,
		String type,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(!skipSecurityChecks) {
			PackageHandler.checkAccessPackage(conn, source, "addGroup", packageName);
			if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to add Group, Package disabled: "+packageName);
		}
		if (
			name.equals(Group.FTPONLY)
			|| name.equals(Group.MAIL)
			|| name.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to add Group: "+name);

		conn.update("insert into linux.\"Group\" values(?,?,?)", name, packageName, type);

		// Notify all clients of the update
		invalidateList.addTable(conn,
			Table.TableID.LINUX_GROUPS,
			PackageHandler.getAccountForPackage(conn, packageName),
			InvalidateList.allHosts,
			false
		);
	}

	public static int addGroupUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name group,
		com.aoindustries.aoserv.client.linux.User.Name user,
		boolean isPrimary,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(group.equals(Group.MAIL)) throw new SQLException("Not allowed to add GroupUser for group '"+Group.MAIL+'\'');
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to add GroupUser for user '"+User.MAIL+'\'');
		if(!skipSecurityChecks) {
			if(
				!group.equals(Group.FTPONLY)
				&& !group.equals(Group.MAILONLY)
			) checkAccessGroup(conn, source, "addGroupUser", group);
			checkAccessUser(conn, source, "addGroupUser", user);
			if(isUserDisabled(conn, user)) throw new SQLException("Unable to add GroupUser, User disabled: "+user);
		}
		if(group.equals(Group.FTPONLY)) {
			// Only allowed to have ftponly group when it is a ftponly account
			String type=getTypeForUser(conn, user);
			if(!type.equals(UserType.FTPONLY)) throw new SQLException("Not allowed to add GroupUser for group '"+Group.FTPONLY+"' on non-ftp-only-type User named "+user);
		}
		if(group.equals(Group.MAILONLY)) {
			// Only allowed to have mail group when it is a "mailonly" account
			String type=getTypeForUser(conn, user);
			if(!type.equals(UserType.EMAIL)) throw new SQLException("Not allowed to add GroupUser for group '"+Group.MAILONLY+"' on non-email-type User named "+user);
		}

		// Do not allow more than 31 groups per account
		int count=conn.queryInt("select count(*) from linux.\"GroupUser\" where \"user\"=?", user);
		if(count >= GroupUser.MAX_GROUPS) throw new SQLException("Only "+GroupUser.MAX_GROUPS+" groups are allowed per user, username="+user+" already has access to "+count+" groups");

		int groupUser = conn.updateInt(
			"INSERT INTO linux.\"GroupUser\" VALUES (default,?,?,?,null) RETURNING id",
			group,
			user,
			isPrimary
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getAccountCollection(
				AccountUserHandler.getAccountForUser(conn, user),
				getAccountForGroup(conn, group)
			),
			getServersForGroupUser(conn, groupUser),
			false
		);
		return groupUser;
	}

	public static int addUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		int linuxServer,
		PosixPath home,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(user.equals(User.MAIL)) {
			throw new SQLException("Not allowed to add UserServer for user '"+User.MAIL+'\'');
		}
		if(!skipSecurityChecks) {
			checkAccessUser(conn, source, "addUserServer", user);
			if(isUserDisabled(conn, user)) throw new SQLException("Unable to add UserServer, User disabled: "+user);
			NetHostHandler.checkAccessHost(conn, source, "addUserServer", linuxServer);
			AccountUserHandler.checkUserAccessHost(conn, source, "addUserServer", user, linuxServer);
		}

		// OperatingSystem settings
		int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
		if(osv == -1) throw new SQLException("Operating system version not known for server #" + linuxServer);
		PosixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
		PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		if(home.equals(UserServer.getDefaultHomeDirectory(user))) {
			// Make sure no conflicting /home/u/username account exists.
			String prefix = home + "/";
			List<String> conflicting = conn.queryStringList(
				"select distinct home from linux.\"UserServer\" where ao_server=? and substring(home from 1 for " + prefix.length() + ")=? order by home",
				linuxServer,
				prefix
			);
			if(!conflicting.isEmpty()) throw new SQLException("Found conflicting home directories: " + conflicting);
		} else if(home.equals(UserServer.getHashedHomeDirectory(user))) {
			// Make sure no conflicting /home/u account exists.
			String conflictHome = "/home/" + user.toString().charAt(0);
			if(
				conn.queryBoolean(
					"select (select id from linux.\"UserServer\" where ao_server=? and home=? limit 1) is not null",
					linuxServer,
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
				int httpdSite = WebHandler.getSite(conn, linuxServer, siteName);
				if(httpdSite != -1) {
					if(!skipSecurityChecks) {
						// Must be able to access an existing site
						WebHandler.checkAccessSite(conn, source, "addUserServer", httpdSite);
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
				int httpdSharedTomcat = WebHandler.getSharedTomcat(conn, linuxServer, tomcatName);
				if(httpdSharedTomcat != -1) {
					if(!skipSecurityChecks) {
						// Must be able to access an existing site
						WebHandler.checkAccessSharedTomcat(conn, source, "addUserServer", httpdSharedTomcat);
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
		Group.Name primaryGroup=getPrimaryGroup(conn, user, osv);
		int primaryLSG=getGroupServer(conn, primaryGroup, linuxServer);
		if(primaryLSG<0) throw new SQLException("Unable to find primary Linux group '"+primaryGroup+"' on Server #"+linuxServer+" for Linux account '"+user+"'");

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, linuxServer);
		int userServer = conn.updateInt(
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
			+ "  " + (user.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_TRASH_EMAIL_RETENTION)) + ",\n"
			+ "  " + (user.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_JUNK_EMAIL_RETENTION)) + ",\n"
			+ "  ?,\n"
			+ "  " + UserServer.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE + ",\n"
			+ "  " + (user.equals(User.EMAILMON) ? "null::int" : Integer.toString(UserServer.DEFAULT_SPAM_ASSASSIN_DISCARD_SCORE)) + ",\n"
			+ "  null\n" // sudo
			+ ") RETURNING id",
			user,
			linuxServer,
			linuxServer,
			home,
			SpamAssassinMode.DEFAULT_SPAMASSASSIN_INTEGRATION_MODE
		);
		// Notify all clients of the update
		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			account,
			linuxServer,
			true
		);
		// If it is a email type, add the default attachment blocks
		if(!user.equals(User.EMAILMON) && isUserEmailType(conn, user)) {
			conn.update(
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
				userServer
			);
			invalidateList.addTable(
				conn,
				Table.TableID.EMAIL_ATTACHMENT_BLOCKS,
				account,
				linuxServer,
				false
			);
		}
		return userServer;
	}

	public static int addGroupServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name group,
		int linuxServer,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(
			group.equals(Group.FTPONLY)
			|| group.equals(Group.MAIL)
			|| group.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to add GroupServer for group '"+group+'\'');
		Account.Name account = getAccountForGroup(conn, group);
		if(!skipSecurityChecks) {
			checkAccessGroup(conn, source, "addGroupServer", group);
			NetHostHandler.checkAccessHost(conn, source, "addGroupServer", linuxServer);
			checkGroupAccessServer(conn, source, "addGroupServer", group, linuxServer);
			AccountHandler.checkAccountAccessHost(conn, source, "addGroupServer", account, linuxServer);
		}

		// Now allocating unique to entire system for server portability between farms
		//String farm=ServerHandler.getFarmForServer(conn, linuxServer);
		int groupServer = conn.updateInt(
			"INSERT INTO\n"
			+ "  linux.\"GroupServer\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  linux.get_next_gid(?),\n"
			+ "  now()\n"
			+ ") RETURNING id",
			group,
			linuxServer,
			linuxServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_GROUPS,
			account,
			linuxServer,
			true
		);
		return groupServer;
	}

	/**
	 * Gets the group name that exists on a server for the given gid
	 * or {@code null} if the gid is not allocated to the server.
	 */
	public static Group.Name getGroupByGid(DatabaseConnection conn, int linuxServer, int gid) throws SQLException {
		return conn.queryObject(
			Connections.DEFAULT_TRANSACTION_ISOLATION, true, false,
			ObjectFactories.groupNameFactory,
			"select name from linux.\"GroupServer\" where ao_server=? and gid=?",
			linuxServer,
			gid
		);
	}

	/**
	 * Gets the username that exists on a server for the given uid
	 * or {@code null} if the uid is not allocated to the server.
	 */
	public static com.aoindustries.aoserv.client.linux.User.Name getUserByUid(DatabaseConnection conn, int linuxServer, int uid) throws SQLException {
		return conn.queryObject(
			Connections.DEFAULT_TRANSACTION_ISOLATION, true, false,
			ObjectFactories.linuxUserNameFactory,
			"select username from linux.\"UserServer\" where ao_server=? and uid=?",
			linuxServer,
			uid
		);
	}

	public static int addSystemGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int linuxServer,
		Group.Name group,
		int gid
	) throws IOException, SQLException {
		// This must be a master user with access to the server
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu == null) throw new SQLException("Not a master user: " + source.getCurrentAdministrator());
		NetHostHandler.checkAccessHost(conn, source, "addSystemGroup", linuxServer);
		// The group ID must be in the system group range
		if(gid < 0) throw new SQLException("Invalid gid: " + gid);
		int gidMin = LinuxServerHandler.getGidMin(conn, linuxServer);
		int gidMax = LinuxServerHandler.getGidMax(conn, linuxServer);
		// The group ID must not already exist on this server
		{
			Group.Name existing = getGroupByGid(conn, linuxServer, gid);
			if(existing != null) throw new SQLException("Group #" + gid + " already exists on server #" + linuxServer + ": " + existing);
		}
		// Must be one of the expected patterns for the servers operating system version
		int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
		if(
			osv == OperatingSystemVersion.CENTOS_7_X86_64
			&& (
				// Fixed group ids
				   (group.equals(Group.ROOT)            && gid == 0)
				|| (group.equals(Group.BIN)             && gid == 1)
				|| (group.equals(Group.DAEMON)          && gid == 2)
				|| (group.equals(Group.SYS)             && gid == 3)
				|| (group.equals(Group.ADM)             && gid == 4)
				|| (group.equals(Group.TTY)             && gid == 5)
				|| (group.equals(Group.DISK)            && gid == 6)
				|| (group.equals(Group.LP)              && gid == 7)
				|| (group.equals(Group.MEM)             && gid == 8)
				|| (group.equals(Group.KMEM)            && gid == 9)
				|| (group.equals(Group.WHEEL)           && gid == 10)
				|| (group.equals(Group.CDROM)           && gid == 11)
				|| (group.equals(Group.MAIL)            && gid == 12)
				|| (group.equals(Group.MAN)             && gid == 15)
				|| (group.equals(Group.DIALOUT)         && gid == 18)
				|| (group.equals(Group.FLOPPY)          && gid == 19)
				|| (group.equals(Group.GAMES)           && gid == 20)
				|| (group.equals(Group.UTMP)            && gid == 22)
				|| (group.equals(Group.NAMED)           && gid == 25)
				|| (group.equals(Group.POSTGRES)        && gid == 26)
				|| (group.equals(Group.RPCUSER)         && gid == 29)
				|| (group.equals(Group.MYSQL)           && gid == 31)
				|| (group.equals(Group.RPC)             && gid == 32)
				|| (group.equals(Group.TAPE)            && gid == 33)
				|| (group.equals(Group.UTEMPTER)        && gid == 35)
				|| (group.equals(Group.VIDEO)           && gid == 39)
				|| (group.equals(Group.DIP)             && gid == 40)
				|| (group.equals(Group.MAILNULL)        && gid == 47)
				|| (group.equals(Group.APACHE)          && gid == 48)
				|| (group.equals(Group.FTP)             && gid == 50)
				|| (group.equals(Group.SMMSP)           && gid == 51)
				|| (group.equals(Group.LOCK)            && gid == 54)
				|| (group.equals(Group.TSS)             && gid == 59)
				|| (group.equals(Group.AUDIO)           && gid == 63)
				|| (group.equals(Group.TCPDUMP)         && gid == 72)
				|| (group.equals(Group.SSHD)            && gid == 74)
				|| (group.equals(Group.SASLAUTH)        && gid == 76)
				|| (group.equals(Group.AWSTATS)         && gid == 78)
				|| (group.equals(Group.DBUS)            && gid == 81)
				|| (group.equals(Group.MAILONLY)        && gid == 83)
				|| (group.equals(Group.SCREEN)          && gid == 84)
				|| (group.equals(Group.BIRD)            && gid == 95)
				|| (group.equals(Group.NOBODY)          && gid == 99)
				|| (group.equals(Group.USERS)           && gid == 100)
				|| (group.equals(Group.AVAHI_AUTOIPD)   && gid == 170)
				|| (group.equals(Group.DHCPD)           && gid == 177)
				|| (group.equals(Group.SYSTEMD_JOURNAL) && gid == 190)
				|| (group.equals(Group.SYSTEMD_NETWORK) && gid == 192)
				|| (group.equals(Group.NFSNOBODY)       && gid == 65534)
				|| (
					// System groups in range 201 through gidMin - 1
					gid >= CENTOS_7_SYS_GID_MIN
					&& gid < gidMin
					&& (
						   group.equals(Group.AOSERV_JILTER)
						|| group.equals(Group.AOSERV_MASTER)
						|| group.equals(Group.AOSERV_XEN_MIGRATION)
						|| group.equals(Group.CGRED)
						|| group.equals(Group.CHRONY)
						|| group.equals(Group.CLAMSCAN)
						|| group.equals(Group.CLAMUPDATE)
						|| group.equals(Group.INPUT)
						|| group.equals(Group.MEMCACHED)
						|| group.equals(Group.NGINX)
						|| group.equals(Group.POLKITD)
						|| group.equals(Group.REDIS)
						|| group.equals(Group.SSH_KEYS)
						|| group.equals(Group.SYSTEMD_BUS_PROXY)
						|| group.equals(Group.SYSTEMD_NETWORK)
						|| group.equals(Group.UNBOUND)
						|| group.equals(Group.VIRUSGROUP)
					)
				) || (
					// Regular user groups in range gidMin through Group.GID_MAX
					gid >= gidMin
					&& gid <= gidMax
					&& (
						group.equals(Group.AOADMIN)
						// AOServ Schema
						|| group.equals(Group.ACCOUNTING)
						|| group.equals(Group.BILLING)
						|| group.equals(Group.DISTRIBUTION)
						|| group.equals(Group.INFRASTRUCTURE)
						|| group.equals(Group.MANAGEMENT)
						|| group.equals(Group.MONITORING)
						|| group.equals(Group.RESELLER)
						// Amazon EC2 cloud-init
						|| group.equals(Group.CENTOS)
					)
				)
			)
		) {
			int groupServer = conn.updateInt(
				"INSERT INTO\n"
				+ "  linux.\"GroupServer\"\n"
				+ "VALUES (\n"
				+ "  default,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  now()\n"
				+ ") RETURNING id",
				group,
				linuxServer,
				gid
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				Table.TableID.LINUX_SERVER_GROUPS,
				NetHostHandler.getAccountsForHost(conn, linuxServer),
				linuxServer,
				true
			);
			return groupServer;
		} else {
			throw new SQLException("Unexpected system group: " + group + " #" + gid + " on operating system #" + osv);
		}
	}

	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	static class SystemUser {

		static final int ANY_SYSTEM_UID = -1;
		static final int ANY_USER_UID = -2;

		/**
		 * The set of allowed system group patterns for CentOS 7.
		 */
		private static final Map<com.aoindustries.aoserv.client.linux.User.Name, SystemUser> centos7SystemUsers = new HashMap<>();
		private static void addCentos7SystemUser(
			com.aoindustries.aoserv.client.linux.User.Name user,
			int uid,
			Group.Name group,
			String fullName,
			String home,
			PosixPath shell,
			String sudo
		) throws ValidationException {
			if(
				centos7SystemUsers.put(
					user,
					new SystemUser(
						user,
						uid,
						group,
						InternUtils.intern(Gecos.valueOf(fullName)), null, null, null,
						PosixPath.valueOf(home).intern(),
						shell,
						sudo
					)
				) != null
			) throw new AssertionError("Duplicate username: " + user);
		}
		static {
			try {
				try {
					// TODO: We should probably have a database table instead of this hard-coded list, same for system groups
					addCentos7SystemUser(User.ROOT,                              0, Group.ROOT,                 "root",                                                            "/root",                         Shell.BASH,     null);
					addCentos7SystemUser(User.BIN,                               1, Group.BIN,                  "bin",                                                             "/bin",                          Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.DAEMON,                            2, Group.DAEMON,               "daemon",                                                          "/sbin",                         Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.ADM,                               3, Group.ADM,                  "adm",                                                             "/var/adm",                      Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.LP,                                4, Group.LP,                   "lp",                                                              "/var/spool/lpd",                Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SYNC,                              5, Group.ROOT,                 "sync",                                                            "/sbin",                         Shell.SYNC,     null);
					addCentos7SystemUser(User.SHUTDOWN,                          6, Group.ROOT,                 "shutdown",                                                        "/sbin",                         Shell.SHUTDOWN, null);
					addCentos7SystemUser(User.HALT,                              7, Group.ROOT,                 "halt",                                                            "/sbin",                         Shell.HALT,     null);
					addCentos7SystemUser(User.MAIL,                              8, Group.MAIL,                 "mail",                                                            "/var/spool/mail",               Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.OPERATOR,                         11, Group.ROOT,                 "operator",                                                        "/root",                         Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.GAMES,                            12, Group.USERS,                "games",                                                           "/usr/games",                    Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.FTP,                              14, Group.FTP,                  "FTP User",                                                        "/var/ftp",                      Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.NAMED,                            25, Group.NAMED,                "Named",                                                           "/var/named",                    Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.POSTGRES,                         26, Group.POSTGRES,             "PostgreSQL Server",                                               "/var/lib/pgsql",                Shell.BASH,     null);
					addCentos7SystemUser(User.RPCUSER,                          29, Group.RPCUSER,              "RPC Service User",                                                "/var/lib/nfs",                  Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.MYSQL,                            31, Group.MYSQL,                "MySQL server",                                                    "/var/lib/mysql",                Shell.BASH,     null);
					addCentos7SystemUser(User.RPC,                              32, Group.RPC,                  "Rpcbind Daemon",                                                  "/var/lib/rpcbind",              Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.MAILNULL,                         47, Group.MAILNULL,             null,                                                              "/var/spool/mqueue",             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.APACHE,                           48, Group.APACHE,               "Apache",                                                          "/usr/share/httpd",              Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SMMSP,                            51, Group.SMMSP,                null,                                                              "/var/spool/mqueue",             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.TSS,                              59, Group.TSS,                  "Account used by the trousers package to sandbox the tcsd daemon", "/dev/null",                     Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.TCPDUMP,                          72, Group.TCPDUMP,              null,                                                              "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SSHD,                             74, Group.SSHD,                 "Privilege-separated SSH",                                         "/var/empty/sshd",               Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.CYRUS,                            76, Group.MAIL,                 "Cyrus IMAP Server",                                               "/var/lib/imap",                 Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.AWSTATS,                          78, Group.AWSTATS,              "AWStats Background Log Processing",                               "/var/opt/awstats",              Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.DBUS,                             81, Group.DBUS,                 "System message bus",                                              "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.BIRD,                             95, Group.BIRD,                 "BIRD Internet Routing Daemon",                                    "/var/opt/bird",                 Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.NOBODY,                           99, Group.NOBODY,               "Nobody",                                                          "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.AVAHI_AUTOIPD,                   170, Group.AVAHI_AUTOIPD,        "Avahi IPv4LL Stack",                                              "/var/lib/avahi-autoipd",        Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.DHCPD,                           177, Group.DHCPD,                "DHCP server",                                                     "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SYSTEMD_NETWORK,                 192, Group.SYSTEMD_NETWORK,      "systemd Network Management",                                      "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.NFSNOBODY,                     65534, Group.NFSNOBODY,            "Anonymous NFS User",                                              "/var/lib/nfs",                  Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.AOSERV_JILTER,        ANY_SYSTEM_UID, Group.AOSERV_JILTER,        "AOServ Jilter",                                                   "/var/opt/aoserv-jilter",        Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.AOSERV_MASTER,        ANY_SYSTEM_UID, Group.AOSERV_MASTER,        "AOServ Master",                                                   "/var/opt/aoserv-master",        Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.CHRONY,               ANY_SYSTEM_UID, Group.CHRONY,               null,                                                              "/var/lib/chrony",               Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.CLAMSCAN,             ANY_SYSTEM_UID, Group.CLAMSCAN,             "Clamav scanner user",                                             "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.CLAMUPDATE,           ANY_SYSTEM_UID, Group.CLAMUPDATE,           "Clamav database update user",                                     "/var/lib/clamav",               Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.MEMCACHED,            ANY_SYSTEM_UID, Group.MEMCACHED,            "Memcached daemon",                                                "/run/memcached",                Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.NGINX,                ANY_SYSTEM_UID, Group.NGINX,                "nginx user",                                                      "/var/cache/nginx",              Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.POLKITD,              ANY_SYSTEM_UID, Group.POLKITD,              "User for polkitd",                                                "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.REDIS,                ANY_SYSTEM_UID, Group.REDIS,                "Redis Database Server",                                           "/var/lib/redis",                Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SASLAUTH,             ANY_SYSTEM_UID, Group.SASLAUTH,             "Saslauthd user",                                                  "/run/saslauthd",                Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.SYSTEMD_BUS_PROXY,    ANY_SYSTEM_UID, Group.SYSTEMD_BUS_PROXY,    "systemd Bus Proxy",                                               "/",                             Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.UNBOUND,              ANY_SYSTEM_UID, Group.UNBOUND,              "Unbound DNS resolver",                                            "/etc/unbound",                  Shell.NOLOGIN,  null);
					addCentos7SystemUser(User.AOADMIN,              ANY_USER_UID,   Group.AOADMIN,              "AO Industries Administrator",                                     "/home/aoadmin",                 Shell.BASH,     AOADMIN_SUDO);
					addCentos7SystemUser(User.AOSERV_XEN_MIGRATION, ANY_SYSTEM_UID, Group.AOSERV_XEN_MIGRATION, "AOServ Xen Migration",                                            "/var/opt/aoserv-xen-migration", Shell.BASH,     AOSERV_XEN_MIGRATION_SUDO);
					// AOServ Schema:
					addCentos7SystemUser(User.ACCOUNTING,           ANY_USER_UID,   Group.ACCOUNTING,           "masterdb access",                                                 "/home/accounting",              Shell.BASH,     null);
					addCentos7SystemUser(User.BILLING,              ANY_USER_UID,   Group.BILLING,              "masterdb access",                                                 "/home/billing",                 Shell.BASH,     null);
					addCentos7SystemUser(User.DISTRIBUTION,         ANY_USER_UID,   Group.DISTRIBUTION,         "masterdb access",                                                 "/home/distribution",            Shell.BASH,     null);
					addCentos7SystemUser(User.INFRASTRUCTURE,       ANY_USER_UID,   Group.INFRASTRUCTURE,       "masterdb access",                                                 "/home/infrastructure",          Shell.BASH,     null);
					addCentos7SystemUser(User.MANAGEMENT,           ANY_USER_UID,   Group.MANAGEMENT,           "masterdb access",                                                 "/home/management",              Shell.BASH,     null);
					addCentos7SystemUser(User.MONITORING,           ANY_USER_UID,   Group.MONITORING,           "masterdb access",                                                 "/home/monitoring",              Shell.BASH,     null);
					addCentos7SystemUser(User.RESELLER,             ANY_USER_UID,   Group.RESELLER,             "masterdb access",                                                 "/home/reseller",                Shell.BASH,     null);
					// Amazon EC2 cloud-init
					addCentos7SystemUser(User.CENTOS,               ANY_USER_UID,   Group.CENTOS,               "Cloud User",                                                      "/home/centos",                  Shell.BASH,     CENTOS_SUDO);
				} catch(ValidationException e) {
					throw new AssertionError("These hard-coded values are valid", e);
				}
			} catch(Throwable t) {
				t.printStackTrace(System.err);
				throw Throwables.wrap(t, ExceptionInInitializerError.class, ExceptionInInitializerError::new);
			}
		}

		final com.aoindustries.aoserv.client.linux.User.Name user;
		final int uid;
		final Group.Name group;
		final Gecos fullName;
		final Gecos officeLocation;
		final Gecos officePhone;
		final Gecos homePhone;
		final PosixPath home;
		final PosixPath shell;
		final String sudo;

		SystemUser(
			com.aoindustries.aoserv.client.linux.User.Name user,
			int uid,
			Group.Name group,
			Gecos fullName,
			Gecos officeLocation,
			Gecos officePhone,
			Gecos homePhone,
			PosixPath home,
			PosixPath shell,
			String sudo
		) {
			this.user = user;
			this.uid = uid;
			this.group = group;
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
		int linuxServer,
		com.aoindustries.aoserv.client.linux.User.Name user,
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
		com.aoindustries.aoserv.client.master.User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu == null) throw new SQLException("Not a master user: " + source.getCurrentAdministrator());
		NetHostHandler.checkAccessHost(conn, source, "addSystemUser", linuxServer);
		// The user ID must be in the system user range
		if(uid < 0) throw new SQLException("Invalid uid: " + uid);
		int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);
		int uidMax = LinuxServerHandler.getUidMax(conn, linuxServer);
		// The user ID must not already exist on this server
		{
			com.aoindustries.aoserv.client.linux.User.Name existing = getUserByUid(conn, linuxServer, uid);
			if(existing != null) throw new SQLException("User #" + uid + " already exists on server #" + linuxServer + ": " + existing);
		}
		// Get the group name for the requested gid
		Group.Name group = getGroupByGid(conn, linuxServer, gid);
		if(group == null) throw new SQLException("Group #" + gid + " does not exist on server #" + linuxServer);
		// Must be one of the expected patterns for the servers operating system version
		int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
		SystemUser systemUser;
		if(
			osv == OperatingSystemVersion.CENTOS_7_X86_64
			&& (systemUser = SystemUser.centos7SystemUsers.get(user)) != null
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
			if(!Objects.equals(group,      systemUser.group))      throw new SQLException("Unexpected system group: "          + group      + " != " + systemUser.group);
			if(!Objects.equals(fullName,       systemUser.fullName))       throw new SQLException("Unexpected system fullName: "       + fullName       + " != " + systemUser.fullName);
			if(!Objects.equals(officeLocation, systemUser.officeLocation)) throw new SQLException("Unexpected system officeLocation: " + officeLocation + " != " + systemUser.officeLocation);
			if(!Objects.equals(officePhone,    systemUser.officePhone))    throw new SQLException("Unexpected system officePhone: "    + officePhone    + " != " + systemUser.officePhone);
			if(!Objects.equals(homePhone,      systemUser.homePhone))      throw new SQLException("Unexpected system homePhone: "      + homePhone      + " != " + systemUser.homePhone);
			if(!Objects.equals(home,           systemUser.home))           throw new SQLException("Unexpected system home: "           + home           + " != " + systemUser.home);
			if(!Objects.equals(shell,          systemUser.shell))          throw new SQLException("Unexpected system shell: "          + shell          + " != " + systemUser.shell);
			// Add to database
			int userServer = conn.updateInt(
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
				user,
				linuxServer,
				uid,
				home,
				SpamAssassinMode.NONE,
				systemUser.sudo
			);
			// Notify all clients of the update
			invalidateList.addTable(
				conn,
				Table.TableID.LINUX_SERVER_ACCOUNTS,
				NetHostHandler.getAccountsForHost(conn, linuxServer),
				linuxServer,
				true
			);
			return userServer;
		} else {
			throw new SQLException("Unexpected system user: " + user + " #" + uid + " on operating system #" + osv);
		}
	}

	/**
	 * Copies the contents of a home directory from one server to another.
	 */
	public static long copyHomeDirectory(
		DatabaseConnection conn,
		RequestSource source,
		int from_userServer,
		int to_server
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "copyHomeDirectory", from_userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, from_userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to copy User named '"+User.MAIL+'\'');
		int from_server=getServerForUserServer(conn, from_userServer);
		int to_lsa=conn.queryInt(
			"select id from linux.\"UserServer\" where username=? and ao_server=?",
			user,
			to_server
		);
		checkAccessUserServer(conn, source, "copyHomeDirectory", to_lsa);
		String type=getTypeForUser(conn, user);
		if(
			!type.equals(UserType.USER)
			&& !type.equals(UserType.EMAIL)
			&& !type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy LinuxAccounts of type '"+type+"', username="+user);

		AOServDaemonConnector fromDaemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
		AOServDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
		conn.close(); // Don't hold database connection while connecting to the daemon
		long byteCount = fromDaemonConnector.copyHomeDirectory(user, toDaemonConnector);
		return byteCount;
	}

	/**
	 * Copies a password from one linux account to another
	 */
	public static void copyUserServerPassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int from_userServer,
		int to_userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "copyLinuxServerAccountPassword", from_userServer);
		if(isUserServerDisabled(conn, from_userServer)) throw new SQLException("Unable to copy UserServer password, from account disabled: "+from_userServer);
		com.aoindustries.aoserv.client.linux.User.Name from_user = getUserForUserServer(conn, from_userServer);
		if(from_user.equals(User.MAIL)) throw new SQLException("Not allowed to copy the password from User named '"+User.MAIL+'\'');
		checkAccessUserServer(conn, source, "copyLinuxServerAccountPassword", to_userServer);
		if(isUserServerDisabled(conn, to_userServer)) throw new SQLException("Unable to copy UserServer password, to account disabled: "+to_userServer);
		com.aoindustries.aoserv.client.linux.User.Name to_user = getUserForUserServer(conn, to_userServer);
		if(to_user.equals(User.MAIL)) throw new SQLException("Not allowed to copy the password to User named '"+User.MAIL+'\'');

		int from_server=getServerForUserServer(conn, from_userServer);
		int to_server=getServerForUserServer(conn, to_userServer);

		String from_type=getTypeForUser(conn, from_user);
		if(
			!from_type.equals(UserType.APPLICATION)
			&& !from_type.equals(UserType.USER)
			&& !from_type.equals(UserType.EMAIL)
			&& !from_type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords from LinuxAccounts of type '"+from_type+"', username="+from_user);

		String to_type=getTypeForUser(conn, to_user);
		if(
			!to_type.equals(UserType.APPLICATION)
			&& !to_type.equals(UserType.USER)
			&& !to_type.equals(UserType.EMAIL)
			&& !to_type.equals(UserType.FTPONLY)
		) throw new SQLException("Not allowed to copy passwords to LinuxAccounts of type '"+to_type+"', username="+to_user);

		AOServDaemonConnector fromDemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
		AOServDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
		conn.close(); // Don't hold database connection while connecting to the daemon
		Tuple2<String, Integer> enc_password = fromDemonConnector.getEncryptedLinuxAccountPassword(from_user);
		toDaemonConnector.setEncryptedLinuxAccountPassword(to_user, enc_password.getElement1(), enc_password.getElement2());

		//Account.Name from_account=UsernameHandler.getAccountForUsername(conn, from_username);
		//Account.Name to_account=UsernameHandler.getAccountForUsername(conn, to_username);
	}

	public static void disableUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		com.aoindustries.aoserv.client.linux.User.Name user
	) throws IOException, SQLException {
		AccountHandler.checkAccessDisableLog(conn, source, "disableUser", disableLog, false);
		checkAccessUser(conn, source, "disableUser", user);
		if(isUserDisabled(conn, user)) throw new SQLException("linux.User is already disabled: "+user);
		IntList lsas=getUserServersForUser(conn, user);
		for(int c=0;c<lsas.size();c++) {
			int lsa=lsas.getInt(c);
			if(!isUserServerDisabled(conn, lsa)) {
				throw new SQLException("Cannot disable User '"+user+"': UserServer not disabled: "+lsa);
			}
		}

		conn.update(
			"update linux.\"User\" set disable_log=? where username=?",
			disableLog,
			user
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_ACCOUNTS,
			AccountUserHandler.getAccountForUser(conn, user),
			AccountUserHandler.getHostsForUser(conn, user),
			false
		);
	}

	public static void disableUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int userServer
	) throws IOException, SQLException {
		AccountHandler.checkAccessDisableLog(conn, source, "disableUserServer", disableLog, false);
		checkAccessUserServer(conn, source, "disableUserServer", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("linux.UserServer is already disabled: "+userServer);

		int linuxServer = getServerForUserServer(conn, userServer);
		int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);

		// The UID must be a user UID
		int uid=getUidForUserServer(conn, userServer);
		if(uid < uidMin) throw new SQLException("Not allowed to disable a system UserServer: id="+userServer+", uid="+uid);

		IntList crs=CvsHandler.getCvsRepositoriesForLinuxUserServer(conn, userServer);
		for(int c=0;c<crs.size();c++) {
			int cr=crs.getInt(c);
			if(!CvsHandler.isCvsRepositoryDisabled(conn, cr)) {
				throw new SQLException("Cannot disable UserServer #"+userServer+": CvsRepository not disabled: "+cr);
			}
		}
		IntList hsts=WebHandler.getSharedTomcatsForLinuxUserServer(conn, userServer);
		for(int c=0;c<hsts.size();c++) {
			int hst=hsts.getInt(c);
			if(!WebHandler.isSharedTomcatDisabled(conn, hst)) {
				throw new SQLException("Cannot disable UserServer #"+userServer+": SharedTomcat not disabled: "+hst);
			}
		}
		IntList hss=WebHandler.getSitesForLinuxUserServer(conn, userServer);
		for(int c=0;c<hss.size();c++) {
			int hs=hss.getInt(c);
			if(!WebHandler.isSiteDisabled(conn, hs)) {
				throw new SQLException("Cannot disable UserServer #"+userServer+": Site not disabled: "+hs);
			}
		}
		IntList els=EmailHandler.getListsForLinuxUserServer(conn, userServer);
		for(int c=0;c<els.size();c++) {
			int el=els.getInt(c);
			if(!EmailHandler.isListDisabled(conn, el)) {
				throw new SQLException("Cannot disable UserServer #"+userServer+": List not disabled: "+el);
			}
		}

		conn.update(
			"update linux.\"UserServer\" set disable_log=? where id=?",
			disableLog,
			userServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			linuxServer,
			false
		);
	}

	public static void enableUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "enableUser", user);
		int disableLog=getDisableLogForUser(conn, user);
		if(disableLog==-1) throw new SQLException("linux.User is already enabled: "+user);
		AccountHandler.checkAccessDisableLog(conn, source, "enableUser", disableLog, true);
		if(AccountUserHandler.isUserDisabled(conn, user)) throw new SQLException("Unable to enable User '"+user+"', Username not enabled: "+user);

		conn.update(
			"update linux.\"User\" set disable_log=null where username=?",
			user
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_ACCOUNTS,
			AccountUserHandler.getAccountForUser(conn, user),
			AccountUserHandler.getHostsForUser(conn, user),
			false
		);
	}

	public static void enableUserServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "enableUserServer", userServer);
		int disableLog=getDisableLogForUserServer(conn, userServer);
		if(disableLog==-1) throw new SQLException("linux.UserServer is already enabled: "+userServer);
		AccountHandler.checkAccessDisableLog(conn, source, "enableUserServer", disableLog, true);
		com.aoindustries.aoserv.client.linux.User.Name la=getUserForUserServer(conn, userServer);
		if(isUserDisabled(conn, la)) throw new SQLException("Unable to enable UserServer #"+userServer+", User not enabled: "+la);

		conn.update(
			"update linux.\"UserServer\" set disable_log=null where id=?",
			userServer
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			AccountUserHandler.getAccountForUser(conn, la),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	/**
	 * Gets the contents of an autoresponder.
	 */
	public static String getAutoresponderContent(DatabaseConnection conn, RequestSource source, int userServer) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "getAutoresponderContent", userServer);
		com.aoindustries.aoserv.client.linux.User.Name name = getUserForUserServer(conn, userServer);
		if(name.equals(User.MAIL)) throw new SQLException("Not allowed to get the autoresponder content for User named '"+User.MAIL+'\'');
		PosixPath path = conn.queryObject(
			ObjectFactories.posixPathFactory,
			"select autoresponder_path from linux.\"UserServer\" where id=?",
			userServer
		);
		String content;
		if(path == null) {
			content="";
		} else {
			int linuxServer = getServerForUserServer(conn, userServer);
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
			// TODO: Add a listener to daemonConnector, which would ensure (and maybe close?) no active database connection
			//       while performing I/O with a daemon.
			conn.close(); // Don't hold database connection while connecting to the daemon
			content = daemonConnector.getAutoresponderContent(path);
		}
		return content;
	}

	/**
	 * Gets the contents of a user cron table.
	 */
	public static String getCronTable(DatabaseConnection conn, RequestSource source, int userServer) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "getCronTable", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to get the cron table for User named '"+User.MAIL+'\'');
		String type = getTypeForUser(conn, user);
		if(!type.equals(UserType.USER)) {
			throw new SQLException("Not allowed to get the cron table for LinuxAccounts of type '"+type+"', username="+user);
		}
		int linuxServer = getServerForUserServer(conn, userServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.getCronTable(user);
	}

	public static int getDisableLogForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryInt("select coalesce(disable_log, -1) from linux.\"User\" where username=?", user);
	}

	public static int getDisableLogForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryInt("select coalesce(disable_log, -1) from linux.\"UserServer\" where id=?", userServer);
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.LINUX_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledUsers.clear();
			}
		} else if(tableID==Table.TableID.LINUX_SERVER_ACCOUNTS) {
			synchronized(LinuxAccountHandler.class) {
				disabledUserServers.clear();
			}
		}
	}

	public static boolean isUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryBoolean(
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
			user
		);
	}

	public static boolean isUserDisabled(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Boolean O = disabledUsers.get(user);
			if(O != null) return O;
			boolean isDisabled = getDisableLogForUser(conn, user)!=-1;
			disabledUsers.put(user, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isUserEmailType(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryBoolean(
			"select\n"
			+ "  lat.is_email\n"
			+ "from\n"
			+ "  linux.\"User\" la,\n"
			+ "  linux.\"UserType\" lat\n"
			+ "where\n"
			+ "  la.username=?\n"
			+ "  and la.type=lat.name",
			user
		);
	}

	public static boolean isUserServerDisabled(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		synchronized(LinuxAccountHandler.class) {
			Integer I=userServer;
			Boolean O=disabledUserServers.get(I);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForUserServer(conn, userServer)!=-1;
			disabledUserServers.put(I, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isLinuxGroupAvailable(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.queryBoolean("select (select name from linux.\"Group\" where name=?) is null", name);
	}

	public static int getGroupServer(DatabaseConnection conn, Group.Name group, int linuxServer) throws IOException, SQLException {
		int groupServer = conn.queryInt("select coalesce((select id from linux.\"GroupServer\" where name=? and ao_server=?), -1)", group, linuxServer);
		if(groupServer == -1) throw new SQLException("Unable to find GroupServer " + group + " on " + linuxServer);
		return groupServer;
	}

	public static Group.Name getPrimaryGroup(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user, int operatingSystemVersion) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.groupNameFactory,
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
			user,
			operatingSystemVersion
		);
	}

	public static boolean isUserServerPasswordSet(
		DatabaseConnection conn,
		RequestSource source, 
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "isUserServerPasswordSet", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to check if a password is set for UserServer '"+User.MAIL+'\'');

		int linuxServer = getServerForUserServer(conn, userServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		String crypted = daemonConnector.getEncryptedLinuxAccountPassword(user).getElement1();
		return crypted.length() >= 2 && !User.NO_PASSWORD_CONFIG_VALUE.equals(crypted);
	}

	public static int isUserServerProcmailManual(
		DatabaseConnection conn,
		RequestSource source, 
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "isUserServerProcmailManual", userServer);

		int linuxServer = getServerForUserServer(conn, userServer);
		if(DaemonHandler.isDaemonAvailable(linuxServer)) {
			try {
				AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
				conn.close(); // Don't hold database connection while connecting to the daemon
				return daemonConnector.isProcmailManual(userServer) ? AoservProtocol.TRUE : AoservProtocol.FALSE;
			} catch(IOException err) {
				DaemonHandler.flagDaemonAsDown(linuxServer);
				return AoservProtocol.SERVER_DOWN;
			}
		} else {
			return AoservProtocol.SERVER_DOWN;
		}
	}

	public static void removeUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "removeUser", user);

		removeUser(conn, invalidateList, user);
	}

	public static void removeUser(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user
	) throws IOException, SQLException {
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to remove User with username '"+User.MAIL+'\'');

		// Detach the linux account from its autoresponder address
		IntList linuxServers = getServersForUser(conn, user);
		for(int c=0;c<linuxServers.size();c++) {
			int linuxServer = linuxServers.getInt(c);
			conn.update("update linux.\"UserServer\" set autoresponder_from=null where username=? and ao_server=?", user, linuxServer);
		}
		// Delete any FTP guest user info attached to this account
		boolean ftpModified = conn.update("delete from ftp.\"GuestUser\" where username=?", user) > 0;
		// Delete the account from all servers
		// Get the values for later use
		for(int c=0;c<linuxServers.size();c++) {
			int linuxServer = linuxServers.getInt(c);
			int userServer = conn.queryInt("select id from linux.\"UserServer\" where username=? and ao_server=?", user, linuxServer);
			removeUserServer(conn, invalidateList, userServer);
		}
		// Delete the group relations for this account
		boolean groupAccountModified = conn.update("delete from linux.\"GroupUser\" where \"user\"=?", user) > 0;
		// Delete from the database
		conn.update("delete from linux.\"User\" where username=?", user);

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);

		if(ftpModified) invalidateList.addTable(conn, Table.TableID.FTP_GUEST_USERS, account, linuxServers, false);
		if(groupAccountModified) invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, account, linuxServers, false);
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void removeGroup(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Group.Name group
	) throws IOException, SQLException {
		checkAccessGroup(conn, source, "removeGroup", group);

		removeGroup(conn, invalidateList, group);
	}

	public static void removeGroup(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		Group.Name group
	) throws IOException, SQLException {
		if(
			group.equals(Group.FTPONLY)
			|| group.equals(Group.MAIL)
			|| group.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to remove Group named '"+group+"'");

		// Must not be the primary group for any User
		int primaryCount=conn.queryInt("select count(*) from linux.\"GroupUser\" where \"group\"=? and \"isPrimary\"", group);
		if(primaryCount>0) throw new SQLException("linux_group.name="+group+" is the primary group for "+primaryCount+" Linux "+(primaryCount==1?"account":"accounts"));
		// Get the values for later use
		Account.Name account = getAccountForGroup(conn, group);
		IntList linuxServers = getServersForGroup(conn, group);
		for(int c=0;c<linuxServers.size();c++) {
			int linuxServer = linuxServers.getInt(c);
			conn.update("delete from linux.\"GroupServer\" where name=? and ao_server=?", group, linuxServer);
		}
		// Delete the group relations for this group
		boolean groupAccountsModified=conn.queryInt("select count(*) from linux.\"GroupUser\" where \"group\"=? limit 1", group)>0;
		if(groupAccountsModified) conn.update("delete from linux.\"GroupUser\" where \"group\"=?", group);
		// Delete from the database
		conn.update("delete from linux.\"Group\" where name=?", group);

		// Notify all clients of the update
		if(linuxServers.size()>0) invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_GROUPS, account, linuxServers, false);
		if(groupAccountsModified) invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, account, linuxServers, false);
		invalidateList.addTable(conn, Table.TableID.LINUX_GROUPS, account, linuxServers, false);
	}

	public static void removeGroupUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int groupUser
	) throws IOException, SQLException {
		checkAccessGroupUser(conn, source, "removeGroupUser", groupUser);

		// Must not be a primary group
		boolean isPrimary=conn.queryBoolean("select \"isPrimary\" from linux.\"GroupUser\" where id=?", groupUser);
		if(isPrimary) throw new SQLException("linux.GroupUser.id="+groupUser+" is a primary group");

		// Must be needingful not by SharedTomcatSite to be tying to SharedTomcat please
		int useCount = conn.queryInt(
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
			groupUser
		);
		if (useCount==0) {
			useCount = conn.queryInt(
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
				groupUser
			);
		}
		if (useCount>0) throw new SQLException("linux_group_account("+groupUser+") has been used by "+useCount+" web.tomcat.SharedTomcatSite.");

		// Get the values for later use
		List<Account.Name> accounts = getAccountsForGroupUser(conn, groupUser);
		IntList linuxServers = getServersForGroupUser(conn, groupUser);
		// Delete the group relations for this group
		conn.update("delete from linux.\"GroupUser\" where id=?", groupUser);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, linuxServers, false);
	}

	/* Unused 2019-07-10:
	public static void removeUnusedAlternateGroupUser(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		Group.Name group,
		com.aoindustries.aoserv.client.linux.User.Name user
	) throws IOException, SQLException {
		int groupUser = conn.queryInt(
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
			user
		);
		if(groupUser!=-1) {
			// Get the values for later use
			List<Account.Name> accounts = getAccountsForGroupUser(conn, groupUser);
			IntList linuxServers = getServersForGroupUser(conn, groupUser);
			conn.update("delete from linux.\"GroupUser\" where id=?", groupUser);

			// Notify all clients of the update
			invalidateList.addTable(conn, Table.TableID.LINUX_GROUP_ACCOUNTS, accounts, linuxServers, false);
		}
	}
	 */

	public static void removeUserServer(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		int userServer
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "removeUserServer", userServer);

		removeUserServer(conn, invalidateList, userServer);
	}

	public static void removeUserServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int userServer
	) throws IOException, SQLException {
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to remove UserServer for user '"+User.MAIL+'\'');

		int linuxServer = getServerForUserServer(conn, userServer);
		int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);

		// The UID must be a user UID
		int uid=getUidForUserServer(conn, userServer);
		if(uid < uidMin) throw new SQLException("Not allowed to remove a system UserServer: id="+userServer+", uid="+uid);

		// Must not contain a CVS repository
		String home=conn.queryString("select home from linux.\"UserServer\" where id=?", userServer);
		int count=conn.queryInt(
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
			userServer,
			home,
			home+'/'
		);
		if(count>0) throw new SQLException("Home directory on "+linuxServer+" contains "+count+" CVS "+(count==1?"repository":"repositories")+": "+home);

		// Delete the email configurations that depend on this account
		IntList addresses=conn.queryIntList("select email_address from email.\"InboxAddress\" where linux_server_account=?", userServer);
		int size=addresses.size();
		boolean addressesModified=size>0;
		for(int c=0;c<size;c++) {
			int address=addresses.getInt(c);
			conn.update("delete from email.\"InboxAddress\" where email_address=?", address);
			if(!EmailHandler.isAddressUsed(conn, address)) {
				conn.update("delete from email.\"Address\" where id=?", address);
			}
		}

		Account.Name account = getAccountForUserServer(conn, userServer);

		// Delete the attachment blocks
		if(conn.update("delete from email.\"AttachmentBlock\" where linux_server_account=?", userServer) > 0) {
			invalidateList.addTable(conn, Table.TableID.EMAIL_ATTACHMENT_BLOCKS, account, linuxServer, false);
		}

		// Delete the account from the server
		conn.update("delete from linux.\"UserServer\" where id=?", userServer);
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_ACCOUNTS, account, linuxServer, true);

		// Notify all clients of the update
		if(addressesModified) {
			invalidateList.addTable(conn, Table.TableID.LINUX_ACC_ADDRESSES, account, linuxServer, false);
			invalidateList.addTable(conn, Table.TableID.EMAIL_ADDRESSES, account, linuxServer, false);
		}
	}

	public static void removeGroupServer(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int groupServer
	) throws IOException, SQLException {
		checkAccessGroupServer(conn, source, "removeGroupServer", groupServer);

		removeGroupServer(conn, invalidateList, groupServer);
	}

	public static void removeGroupServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int groupServer
	) throws IOException, SQLException {
		Group.Name group = getGroupForGroupServer(conn, groupServer);
		if(
			group.equals(Group.FTPONLY)
			|| group.equals(Group.MAIL)
			|| group.equals(Group.MAILONLY)
		) throw new SQLException("Not allowed to remove GroupServer for group '"+group+"'");

		// Get the server this group is on
		Account.Name account = getAccountForGroupServer(conn, groupServer);
		int linuxServer = getServerForGroupServer(conn, groupServer);
		// Must not be the primary group for any UserServer on the same server
		int primaryCount=conn.queryInt(
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
			groupServer
		);

		if(primaryCount>0) throw new SQLException("linux_server_group.id="+groupServer+" is the primary group for "+primaryCount+" Linux server "+(primaryCount==1?"account":"accounts")+" on "+linuxServer);
		// Delete from the database
		conn.update("delete from linux.\"GroupServer\" where id=?", groupServer);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_GROUPS, account, linuxServer, true);
	}

	public static void setAutoresponder(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		int from,
		String subject,
		String content,
		boolean enabled
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "setAutoresponder", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set autoresponder, UserServer disabled: "+userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set autoresponder for user '"+User.MAIL+'\'');
		String type=getTypeForUser(conn, user);
		if(
			!type.equals(UserType.EMAIL)
			&& !type.equals(UserType.USER)
		) throw new SQLException("Not allowed to set autoresponder for this type of account: "+type);

		// The from must be on this account
		if(from!=-1) {
			int fromLSA=conn.queryInt("select linux_server_account from email.\"InboxAddress\" where id=?", from);
			if(fromLSA!=userServer) throw new SQLException("((linux_acc_address.id="+from+").linux_server_account="+fromLSA+")!=((linux_server_account.id="+userServer+").username="+user+")");
		}

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		int linuxServer = getServerForUserServer(conn, userServer);
		PosixPath path;
		if(content==null && !enabled) {
			path = null;
		} else {
			path = conn.queryObject(
				ObjectFactories.posixPathFactory,
				"select coalesce(autoresponder_path, home || '/.autorespond.txt') from linux.\"UserServer\" where id=?",
				userServer
			);
		}
		int uid;
		int gid;
		if(!enabled) {
			uid=-1;
			gid=-1;
		} else {
			uid = getUidForUserServer(conn, userServer);
			gid = conn.queryInt(
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
				userServer
			);
		}
		try (
			PreparedStatement pstmt = conn.getConnection().prepareStatement(
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
				pstmt.setString(3, Objects.toString(path, null));
				pstmt.setBoolean(4, enabled);
				pstmt.setInt(5, userServer);
				pstmt.executeUpdate();
			} catch(Error | RuntimeException | SQLException e) {
				ErrorPrinter.addSQL(e, pstmt);
				throw e;
			}
		}

		// Store the content on the server
		if(path!=null) {
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
			conn.commit();
			conn.close(); // Don't hold database connection while connecting to the daemon
			daemonConnector.setAutoresponderContent(path, content==null?"":content, uid, gid);
		}

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_SERVER_ACCOUNTS, account, linuxServer, false);
	}

	/**
	 * Gets the contents of a user cron table.
	 */
	public static void setCronTable(
		DatabaseConnection conn,
		RequestSource source,
		int userServer,
		String cronTable
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "setCronTable", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set cron table, UserServer disabled: "+userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the cron table for User named '"+User.MAIL+'\'');
		String type=getTypeForUser(conn, user);
		if(
			!type.equals(UserType.USER)
		) throw new SQLException("Not allowed to set the cron table for LinuxAccounts of type '"+type+"', username="+user);
		int linuxServer = getServerForUserServer(conn, userServer);

		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.setCronTable(user, cronTable);
	}

	public static void setUserHomePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "setUserHomePhone", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set home phone number, User disabled: "+user);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set home phone number for user '"+User.MAIL+'\'');

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		IntList linuxServers = getServersForUser(conn, user);

		conn.update("update linux.\"User\" set home_phone=? where username=?", phone, user);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void setUserFullName(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		Gecos name
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "setUserFullName", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set full name, User disabled: "+user);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set LinuxAccountName for user '"+User.MAIL+'\'');

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		IntList linuxServers = getServersForUser(conn, user);

		conn.update("update linux.\"User\" set name=? where username=?", name, user);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void setUserOfficeLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		Gecos location
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "setUserOfficeLocation", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set office location, User disabled: "+user);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set office location for user '"+User.MAIL+'\'');

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		IntList linuxServers = getServersForUser(conn, user);

		conn.update("update linux.\"User\" set office_location=? where username=?", location, user);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void setUserOfficePhone(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		Gecos phone
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "setUserOfficePhone", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set office phone number, User disabled: "+user);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set office phone number for user '"+User.MAIL+'\'');

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		IntList linuxServers = getServersForUser(conn, user);

		conn.update("update linux.\"User\" set office_phone=? where username=?", phone, user);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void setUserShell(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		com.aoindustries.aoserv.client.linux.User.Name user,
		PosixPath shell
	) throws IOException, SQLException {
		checkAccessUser(conn, source, "setUserShell", user);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set shell, User disabled: "+user);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set shell for account named '"+User.MAIL+'\'');
		String type=getTypeForUser(conn, user);
		if(!UserType.isAllowedShell(type, shell)) throw new SQLException("Shell '"+shell+"' not allowed for Linux accounts with the type '"+type+'\'');

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		IntList linuxServers = getServersForUser(conn, user);

		conn.update("update linux.\"User\" set shell=? where username=?", shell, user);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.LINUX_ACCOUNTS, account, linuxServers, false);
	}

	public static void setUserServerPassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		String password
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setUserServerPassword", Permission.Name.set_linux_server_account_password);
		checkAccessUserServer(conn, source, "setUserServerPassword", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set UserServer password, account disabled: "+userServer);

		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set password for UserServer named '"+User.MAIL+"': "+userServer);
		String type=conn.queryString("select type from linux.\"User\" where username=?", user);

		// Make sure passwords can be set before doing a strength check
		if(!UserType.canSetPassword(type)) throw new SQLException("Passwords may not be set for UserType="+type);

		if(password!=null && password.length()>0) {
			// Perform the password check here, too.
			List<PasswordChecker.Result> results = User.checkPassword(user, type, password);
			if(PasswordChecker.hasResults(results)) throw new SQLException("Invalid password: "+PasswordChecker.getResultsString(results).replace('\n', '|'));
		}

		Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
		int linuxServer = getServerForUserServer(conn, userServer);
		try {
			AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
			conn.close(); // Don't hold database connection while connecting to the daemon
			daemonConnector.setLinuxServerAccountPassword(user, password);
		} catch(IOException | SQLException err) {
			System.err.println("Unable to set linux account password for "+user+" on "+linuxServer);
			throw err;
		}

		// Update the linux.Server table for emailmon and ftpmon
		/*if(username.equals(User.EMAILMON)) {
			conn.update("update linux.\"Server\" set emailmon_password=? where server=?", password==null||password.length()==0?null:password, linuxServer);
			invalidateList.addTable(conn, Table.TableID.AO_SERVERS, ServerHandler.getAccountsForHost(conn, linuxServer), linuxServer, false);
		} else if(username.equals(User.FTPMON)) {
			conn.update("update linux.\"Server\" set ftpmon_password=? where server=?", password==null||password.length()==0?null:password, linuxServer);
			invalidateList.addTable(conn, Table.TableID.AO_SERVERS, ServerHandler.getAccountsForHost(conn, linuxServer), linuxServer, false);
		}*/
	}

	public static void setUserServerPredisablePassword(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		String password
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "setUserServerPredisablePassword", userServer);
		if(password==null) {
			if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to clear UserServer predisable password, account disabled: "+userServer);
		} else {
			if(!isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to set UserServer predisable password, account not disabled: "+userServer);
		}

		// Update the database
		conn.update(
			"update linux.\"UserServer\" set predisable_password=? where id=?",
			password,
			userServer
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerJunkEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerJunkEmailRetention", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the junk email retention for User named '"+User.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.update(
				"update linux.\"UserServer\" set junk_email_retention=null where id=?",
				userServer
			);
		} else {
			conn.update(
				"update linux.\"UserServer\" set junk_email_retention=? where id=?",
				days,
				userServer
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerSpamAssassinIntegrationMode(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		String mode
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerSpamAssassinIntegrationMode", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin integration mode for User named '"+User.MAIL+'\'');

		// Update the database
		conn.update(
			"update linux.\"UserServer\" set sa_integration_mode=? where id=?",
			mode,
			userServer
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerSpamAssassinRequiredScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		float requiredScore
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerSpamAssassinRequiredScore", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin required score for User named '"+User.MAIL+'\'');

		// Update the database
		conn.update(
			"update linux.\"UserServer\" set sa_required_score=? where id=?",
			requiredScore,
			userServer
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerSpamAssassinDiscardScore(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		int discardScore
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerSpamAssassinDiscardScore", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the spam assassin discard score for User named '"+User.MAIL+'\'');

		// Update the database
		if(discardScore==-1) {
			conn.update(
				"update linux.\"UserServer\" set sa_discard_score=null where id=?",
				userServer
			);
		} else {
			conn.update(
				"update linux.\"UserServer\" set sa_discard_score=? where id=?",
				discardScore,
				userServer
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerTrashEmailRetention(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		int days
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerTrashEmailRetention", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the trash email retention for User named '"+User.MAIL+'\'');

		// Update the database
		if(days==-1) {
			conn.update(
				"update linux.\"UserServer\" set trash_email_retention=null where id=?",
				userServer
			);
		} else {
			conn.update(
				"update linux.\"UserServer\" set trash_email_retention=? where id=?",
				days,
				userServer
			);
		}

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	public static void setUserServerUseInbox(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int userServer,
		boolean useInbox
	) throws IOException, SQLException {
		// Security checks
		checkAccessUserServer(conn, source, "setUserServerUseInbox", userServer);
		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to set the use_inbox flag for User named '"+User.MAIL+'\'');

		// Update the database
		conn.update(
			"update linux.\"UserServer\" set use_inbox=? where id=?",
			useInbox,
			userServer
		);

		invalidateList.addTable(
			conn,
			Table.TableID.LINUX_SERVER_ACCOUNTS,
			getAccountForUserServer(conn, userServer),
			getServerForUserServer(conn, userServer),
			false
		);
	}

	/**
	 * Waits for any pending or processing account rebuild to complete.
	 */
	public static void waitForUserRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int linuxServer
	) throws IOException, SQLException {
		NetHostHandler.checkAccessHost(conn, source, "waitForLinuxAccountRebuild", linuxServer);
		NetHostHandler.waitForInvalidates(linuxServer);
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
		conn.close(); // Don't hold database connection while connecting to the daemon
		daemonConnector.waitForLinuxAccountRebuild();
	}

	static boolean canGroupAccessServer(DatabaseConnection conn, RequestSource source, Group.Name group, int linuxServer) throws IOException, SQLException {
		return conn.queryBoolean(
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
			group,
			linuxServer
		);
	}

	static void checkGroupAccessServer(DatabaseConnection conn, RequestSource source, String action, Group.Name group, int linuxServer) throws IOException, SQLException {
		if(!canGroupAccessServer(conn, source, group, linuxServer)) {
			throw new SQLException(
				"group="
				+ group
				+ " is not allowed to access server="
				+ linuxServer
				+ ": action='"
				+ action
				+ "'"
			);
		}
	}

	public static Account.Name getAccountForGroup(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select pk.accounting from linux.\"Group\" lg, billing.\"Package\" pk where lg.package=pk.name and lg.name=?",
			name
		);
	}

	// TODO: Is this still relevant?
	public static List<Account.Name> getAccountsForGroupUser(DatabaseConnection conn, int groupUser) throws IOException, SQLException {
		return conn.queryList(
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
			groupUser,
			groupUser
		);
	}

	public static Account.Name getAccountForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
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
			userServer
		);
	}

	public static Account.Name getAccountForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
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
			groupServer
		);
	}

	public static Group.Name getGroupForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.groupNameFactory,
			"select name from linux.\"GroupServer\" where id=?",
			groupServer
		);
	}

	public static int getServerForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryInt("select ao_server from linux.\"UserServer\" where id=?", userServer);
	}

	public static int getServerForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryInt("select ao_server from linux.\"GroupServer\" where id=?", groupServer);
	}

	public static IntList getServersForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryIntList("select ao_server from linux.\"UserServer\" where username=?", user);
	}

	public static IntList getServersForGroup(DatabaseConnection conn, Group.Name group) throws IOException, SQLException {
		return conn.queryIntList("select ao_server from linux.\"GroupServer\" where name=?", group);
	}

	public static IntList getServersForGroupUser(DatabaseConnection conn, int groupUser) throws IOException, SQLException {
		return conn.queryIntList(
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
			groupUser
		);
	}

	public static String getTypeForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryString("select type from linux.\"User\" where username=?", user);
	}

	public static String getTypeForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryString(
			"select\n"
			+ "  la.type\n"
			+ "from\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  linux.\"User\" la\n"
			+ "where\n"
			+ "  lsa.id=?\n"
			+ "  and lsa.username=la.username",
			userServer
		);
   }

	public static String getTypeForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryString(
			"select\n"
			+ "  lg.type\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  lsg.id=?\n"
			+ "  and lsg.name=lg.name",
			groupServer
		);
	}

	public static int getUserServer(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user, int linuxServer) throws IOException, SQLException {
		int userServer = conn.queryInt(
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
			user,
			linuxServer
		);
		if(userServer == -1) throw new SQLException("Unable to find UserServer for " + user + " on " + linuxServer);
		return userServer;
	}

	public static IntList getUserServersForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
		return conn.queryIntList("select id from linux.\"UserServer\" where username=?", user);
	}

	public static IntList getGroupServersForGroup(DatabaseConnection conn, Group.Name group) throws IOException, SQLException {
		return conn.queryIntList("select id from linux.\"GroupServer\" where name=?", group);
	}

	public static Account.Name getPackageForGroup(DatabaseConnection conn, Group.Name group) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  package\n"
			+ "from\n"
			+ "  linux.\"Group\"\n"
			+ "where\n"
			+ "  name=?",
			group
		);
	}

	public static Account.Name getPackageForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select\n"
			+ "  lg.package\n"
			+ "from\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  lsg.id=?\n"
			+ "  and lsg.name=lg.name",
			groupServer
		);
	}

	public static int getGidForGroupServer(DatabaseConnection conn, int groupServer) throws IOException, SQLException {
		return conn.queryInt("select gid from linux.\"GroupServer\" where id=?", groupServer);
	}

	public static int getUidForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryInt("select uid from linux.\"UserServer\" where id=?", userServer);
	}

	public static com.aoindustries.aoserv.client.linux.User.Name getUserForGroupUser(DatabaseConnection conn, int groupUser) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.linuxUserNameFactory,
			"select \"user\" from linux.\"GroupUser\" where id=?",
			groupUser
		);
	}

	public static Group.Name getGroupForGroupUser(DatabaseConnection conn, int groupUser) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.groupNameFactory,
			"select \"group\" from linux.\"GroupUser\" where id=?",
			groupUser
		);
	}

	public static com.aoindustries.aoserv.client.linux.User.Name getUserForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.linuxUserNameFactory,
			"select username from linux.\"UserServer\" where id=?",
			userServer
		);
	}

	public static boolean comparePassword(
		DatabaseConnection conn,
		RequestSource source, 
		int userServer, 
		String password
	) throws IOException, SQLException {
		checkAccessUserServer(conn, source, "comparePassword", userServer);
		if(isUserServerDisabled(conn, userServer)) throw new SQLException("Unable to compare password, UserServer disabled: "+userServer);

		com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
		if(user.equals(User.MAIL)) throw new SQLException("Not allowed to compare password for UserServer named '"+User.MAIL+"': "+userServer);
		String type=conn.queryString("select type from linux.\"User\" where username=?", user);

		// Make sure passwords can be set before doing a comparison
		if(!UserType.canSetPassword(type)) throw new SQLException("Passwords may not be compared for UserType="+type);

		// Perform the password comparison
		AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn,
			getServerForUserServer(conn, userServer)
		);
		conn.close(); // Don't hold database connection while connecting to the daemon
		return daemonConnector.compareLinuxAccountPassword(user, password);
	}

	public static void setPrimaryGroupUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int groupUser
	) throws IOException, SQLException {
		checkAccessGroupUser(conn, source, "setPrimaryGroupUser", groupUser);
		com.aoindustries.aoserv.client.linux.User.Name user = conn.queryObject(
			ObjectFactories.linuxUserNameFactory,
			"select \"user\" from linux.\"GroupUser\" where id=?",
			groupUser
		);
		if(isUserDisabled(conn, user)) throw new SQLException("Unable to set primary GroupUser, User disabled: "+user);
		Group.Name group = conn.queryObject(
			ObjectFactories.groupNameFactory,
			"select \"group\" from linux.\"GroupUser\" where id=?",
			groupUser
		);

		conn.update(
			"update linux.\"GroupUser\" set \"isPrimary\" = true where id = ?",
			groupUser
		);
		conn.update(
			"update linux.\"GroupUser\" set \"isPrimary\" = false where \"isPrimary\" and id != ? and \"user\" = ?",
			groupUser,
			user
		);
		// Notify all clients of the update
		invalidateList.addTable(conn,
			Table.TableID.LINUX_GROUP_ACCOUNTS,
			InvalidateList.getAccountCollection(AccountUserHandler.getAccountForUser(conn, user), getAccountForGroup(conn, group)),
			getServersForGroupUser(conn, groupUser),
			false
		);
	}
}
