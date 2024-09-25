/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024  AO Industries, Inc.
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
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.util.Tuple2;
import com.aoapps.lang.Throwables;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoapps.lang.util.InternUtils;
import com.aoapps.lang.validation.ValidationException;
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
import com.aoindustries.aoserv.daemon.client.AoservDaemonConnector;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The <code>LinuxAccountHandler</code> handles all the accesses to the Linux tables.
 *
 * @author  AO Industries, Inc.
 */
public final class LinuxAccountHandler {

  /** Make no instances. */
  private LinuxAccountHandler() {
    throw new AssertionError();
  }

  /** Matches value in /etc/login.defs on CentOS 7. */
  private static final int
      CENTOS_7_SYS_GID_MIN = 201,
      CENTOS_7_SYS_UID_MIN = 201;

  /** Matches value in /etc/login.defs on Rocky 9. */
  private static final int
      ROCKY_9_SYS_GID_MIN = 201,
      ROCKY_9_SYS_UID_MIN = 201;

  /** See {@link #isFixedSystemGroup(int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Tuple2<Group.Name, Integer>> CENTOS_7_FIXED_SYSTEM_GROUPS = new HashSet<>(Arrays.asList(
      new Tuple2<>(Group.ROOT, 0),
      new Tuple2<>(Group.BIN, 1),
      new Tuple2<>(Group.DAEMON, 2),
      new Tuple2<>(Group.SYS, 3),
      new Tuple2<>(Group.ADM, 4),
      new Tuple2<>(Group.TTY, 5),
      new Tuple2<>(Group.DISK, 6),
      new Tuple2<>(Group.LP, 7),
      new Tuple2<>(Group.MEM, 8),
      new Tuple2<>(Group.KMEM, 9),
      new Tuple2<>(Group.WHEEL, 10),
      new Tuple2<>(Group.CDROM, 11),
      new Tuple2<>(Group.MAIL, 12),
      new Tuple2<>(Group.MAN, 15),
      new Tuple2<>(Group.OPROFILE, 16),
      new Tuple2<>(Group.DIALOUT, 18),
      new Tuple2<>(Group.FLOPPY, 19),
      new Tuple2<>(Group.GAMES, 20),
      new Tuple2<>(Group.UTMP, 22),
      new Tuple2<>(Group.NAMED, 25),
      new Tuple2<>(Group.POSTGRES, 26),
      new Tuple2<>(Group.RPCUSER, 29),
      new Tuple2<>(Group.MYSQL, 31),
      new Tuple2<>(Group.RPC, 32),
      new Tuple2<>(Group.TAPE, 33),
      new Tuple2<>(Group.UTEMPTER, 35),
      new Tuple2<>(Group.VIDEO, 39),
      new Tuple2<>(Group.DIP, 40),
      new Tuple2<>(Group.MAILNULL, 47),
      new Tuple2<>(Group.APACHE, 48),
      new Tuple2<>(Group.FTP, 50),
      new Tuple2<>(Group.SMMSP, 51),
      new Tuple2<>(Group.LOCK, 54),
      new Tuple2<>(Group.TSS, 59),
      new Tuple2<>(Group.AUDIO, 63),
      new Tuple2<>(Group.TCPDUMP, 72),
      new Tuple2<>(Group.SSHD, 74),
      new Tuple2<>(Group.SASLAUTH, 76),
      new Tuple2<>(Group.AWSTATS, 78),
      new Tuple2<>(Group.DBUS, 81),
      new Tuple2<>(Group.MAILONLY, 83),
      new Tuple2<>(Group.SCREEN, 84),
      new Tuple2<>(Group.BIRD, 95),
      new Tuple2<>(Group.NOBODY, 99),
      new Tuple2<>(Group.USERS, 100),
      new Tuple2<>(Group.STAPUSR, 156),
      new Tuple2<>(Group.STAPSYS, 157),
      new Tuple2<>(Group.STAPDEV, 158),
      new Tuple2<>(Group.AVAHI_AUTOIPD, 170),
      new Tuple2<>(Group.DHCPD, 177),
      new Tuple2<>(Group.SYSTEMD_JOURNAL, 190),
      new Tuple2<>(Group.SYSTEMD_NETWORK, 192),
      new Tuple2<>(Group.NFSNOBODY, 65534)
  ));

  /** See {@link #isFixedSystemGroup(int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Tuple2<Group.Name, Integer>> ROCKY_9_FIXED_SYSTEM_GROUPS = new HashSet<>(Arrays.asList(
      new Tuple2<>(Group.ROOT, 0),
      new Tuple2<>(Group.BIN, 1),
      new Tuple2<>(Group.DAEMON, 2),
      new Tuple2<>(Group.SYS, 3),
      new Tuple2<>(Group.ADM, 4),
      new Tuple2<>(Group.TTY, 5),
      new Tuple2<>(Group.DISK, 6),
      new Tuple2<>(Group.LP, 7),
      new Tuple2<>(Group.MEM, 8),
      new Tuple2<>(Group.KMEM, 9),
      new Tuple2<>(Group.WHEEL, 10),
      new Tuple2<>(Group.CDROM, 11),
      new Tuple2<>(Group.MAIL, 12),
      new Tuple2<>(Group.MAN, 15),
      new Tuple2<>(Group.DIALOUT, 18),
      new Tuple2<>(Group.FLOPPY, 19),
      new Tuple2<>(Group.GAMES, 20),
      new Tuple2<>(Group.UTMP, 22),
      new Tuple2<>(Group.NAMED, 25),
      new Tuple2<>(Group.POSTGRES, 26),
      new Tuple2<>(Group.RPCUSER, 29),
      new Tuple2<>(Group.MYSQL, 31),
      new Tuple2<>(Group.RPC, 32),
      new Tuple2<>(Group.TAPE, 33),
      new Tuple2<>(Group.UTEMPTER, 35),
      new Tuple2<>(Group.KVM, 36),
      new Tuple2<>(Group.VIDEO, 39),
      new Tuple2<>(Group.APACHE, 48),
      new Tuple2<>(Group.FTP, 50),
      new Tuple2<>(Group.LOCK, 54),
      new Tuple2<>(Group.TSS, 59),
      new Tuple2<>(Group.AUDIO, 63),
      new Tuple2<>(Group.TCPDUMP, 72),
      new Tuple2<>(Group.SSHD, 74),
      new Tuple2<>(Group.SASLAUTH, 76),
      new Tuple2<>(Group.AWSTATS, 78),
      new Tuple2<>(Group.DBUS, 81),
      new Tuple2<>(Group.SCREEN, 84),
      new Tuple2<>(Group.USERS, 100),
      new Tuple2<>(Group.RTKIT, 172),
      new Tuple2<>(Group.DHCPD, 177),
      new Tuple2<>(Group.SYSTEMD_JOURNAL, 190),
      new Tuple2<>(Group.NOBODY, 65534)
  ));

  /** See {@link #isDynamicSystemGroup(int, int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Group.Name> CENTOS_7_DYNAMIC_SYSTEM_GROUPS = new HashSet<>(Arrays.asList(
      Group.AOSERV_JILTER,
      Group.AOSERV_MASTER,
      Group.AOSERV_XEN_MIGRATION,
      Group.CGRED,
      Group.CHRONY,
      Group.CLAMSCAN,
      Group.CLAMUPDATE,
      Group.INPUT,
      Group.JENKINS,
      Group.MEMCACHED,
      Group.NGINX,
      Group.POLKITD,
      Group.REDIS,
      Group.SSH_KEYS,
      Group.SYSTEMD_BUS_PROXY,
      Group.SYSTEMD_NETWORK,
      Group.UNBOUND,
      Group.VIRUSGROUP
  ));

  /** See {@link #isDynamicSystemGroup(int, int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Group.Name> ROCKY_9_DYNAMIC_SYSTEM_GROUPS = new HashSet<>(Arrays.asList(
      Group.AOSERV_JILTER,
      Group.AOSERV_MASTER,
      Group.CHRONY,
      Group.CLAMSCAN,
      Group.CLAMUPDATE,
      Group.FLATPAK,
      Group.GEOCLUE,
      Group.INPUT,
      Group.JENKINS,
      Group.MEMCACHED,
      Group.NGINX,
      Group.PIPEWIRE,
      Group.POLKITD,
      Group.RENDER,
      Group.SGX,
      Group.SSH_KEYS,
      Group.SSSD,
      Group.SYSTEMD_COREDUMP,
      Group.SYSTEMD_OOM,
      Group.UNBOUND,
      Group.VIRUSGROUP
  ));

  /** See {@link #isRegularUserGroup(int, int, int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Group.Name> CENTOS_7_REGULAR_USER_GROUPS = new HashSet<>(Arrays.asList(
      Group.AOADMIN,
      // AOServ Schema
      Group.ACCOUNTING,
      Group.BILLING,
      Group.DISTRIBUTION,
      Group.INFRASTRUCTURE,
      Group.MANAGEMENT,
      Group.MONITORING,
      Group.RESELLER,
      // Amazon EC2 cloud-init
      Group.CENTOS,
      // SonarQube
      Group.SONARQUBE
  ));

  /** See {@link #isRegularUserGroup(int, int, int, com.aoindustries.aoserv.client.linux.Group.Name, int)}. */
  private static final Set<Group.Name> ROCKY_9_REGULAR_USER_GROUPS = new HashSet<>(Arrays.asList(
      Group.AOADMIN,
      // AOServ Schema
      Group.ACCOUNTING,
      Group.BILLING,
      Group.DISTRIBUTION,
      Group.INFRASTRUCTURE,
      Group.MANAGEMENT,
      Group.MONITORING,
      Group.RESELLER,
      // SonarQube
      Group.SONARQUBE
  ));

  /** Default sudo setting for newly added "aoadmin" system users. */
  private static final String AOADMIN_SUDO = "ALL=(ALL) NOPASSWD:ALL";

  /** Amazon EC2 cloud-init. */
  private static final String CENTOS_SUDO = "ALL=(ALL) NOPASSWD:ALL";

  /** Default sudo setting for newly added "aoserv-xen-migration" system users. */
  private static final String AOSERV_XEN_MIGRATION_SUDO = "ALL=(ALL) NOPASSWD: /usr/sbin/xl -t migrate-receive";

  private static final Map<com.aoindustries.aoserv.client.linux.User.Name, Boolean> disabledUsers = new HashMap<>();
  private static final Map<Integer, Boolean> disabledUserServers = new HashMap<>();

  public static void checkAccessUser(DatabaseConnection conn, RequestSource source, String action, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
    com.aoindustries.aoserv.client.master.User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        IntList lsas = getUserServersForUser(conn, user);
        boolean found = false;
        for (Integer lsa : lsas) {
          if (NetHostHandler.canAccessHost(conn, source, getServerForUserServer(conn, lsa))) {
            found = true;
            break;
          }
        }
        if (!found) {
          String message =
              "currentAdministrator="
                  + source.getCurrentAdministrator()
                  + " is not allowed to access linux_account: action='"
                  + action
                  + ", username="
                  + user;
          throw new SQLException(message);
        }
      }
    } else {
      AccountUserHandler.checkAccessUser(conn, source, action, user);
    }
  }

  public static void checkAccessGroup(DatabaseConnection conn, RequestSource source, String action, Group.Name group) throws IOException, SQLException {
    com.aoindustries.aoserv.client.master.User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        IntList lsgs = getGroupServersForGroup(conn, group);
        boolean found = false;
        for (int lsg : lsgs) {
          if (NetHostHandler.canAccessHost(conn, source, getServerForGroupServer(conn, lsg))) {
            found = true;
            break;
          }
        }
        if (!found) {
          String message =
              "currentAdministrator="
                  + source.getCurrentAdministrator()
                  + " is not allowed to access linux_group: action='"
                  + action
                  + ", name="
                  + group;
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
    com.aoindustries.aoserv.client.master.User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        return NetHostHandler.canAccessHost(conn, source, getServerForUserServer(conn, userServer));
      } else {
        return true;
      }
    } else {
      return AccountUserHandler.canAccessUser(conn, source, getUserForUserServer(conn, userServer));
    }
  }

  public static void checkAccessUserServer(DatabaseConnection conn, RequestSource source, String action, int userServer) throws IOException, SQLException {
    if (!canAccessUserServer(conn, source, userServer)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access linux_server_account: action='"
              + action
              + ", id="
              + userServer;
      throw new SQLException(message);
    }
  }

  public static boolean canAccessGroupServer(DatabaseConnection conn, RequestSource source, int groupServer) throws IOException, SQLException {
    return
        PackageHandler.canAccessPackage(conn, source, getPackageForGroupServer(conn, groupServer))
            && NetHostHandler.canAccessHost(conn, source, getServerForGroupServer(conn, groupServer));
  }

  public static void checkAccessGroupServer(DatabaseConnection conn, RequestSource source, String action, int groupServer) throws IOException, SQLException {
    if (!canAccessGroupServer(conn, source, groupServer)) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access linux_server_group: action='"
              + action
              + ", id="
              + groupServer;
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
      Gecos officeLocation,
      Gecos officePhone,
      Gecos homePhone,
      String type,
      PosixPath shell,
      boolean skipSecurityChecks
  ) throws IOException, SQLException {
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to add User named '" + User.MAIL + '\'');
    }

    // Make sure the shell is allowed for the type of account being added
    if (!UserType.isAllowedShell(type, shell)) {
      throw new SQLException("shell='" + shell + "' not allowed for type='" + type + '\'');
    }

    if (!skipSecurityChecks) {
      AccountUserHandler.checkAccessUser(conn, source, "addUser", user);
      if (AccountUserHandler.isUserDisabled(conn, user)) {
        throw new SQLException("Unable to add User, Username disabled: " + user);
      }
    }

    conn.update(
        "insert into linux.\"User\" values(?,?,?,?,?,?,?,now(),null)",
        user,
        name,
        officeLocation,
        officePhone,
        homePhone,
        type,
        shell
    );
    // Notify all clients of the update
    invalidateList.addTable(conn,
        Table.TableId.LINUX_ACCOUNTS,
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
    if (!skipSecurityChecks) {
      PackageHandler.checkAccessPackage(conn, source, "addGroup", packageName);
      if (PackageHandler.isPackageDisabled(conn, packageName)) {
        throw new SQLException("Unable to add Group, Package disabled: " + packageName);
      }
    }
    if (
        name.equals(Group.FTPONLY)
            || name.equals(Group.MAIL)
            || name.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to add Group: " + name);
    }

    conn.update("insert into linux.\"Group\" values(?,?,?)", name, packageName, type);

    // Notify all clients of the update
    invalidateList.addTable(conn,
        Table.TableId.LINUX_GROUPS,
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
    if (group.equals(Group.MAIL)) {
      throw new SQLException("Not allowed to add GroupUser for group '" + Group.MAIL + '\'');
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to add GroupUser for user '" + User.MAIL + '\'');
    }
    if (!skipSecurityChecks) {
      if (
          !group.equals(Group.FTPONLY)
              && !group.equals(Group.MAILONLY)
      ) {
        checkAccessGroup(conn, source, "addGroupUser", group);
      }
      checkAccessUser(conn, source, "addGroupUser", user);
      if (isUserDisabled(conn, user)) {
        throw new SQLException("Unable to add GroupUser, User disabled: " + user);
      }
    }
    if (group.equals(Group.FTPONLY)) {
      // Only allowed to have ftponly group when it is a ftponly account
      String type = getTypeForUser(conn, user);
      if (!type.equals(UserType.FTPONLY)) {
        throw new SQLException("Not allowed to add GroupUser for group '" + Group.FTPONLY + "' on non-ftp-only-type User named " + user);
      }
    }
    if (group.equals(Group.MAILONLY)) {
      // Only allowed to have mail group when it is a "mailonly" account
      String type = getTypeForUser(conn, user);
      if (!type.equals(UserType.EMAIL)) {
        throw new SQLException("Not allowed to add GroupUser for group '" + Group.MAILONLY + "' on non-email-type User named " + user);
      }
    }

    // Do not allow more than 31 groups per account
    int count = conn.queryInt("select count(*) from linux.\"GroupUser\" where \"user\"=?", user);
    if (count >= GroupUser.MAX_GROUPS) {
      throw new SQLException("Only " + GroupUser.MAX_GROUPS + " groups are allowed per user, username=" + user + " already has access to " + count + " groups");
    }

    int groupUser = conn.updateInt(
        "INSERT INTO linux.\"GroupUser\" VALUES (default,?,?,?,null) RETURNING id",
        group,
        user,
        isPrimary
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_GROUP_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to add UserServer for user '" + User.MAIL + '\'');
    }
    if (!skipSecurityChecks) {
      checkAccessUser(conn, source, "addUserServer", user);
      if (isUserDisabled(conn, user)) {
        throw new SQLException("Unable to add UserServer, User disabled: " + user);
      }
      NetHostHandler.checkAccessHost(conn, source, "addUserServer", linuxServer);
      AccountUserHandler.checkUserAccessHost(conn, source, "addUserServer", user, linuxServer);
    }

    // OperatingSystem settings
    int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    if (osv == -1) {
      throw new SQLException("Operating system version not known for server #" + linuxServer);
    }
    PosixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
    PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

    if (home.equals(UserServer.getDefaultHomeDirectory(user))) {
      // Make sure no conflicting /home/u/username account exists.
      String prefix = home + "/";
      List<String> conflicting = conn.queryStringList(
          "select distinct home from linux.\"UserServer\" where ao_server=? and substring(home from 1 for " + prefix.length() + ")=? order by home",
          linuxServer,
          prefix
      );
      if (!conflicting.isEmpty()) {
        throw new SQLException("Found conflicting home directories: " + conflicting);
      }
    } else if (home.equals(UserServer.getHashedHomeDirectory(user))) {
      // Make sure no conflicting /home/u account exists.
      String conflictHome = "/home/" + user.toString().charAt(0);
      if (
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
      if (
          !homeStr.startsWith(httpdSitesDir + "/")
              && !homeStr.startsWith(httpdSharedTomcatsDir + "/")
      ) {
        throw new SQLException("Invalid home directory: " + home);
      }

      final String slashWebapps = "/webapps";
      if (homeStr.startsWith(httpdSitesDir + "/")) {
        // May also be in /www/(sitename)/webapps
        String siteName = homeStr.substring(httpdSitesDir.toString().length() + 1);
        if (siteName.endsWith(slashWebapps)) {
          siteName = siteName.substring(0, siteName.length() - slashWebapps.length());
        }
        // May be in /www/(sitename)
        int httpdSite = WebHandler.getSite(conn, linuxServer, siteName);
        if (httpdSite != -1) {
          if (!skipSecurityChecks) {
            // Must be able to access an existing site
            WebHandler.checkAccessSite(conn, source, "addUserServer", httpdSite);
          }
        } else {
          // Must be a valid site name
          if (!Site.isValidSiteName(siteName)) {
            throw new SQLException("Invalid site name for www home directory: " + home);
          }
        }
      }

      if (homeStr.startsWith(httpdSharedTomcatsDir + "/")) {
        // May also be in /wwwgroup/(tomcatname)/webapps
        String tomcatName = homeStr.substring(httpdSharedTomcatsDir.toString().length() + 1);
        if (tomcatName.endsWith(slashWebapps)) {
          tomcatName = tomcatName.substring(0, tomcatName.length() - slashWebapps.length());
        }
        // May be in /wwwgroup/(tomcatname)
        int httpdSharedTomcat = WebHandler.getSharedTomcat(conn, linuxServer, tomcatName);
        if (httpdSharedTomcat != -1) {
          if (!skipSecurityChecks) {
            // Must be able to access an existing site
            WebHandler.checkAccessSharedTomcat(conn, source, "addUserServer", httpdSharedTomcat);
          }
        } else {
          // Must be a valid tomcat name
          if (!SharedTomcat.isValidSharedTomcatName(tomcatName)) {
            throw new SQLException("Invalid shared tomcat name for wwwgroup home directory: " + home);
          }
        }
      }
    }

    // The primary group for this user must exist on this server
    Group.Name primaryGroup = getPrimaryGroup(conn, user, osv);
    int primaryGroupServer = getGroupServer(conn, primaryGroup, linuxServer);
    if (primaryGroupServer < 0) {
      throw new SQLException("Unable to find primary Linux group '" + primaryGroup + "' on Server #" + linuxServer + " for Linux account '" + user + "'");
    }

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
        Table.TableId.LINUX_SERVER_ACCOUNTS,
        account,
        linuxServer,
        true
    );
    // If it is a email type, add the default attachment blocks
    if (!user.equals(User.EMAILMON) && isUserEmailType(conn, user)) {
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
          Table.TableId.EMAIL_ATTACHMENT_BLOCKS,
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
    if (
        group.equals(Group.FTPONLY)
            || group.equals(Group.MAIL)
            || group.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to add GroupServer for group '" + group + '\'');
    }
    Account.Name account = getAccountForGroup(conn, group);
    if (!skipSecurityChecks) {
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
        Table.TableId.LINUX_SERVER_GROUPS,
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
    return conn.queryObjectOptional(
        ObjectFactories.groupNameFactory,
        "select name from linux.\"GroupServer\" where ao_server=? and gid=?",
        linuxServer,
        gid
    ).orElse(null);
  }

  /**
   * Gets the username that exists on a server for the given uid
   * or {@code null} if the uid is not allocated to the server.
   */
  public static com.aoindustries.aoserv.client.linux.User.Name getUserByUid(DatabaseConnection conn, int linuxServer, int uid) throws SQLException {
    return conn.queryObjectOptional(
        ObjectFactories.linuxUserNameFactory,
        "select username from linux.\"UserServer\" where ao_server=? and uid=?",
        linuxServer,
        uid
    ).orElse(null);
  }

  /**
   * Determines if is a fixed GID type of system group.
   */
  private static boolean isFixedSystemGroup(int osv, Group.Name group, int gid) throws SQLException {
    if (osv == OperatingSystemVersion.CENTOS_7_X86_64) {
      return CENTOS_7_FIXED_SYSTEM_GROUPS.contains(new Tuple2<>(group, gid));
    } else if (osv == OperatingSystemVersion.ROCKY_9_X86_64) {
      return ROCKY_9_FIXED_SYSTEM_GROUPS.contains(new Tuple2<>(group, gid));
    } else {
      throw new SQLException("Unexpected operating system #" + osv);
    }
  }

  /**
   * Determines if is a dynamic GID type of system group.
   */
  private static boolean isDynamicSystemGroup(int osv, int gidMin, Group.Name group, int gid) throws SQLException {
    if (osv == OperatingSystemVersion.CENTOS_7_X86_64) {
      return gid >= CENTOS_7_SYS_GID_MIN
          && gid < gidMin
          && CENTOS_7_DYNAMIC_SYSTEM_GROUPS.contains(group);
    } else if (osv == OperatingSystemVersion.ROCKY_9_X86_64) {
      return gid >= ROCKY_9_SYS_GID_MIN
          && gid < gidMin
          && ROCKY_9_DYNAMIC_SYSTEM_GROUPS.contains(group);
    } else {
      throw new SQLException("Unexpected operating system #" + osv);
    }
  }

  /**
   * Determines if is a regular user group.
   */
  private static boolean isRegularUserGroup(int osv, int gidMin, int gidMax, Group.Name group, int gid) throws SQLException {
    if (osv == OperatingSystemVersion.CENTOS_7_X86_64) {
      return gid >= gidMin
          && gid <= gidMax
          && CENTOS_7_REGULAR_USER_GROUPS.contains(group);
    } else if (osv == OperatingSystemVersion.ROCKY_9_X86_64) {
      return gid >= gidMin
          && gid <= gidMax
          && ROCKY_9_REGULAR_USER_GROUPS.contains(group);
    } else {
      throw new SQLException("Unexpected operating system #" + osv);
    }
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
    com.aoindustries.aoserv.client.master.User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu == null) {
      throw new SQLException("Not a master user: " + source.getCurrentAdministrator());
    }
    NetHostHandler.checkAccessHost(conn, source, "addSystemGroup", linuxServer);
    // The group ID must be in the system group range
    if (gid < 0) {
      throw new SQLException("Invalid gid: " + gid);
    }
    int gidMin = LinuxServerHandler.getGidMin(conn, linuxServer);
    int gidMax = LinuxServerHandler.getGidMax(conn, linuxServer);
      // The group ID must not already exist on this server
      {
        Group.Name existing = getGroupByGid(conn, linuxServer, gid);
        if (existing != null) {
          throw new SQLException("Group #" + gid + " already exists on server #" + linuxServer + ": " + existing);
        }
      }
    // Must be one of the expected patterns for the servers operating system version
    int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    if (
        // Fixed group ids
        isFixedSystemGroup(osv, group, gid)
        // System groups in range 201 through gidMin - 1
        || isDynamicSystemGroup(osv, gidMin, group, gid)
        // Regular user groups in range gidMin through Group.GID_MAX
        || isRegularUserGroup(osv, gidMin, gidMax, group, gid)
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
          Table.TableId.LINUX_SERVER_GROUPS,
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
     * The set of allowed system user patterns for CentOS 7.
     */
    private static final Map<com.aoindustries.aoserv.client.linux.User.Name, SystemUser> CENTOS_7_SYSTEM_USERS = new HashMap<>();

    /**
     * The set of allowed system user patterns for Rocky 9.
     */
    private static final Map<com.aoindustries.aoserv.client.linux.User.Name, SystemUser> ROCKY_9_SYSTEM_USERS = new HashMap<>();

    private static void addSystemUser(
        Map<com.aoindustries.aoserv.client.linux.User.Name, SystemUser> systemUsers,
        com.aoindustries.aoserv.client.linux.User.Name user,
        int uid,
        Group.Name group,
        String fullName,
        String home,
        PosixPath shell,
        String sudo
    ) throws ValidationException {
      if (
          systemUsers.put(
              user,
              new SystemUser(
                  uid,
                  group,
                  InternUtils.intern(Gecos.valueOf(fullName)), null, null, null,
                  PosixPath.valueOf(home).intern(),
                  shell,
                  sudo
              )
          ) != null
      ) {
        throw new AssertionError("Duplicate username: " + user);
      }
    }

    static {
      try {
        try {
          // TODO: We should probably have a database table instead of this hard-coded list, same for system groups
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.ROOT,                              0, Group.ROOT,                 "root",                                                            "/root",                         Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.ROOT,                              0, Group.ROOT,                 "root",                                                            "/root",                         Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.BIN,                               1, Group.BIN,                  "bin",                                                             "/bin",                          Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.BIN,                               1, Group.BIN,                  "bin",                                                             "/bin",                          Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.DAEMON,                            2, Group.DAEMON,               "daemon",                                                          "/sbin",                         Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.DAEMON,                            2, Group.DAEMON,               "daemon",                                                          "/sbin",                         Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.ADM,                               3, Group.ADM,                  "adm",                                                             "/var/adm",                      Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.ADM,                               3, Group.ADM,                  "adm",                                                             "/var/adm",                      Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.LP,                                4, Group.LP,                   "lp",                                                              "/var/spool/lpd",                Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.LP,                                4, Group.LP,                   "lp",                                                              "/var/spool/lpd",                Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SYNC,                              5, Group.ROOT,                 "sync",                                                            "/sbin",                         Shell.SYNC,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SYNC,                              5, Group.ROOT,                 "sync",                                                            "/sbin",                         Shell.SYNC,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SHUTDOWN,                          6, Group.ROOT,                 "shutdown",                                                        "/sbin",                         Shell.SHUTDOWN, null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SHUTDOWN,                          6, Group.ROOT,                 "shutdown",                                                        "/sbin",                         Shell.SHUTDOWN, null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.HALT,                              7, Group.ROOT,                 "halt",                                                            "/sbin",                         Shell.HALT,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.HALT,                              7, Group.ROOT,                 "halt",                                                            "/sbin",                         Shell.HALT,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MAIL,                              8, Group.MAIL,                 "mail",                                                            "/var/spool/mail",               Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.MAIL,                              8, Group.MAIL,                 "mail",                                                            "/var/spool/mail",               Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.OPERATOR,                         11, Group.ROOT,                 "operator",                                                        "/root",                         Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.OPERATOR,                         11, Group.ROOT,                 "operator",                                                        "/root",                         Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.GAMES,                            12, Group.USERS,                "games",                                                           "/usr/games",                    Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.GAMES,                            12, Group.USERS,                "games",                                                           "/usr/games",                    Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.FTP,                              14, Group.FTP,                  "FTP User",                                                        "/var/ftp",                      Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.FTP,                              14, Group.FTP,                  "FTP User",                                                        "/var/ftp",                      Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.OPROFILE,                         16, Group.OPROFILE,             "Special user account to be used by OProfile",                     "/var/lib/oprofile",             Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.OPROFILE
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.NAMED,                            25, Group.NAMED,                "Named",                                                           "/var/named",                    Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.NAMED,                            25, Group.NAMED,                "Named",                                                           "/var/named",                    Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.POSTGRES,                         26, Group.POSTGRES,             "PostgreSQL Server",                                               "/var/lib/pgsql",                Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.POSTGRES,                         26, Group.POSTGRES,             "PostgreSQL Server",                                               "/var/lib/pgsql",                Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.RPCUSER,                          29, Group.RPCUSER,              "RPC Service User",                                                "/var/lib/nfs",                  Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.RPCUSER,                          29, Group.RPCUSER,              "RPC Service User",                                                "/var/lib/nfs",                  Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MYSQL,                            31, Group.MYSQL,                "MySQL server",                                                    "/var/lib/mysql",                Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.MYSQL,                            31, Group.MYSQL,                "MySQL server",                                                    "/var/lib/mysql",                Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.RPC,                              32, Group.RPC,                  "Rpcbind Daemon",                                                  "/var/lib/rpcbind",              Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.RPC,                              32, Group.RPC,                  "Rpcbind Daemon",                                                  "/var/lib/rpcbind",              Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MAILNULL,                         47, Group.MAILNULL,             null,                                                              "/var/spool/mqueue",             Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.MAILNULL
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.APACHE,                           48, Group.APACHE,               "Apache",                                                          "/usr/share/httpd",              Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.APACHE,                           48, Group.APACHE,               "Apache",                                                          "/usr/share/httpd",              Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SMMSP,                            51, Group.SMMSP,                null,                                                              "/var/spool/mqueue",             Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.SMMSP
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.TSS,                              59, Group.TSS,                  "Account used by the trousers package to sandbox the tcsd daemon", "/dev/null",                     Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.TSS,                              59, Group.TSS,                  "Account used for TPM access",                                     "/dev/null",                     Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.TCPDUMP,                          72, Group.TCPDUMP,              null,                                                              "/",                             Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.TCPDUMP,                          72, Group.TCPDUMP,              null,                                                              "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SSHD,                             74, Group.SSHD,                 "Privilege-separated SSH",                                         "/var/empty/sshd",               Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SSHD,                             74, Group.SSHD,                 "Privilege-separated SSH",                                         "/usr/share/empty.sshd",         Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.CYRUS,                            76, Group.MAIL,                 "Cyrus IMAP Server",                                               "/var/lib/imap",                 Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.CYRUS
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AWSTATS,                          78, Group.AWSTATS,              "AWStats Background Log Processing",                               "/var/opt/awstats",              Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.AWSTATS,                          78, Group.AWSTATS,              "AWStats Background Log Processing",                               "/var/opt/awstats",              Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.DBUS,                             81, Group.DBUS,                 "System message bus",                                              "/",                             Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.DBUS,                             81, Group.DBUS,                 "System message bus",                                              "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.BIRD,                             95, Group.BIRD,                 "BIRD Internet Routing Daemon",                                    "/var/opt/bird",                 Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.BIRD
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.NOBODY,                           99, Group.NOBODY,               "Nobody",                                                          "/",                             Shell.NOLOGIN,  null);
          // Renumbered to 65534 in Rocky 9:   User.NOBODY
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AVAHI_AUTOIPD,                   170, Group.AVAHI_AUTOIPD,        "Avahi IPv4LL Stack",                                              "/var/lib/avahi-autoipd",        Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.AVAHI_AUTOIPD
          // Not in CentOS 7:                  User.RTKIT
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.RTKIT,                           172, Group.RTKIT,                "RealtimeKit",                                                     "/proc",                         Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.DHCPD,                           177, Group.DHCPD,                "DHCP server",                                                     "/",                             Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.DHCPD,                           177, Group.DHCPD,                "DHCP server",                                                     "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SYSTEMD_NETWORK,                 192, Group.SYSTEMD_NETWORK,      "systemd Network Management",                                      "/",                             Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.SYSTEMD_NETWORK
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.NFSNOBODY,                     65534, Group.NFSNOBODY,            "Anonymous NFS User",                                              "/var/lib/nfs",                  Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.NOBODY,                        65534, Group.NOBODY,               "Kernel Overflow User",                                            "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AOSERV_JILTER,        ANY_SYSTEM_UID, Group.AOSERV_JILTER,        "AOServ Jilter",                                                   "/var/opt/aoserv-jilter",        Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.AOSERV_JILTER,        ANY_SYSTEM_UID, Group.AOSERV_JILTER,        "AOServ Jilter",                                                   "/var/opt/aoserv-jilter",        Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AOSERV_MASTER,        ANY_SYSTEM_UID, Group.AOSERV_MASTER,        "AOServ Master",                                                   "/var/opt/aoserv-master",        Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.AOSERV_MASTER,        ANY_SYSTEM_UID, Group.AOSERV_MASTER,        "AOServ Master",                                                   "/var/opt/aoserv-master",        Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.CHRONY,               ANY_SYSTEM_UID, Group.CHRONY,               null,                                                              "/var/lib/chrony",               Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.CHRONY,               ANY_SYSTEM_UID, Group.CHRONY,               null,                                                              "/var/lib/chrony",               Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.CLAMSCAN,             ANY_SYSTEM_UID, Group.CLAMSCAN,             "Clamav scanner user",                                             "/",                             Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.CLAMSCAN,             ANY_SYSTEM_UID, Group.CLAMSCAN,             "Clamav scanner user",                                             "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.CLAMUPDATE,           ANY_SYSTEM_UID, Group.CLAMUPDATE,           "Clamav database update user",                                     "/var/lib/clamav",               Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.CLAMUPDATE,           ANY_SYSTEM_UID, Group.CLAMUPDATE,           "Clamav database update user",                                     "/var/lib/clamav",               Shell.NOLOGIN,  null);
          // Not in CentOS 7:                  User.FLATPAK
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.FLATPAK,              ANY_SYSTEM_UID, Group.FLATPAK,              "User for flatpak system helper",                                  "/",                             Shell.NOLOGIN,  null);
          // Not in CentOS 7:                  User.GEOCLUE
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.GEOCLUE,              ANY_SYSTEM_UID, Group.GEOCLUE,              "User for geoclue",                                                "/var/lib/geoclue",              Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.JENKINS,              ANY_SYSTEM_UID, Group.JENKINS,              "Jenkins Automation Server",                                       "/var/lib/jenkins",              Shell.FALSE,    null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.JENKINS,              ANY_SYSTEM_UID, Group.JENKINS,              "Jenkins Automation Server",                                       "/var/lib/jenkins",              Shell.FALSE,    null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MEMCACHED,            ANY_SYSTEM_UID, Group.MEMCACHED,            "Memcached daemon",                                                "/run/memcached",                Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.MEMCACHED,            ANY_SYSTEM_UID, Group.MEMCACHED,            "Memcached daemon",                                                "/run/memcached",                Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.NGINX,                ANY_SYSTEM_UID, Group.NGINX,                "nginx user",                                                      "/var/cache/nginx",              Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.NGINX,                ANY_SYSTEM_UID, Group.NGINX,                "nginx user",                                                      "/var/cache/nginx",              Shell.NOLOGIN,  null);
          // Not in CentOS 7:                  User.PIPEWIRE
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.PIPEWIRE,             ANY_SYSTEM_UID, Group.PIPEWIRE,             "PipeWire System Daemon",                                          "/var/run/pipewire",             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.POLKITD,              ANY_SYSTEM_UID, Group.POLKITD,              "User for polkitd",                                                "/",                             Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.POLKITD,              ANY_SYSTEM_UID, Group.POLKITD,              "User for polkitd",                                                "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.REDIS,                ANY_SYSTEM_UID, Group.REDIS,                "Redis Database Server",                                           "/var/lib/redis",                Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.REDIS
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SASLAUTH,             ANY_SYSTEM_UID, Group.SASLAUTH,             "Saslauthd user",                                                  "/run/saslauthd",                Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SASLAUTH,             ANY_SYSTEM_UID, Group.SASLAUTH,             "Saslauthd user",                                                  "/run/saslauthd",                Shell.NOLOGIN,  null);
          // Not in CentOS 7:                  User.SSSD
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SSSD,                 ANY_SYSTEM_UID, Group.SSSD,                 "User for sssd",                                                   "/",                             Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SYSTEMD_BUS_PROXY,    ANY_SYSTEM_UID, Group.SYSTEMD_BUS_PROXY,    "systemd Bus Proxy",                                               "/",                             Shell.NOLOGIN,  null);
          // Not in Rocky 9:                   User.SYSTEMD_BUS_PROXY
          // Not in CentOS 7:                  User.SYSTEMD_COREDUMP
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SYSTEMD_COREDUMP,     ANY_SYSTEM_UID, Group.SYSTEMD_COREDUMP,     "systemd Core Dumper",                                             "/",                             Shell.NOLOGIN,  null);
          // Not in CentOS 7:                  User.SYSTEMD_OOM
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SYSTEMD_OOM,          ANY_SYSTEM_UID, Group.SYSTEMD_OOM,          "systemd Userspace OOM Killer",                                    "/",                             Shell.USR_SBIN_NOLOGIN, null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.UNBOUND,              ANY_SYSTEM_UID, Group.UNBOUND,              "Unbound DNS resolver",                                            "/etc/unbound",                  Shell.NOLOGIN,  null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.UNBOUND,              ANY_SYSTEM_UID, Group.UNBOUND,              "Unbound DNS resolver",                                            "/etc/unbound",                  Shell.NOLOGIN,  null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AOADMIN,              ANY_USER_UID,   Group.AOADMIN,              "AO Industries Administrator",                                     "/home/aoadmin",                 Shell.BASH,     AOADMIN_SUDO);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.AOADMIN,              ANY_USER_UID,   Group.AOADMIN,              "AO Industries Administrator",                                     "/home/aoadmin",                 Shell.BASH,     AOADMIN_SUDO);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.AOSERV_XEN_MIGRATION, ANY_SYSTEM_UID, Group.AOSERV_XEN_MIGRATION, "AOServ Xen Migration",                                            "/var/opt/aoserv-xen-migration", Shell.BASH,     AOSERV_XEN_MIGRATION_SUDO);
          // Not in Rocky 9:                   User.AOSERV_XEN_MIGRATION
          // AOServ Schema:
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.ACCOUNTING,           ANY_USER_UID,   Group.ACCOUNTING,           "masterdb access",                                                 "/home/accounting",              Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.ACCOUNTING,           ANY_USER_UID,   Group.ACCOUNTING,           "masterdb access",                                                 "/home/accounting",              Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.BILLING,              ANY_USER_UID,   Group.BILLING,              "masterdb access",                                                 "/home/billing",                 Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.BILLING,              ANY_USER_UID,   Group.BILLING,              "masterdb access",                                                 "/home/billing",                 Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.DISTRIBUTION,         ANY_USER_UID,   Group.DISTRIBUTION,         "masterdb access",                                                 "/home/distribution",            Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.DISTRIBUTION,         ANY_USER_UID,   Group.DISTRIBUTION,         "masterdb access",                                                 "/home/distribution",            Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.INFRASTRUCTURE,       ANY_USER_UID,   Group.INFRASTRUCTURE,       "masterdb access",                                                 "/home/infrastructure",          Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.INFRASTRUCTURE,       ANY_USER_UID,   Group.INFRASTRUCTURE,       "masterdb access",                                                 "/home/infrastructure",          Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MANAGEMENT,           ANY_USER_UID,   Group.MANAGEMENT,           "masterdb access",                                                 "/home/management",              Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.MANAGEMENT,           ANY_USER_UID,   Group.MANAGEMENT,           "masterdb access",                                                 "/home/management",              Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.MONITORING,           ANY_USER_UID,   Group.MONITORING,           "masterdb access",                                                 "/home/monitoring",              Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.MONITORING,           ANY_USER_UID,   Group.MONITORING,           "masterdb access",                                                 "/home/monitoring",              Shell.BASH,     null);
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.RESELLER,             ANY_USER_UID,   Group.RESELLER,             "masterdb access",                                                 "/home/reseller",                Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.RESELLER,             ANY_USER_UID,   Group.RESELLER,             "masterdb access",                                                 "/home/reseller",                Shell.BASH,     null);
          // Amazon EC2 cloud-init
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.CENTOS,               ANY_USER_UID,   Group.CENTOS,               "Cloud User",                                                      "/home/centos",                  Shell.BASH,     CENTOS_SUDO);
          // Not in Rocky 9:                   User.CENTOS
          // SonarQube
          addSystemUser(CENTOS_7_SYSTEM_USERS, User.SONARQUBE,            ANY_USER_UID,   Group.SONARQUBE,            "SonarQube",                                                       "/home/sonarqube",               Shell.BASH,     null);
          addSystemUser(ROCKY_9_SYSTEM_USERS,  User.SONARQUBE,            ANY_USER_UID,   Group.SONARQUBE,            "SonarQube",                                                       "/home/sonarqube",               Shell.BASH,     null);
        } catch (ValidationException e) {
          throw new AssertionError("These hard-coded values are valid", e);
        }
      } catch (Throwable t) {
        t.printStackTrace(System.err);
        throw Throwables.wrap(t, ExceptionInInitializerError.class, ExceptionInInitializerError::new);
      }
    }

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

  private static SystemUser getSystemUser(
      int osv,
      com.aoindustries.aoserv.client.linux.User.Name user
  ) throws SQLException {
    if (osv == OperatingSystemVersion.CENTOS_7_X86_64) {
      return SystemUser.CENTOS_7_SYSTEM_USERS.get(user);
    } else if (osv == OperatingSystemVersion.ROCKY_9_X86_64) {
      return SystemUser.ROCKY_9_SYSTEM_USERS.get(user);
    } else {
      throw new SQLException("Unexpected operating system #" + osv);
    }
  }

  private static int getSysUidMin(int osv) throws SQLException {
    if (osv == OperatingSystemVersion.CENTOS_7_X86_64) {
      return CENTOS_7_SYS_UID_MIN;
    } else if (osv == OperatingSystemVersion.ROCKY_9_X86_64) {
      return ROCKY_9_SYS_UID_MIN;
    } else {
      throw new SQLException("Unexpected operating system #" + osv);
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
    com.aoindustries.aoserv.client.master.User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu == null) {
      throw new SQLException("Not a master user: " + source.getCurrentAdministrator());
    }
    NetHostHandler.checkAccessHost(conn, source, "addSystemUser", linuxServer);
    // The user ID must be in the system user range
    if (uid < 0) {
      throw new SQLException("Invalid uid: " + uid);
    }
    int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);
    int uidMax = LinuxServerHandler.getUidMax(conn, linuxServer);
      // The user ID must not already exist on this server
      {
        com.aoindustries.aoserv.client.linux.User.Name existing = getUserByUid(conn, linuxServer, uid);
        if (existing != null) {
          throw new SQLException("User #" + uid + " already exists on server #" + linuxServer + ": " + existing);
        }
      }
    // Get the group name for the requested gid
    Group.Name group = getGroupByGid(conn, linuxServer, gid);
    if (group == null) {
      throw new SQLException("Group #" + gid + " does not exist on server #" + linuxServer);
    }
    // Must be one of the expected patterns for the servers operating system version
    int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    SystemUser systemUser = getSystemUser(osv, user);
    if (systemUser != null) {
      if (systemUser.uid == SystemUser.ANY_SYSTEM_UID) {
        // System users in range 201 through uidMin - 1
        if (uid < getSysUidMin(osv) || uid >= uidMin) {
          throw new SQLException("Invalid system uid: " + uid);
        }
      } else if (systemUser.uid == SystemUser.ANY_USER_UID) {
        // Regular users in range uidMin through User.UID_MAX
        if (uid < uidMin || uid > uidMax) {
          throw new SQLException("Invalid regular user uid: " + uid);
        }
      } else {
        // UID must match exactly
        if (uid != systemUser.uid) {
          throw new SQLException("Unexpected system uid: " + uid + " != " + systemUser.uid);
        }
      }
      // Check other fields match
      if (!Objects.equals(group,          systemUser.group)) {
        throw new SQLException("Unexpected system group: "          + group          + " != " + systemUser.group);
      }
      if (!Objects.equals(fullName,       systemUser.fullName)) {
        throw new SQLException("Unexpected system fullName: "       + fullName       + " != " + systemUser.fullName);
      }
      if (!Objects.equals(officeLocation, systemUser.officeLocation)) {
        throw new SQLException("Unexpected system officeLocation: " + officeLocation + " != " + systemUser.officeLocation);
      }
      if (!Objects.equals(officePhone,    systemUser.officePhone)) {
        throw new SQLException("Unexpected system officePhone: "    + officePhone    + " != " + systemUser.officePhone);
      }
      if (!Objects.equals(homePhone,      systemUser.homePhone)) {
        throw new SQLException("Unexpected system homePhone: "      + homePhone      + " != " + systemUser.homePhone);
      }
      if (!Objects.equals(home,           systemUser.home)) {
        throw new SQLException("Unexpected system home: "           + home           + " != " + systemUser.home);
      }
      if (!Objects.equals(shell,          systemUser.shell)) {
        throw new SQLException("Unexpected system shell: "          + shell          + " != " + systemUser.shell);
      }
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
              + "  " + UserServer.DEFAULT_SPAM_ASSASSIN_REQUIRED_SCORE + ",\n"
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
          Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to copy User named '" + User.MAIL + '\'');
    }
    int from_server = getServerForUserServer(conn, from_userServer);
    int to_userServer = conn.queryInt(
        "select id from linux.\"UserServer\" where username=? and ao_server=?",
        user,
        to_server
    );
    checkAccessUserServer(conn, source, "copyHomeDirectory", to_userServer);
    String type = getTypeForUser(conn, user);
    if (
        !type.equals(UserType.USER)
            && !type.equals(UserType.EMAIL)
            && !type.equals(UserType.FTPONLY)
    ) {
      throw new SQLException("Not allowed to copy LinuxAccounts of type '" + type + "', username=" + user);
    }

    AoservDaemonConnector fromDaemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
    AoservDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
    conn.close(); // Don't hold database connection while connecting to the daemon
    long byteCount = fromDaemonConnector.copyHomeDirectory(user, toDaemonConnector);
    return byteCount;
  }

  /**
   * Copies a password from one linux account to another.
   */
  public static void copyUserServerPassword(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int from_userServer,
      int to_userServer
  ) throws IOException, SQLException {
    checkAccessUserServer(conn, source, "copyLinuxServerAccountPassword", from_userServer);
    if (isUserServerDisabled(conn, from_userServer)) {
      throw new SQLException("Unable to copy UserServer password, from account disabled: " + from_userServer);
    }
    com.aoindustries.aoserv.client.linux.User.Name from_user = getUserForUserServer(conn, from_userServer);
    if (from_user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to copy the password from User named '" + User.MAIL + '\'');
    }
    checkAccessUserServer(conn, source, "copyLinuxServerAccountPassword", to_userServer);
    if (isUserServerDisabled(conn, to_userServer)) {
      throw new SQLException("Unable to copy UserServer password, to account disabled: " + to_userServer);
    }
    com.aoindustries.aoserv.client.linux.User.Name to_user = getUserForUserServer(conn, to_userServer);
    if (to_user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to copy the password to User named '" + User.MAIL + '\'');
    }

    int from_server = getServerForUserServer(conn, from_userServer);
    int to_server = getServerForUserServer(conn, to_userServer);

    String from_type = getTypeForUser(conn, from_user);
    if (
        !from_type.equals(UserType.APPLICATION)
            && !from_type.equals(UserType.USER)
            && !from_type.equals(UserType.EMAIL)
            && !from_type.equals(UserType.FTPONLY)
    ) {
      throw new SQLException("Not allowed to copy passwords from LinuxAccounts of type '" + from_type + "', username=" + from_user);
    }

    String to_type = getTypeForUser(conn, to_user);
    if (
        !to_type.equals(UserType.APPLICATION)
            && !to_type.equals(UserType.USER)
            && !to_type.equals(UserType.EMAIL)
            && !to_type.equals(UserType.FTPONLY)
    ) {
      throw new SQLException("Not allowed to copy passwords to LinuxAccounts of type '" + to_type + "', username=" + to_user);
    }

    AoservDaemonConnector fromDemonConnector = DaemonHandler.getDaemonConnector(conn, from_server);
    AoservDaemonConnector toDaemonConnector = DaemonHandler.getDaemonConnector(conn, to_server);
    conn.close(); // Don't hold database connection while connecting to the daemon
    Tuple2<String, Integer> encPassword = fromDemonConnector.getEncryptedLinuxAccountPassword(from_user);
    toDaemonConnector.setEncryptedLinuxAccountPassword(to_user, encPassword.getElement1(), encPassword.getElement2());

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
    if (isUserDisabled(conn, user)) {
      throw new SQLException("linux.User is already disabled: " + user);
    }
    IntList lsas = getUserServersForUser(conn, user);
    for (int c = 0; c < lsas.size(); c++) {
      int lsa = lsas.getInt(c);
      if (!isUserServerDisabled(conn, lsa)) {
        throw new SQLException("Cannot disable User '" + user + "': UserServer not disabled: " + lsa);
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
        Table.TableId.LINUX_ACCOUNTS,
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
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("linux.UserServer is already disabled: " + userServer);
    }

    int linuxServer = getServerForUserServer(conn, userServer);
    int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);

    // The UID must be a user UID
    int uid = getUidForUserServer(conn, userServer);
    if (uid < uidMin) {
      throw new SQLException("Not allowed to disable a system UserServer: id=" + userServer + ", uid=" + uid);
    }

    IntList crs = CvsHandler.getCvsRepositoriesForLinuxUserServer(conn, userServer);
    for (int c = 0; c < crs.size(); c++) {
      int cr = crs.getInt(c);
      if (!CvsHandler.isCvsRepositoryDisabled(conn, cr)) {
        throw new SQLException("Cannot disable UserServer #" + userServer + ": CvsRepository not disabled: " + cr);
      }
    }
    IntList hsts = WebHandler.getSharedTomcatsForLinuxUserServer(conn, userServer);
    for (int c = 0; c < hsts.size(); c++) {
      int hst = hsts.getInt(c);
      if (!WebHandler.isSharedTomcatDisabled(conn, hst)) {
        throw new SQLException("Cannot disable UserServer #" + userServer + ": SharedTomcat not disabled: " + hst);
      }
    }
    IntList hss = WebHandler.getSitesForLinuxUserServer(conn, userServer);
    for (int c = 0; c < hss.size(); c++) {
      int hs = hss.getInt(c);
      if (!WebHandler.isSiteDisabled(conn, hs)) {
        throw new SQLException("Cannot disable UserServer #" + userServer + ": Site not disabled: " + hs);
      }
    }
    IntList els = EmailHandler.getListsForLinuxUserServer(conn, userServer);
    for (int c = 0; c < els.size(); c++) {
      int el = els.getInt(c);
      if (!EmailHandler.isListDisabled(conn, el)) {
        throw new SQLException("Cannot disable UserServer #" + userServer + ": List not disabled: " + el);
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
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    int disableLog = getDisableLogForUser(conn, user);
    if (disableLog == -1) {
      throw new SQLException("linux.User is already enabled: " + user);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableUser", disableLog, true);
    if (AccountUserHandler.isUserDisabled(conn, user)) {
      throw new SQLException("Unable to enable User '" + user + "', Username not enabled: " + user);
    }

    conn.update(
        "update linux.\"User\" set disable_log=null where username=?",
        user
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_ACCOUNTS,
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
    int disableLog = getDisableLogForUserServer(conn, userServer);
    if (disableLog == -1) {
      throw new SQLException("linux.UserServer is already enabled: " + userServer);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableUserServer", disableLog, true);
    com.aoindustries.aoserv.client.linux.User.Name la = getUserForUserServer(conn, userServer);
    if (isUserDisabled(conn, la)) {
      throw new SQLException("Unable to enable UserServer #" + userServer + ", User not enabled: " + la);
    }

    conn.update(
        "update linux.\"UserServer\" set disable_log=null where id=?",
        userServer
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (name.equals(User.MAIL)) {
      throw new SQLException("Not allowed to get the autoresponder content for User named '" + User.MAIL + '\'');
    }
    PosixPath path = conn.queryObjectNullable(
        ObjectFactories.posixPathFactory,
        "select autoresponder_path from linux.\"UserServer\" where id=?",
        userServer
    );
    String content;
    if (path == null) {
      content = "";
    } else {
      int linuxServer = getServerForUserServer(conn, userServer);
      AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to get the cron table for User named '" + User.MAIL + '\'');
    }
    String type = getTypeForUser(conn, user);
    if (!type.equals(UserType.USER)) {
      throw new SQLException("Not allowed to get the cron table for LinuxAccounts of type '" + type + "', username=" + user);
    }
    int linuxServer = getServerForUserServer(conn, userServer);

    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.getCronTable(user);
  }

  public static int getDisableLogForUser(DatabaseConnection conn, com.aoindustries.aoserv.client.linux.User.Name user) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from linux.\"User\" where username=?", user);
  }

  public static int getDisableLogForUserServer(DatabaseConnection conn, int userServer) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from linux.\"UserServer\" where id=?", userServer);
  }

  public static void invalidateTable(Table.TableId tableId) {
    if (tableId == Table.TableId.LINUX_ACCOUNTS) {
      synchronized (LinuxAccountHandler.class) {
        disabledUsers.clear();
      }
    } else if (tableId == Table.TableId.LINUX_SERVER_ACCOUNTS) {
      synchronized (LinuxAccountHandler.class) {
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
    synchronized (LinuxAccountHandler.class) {
      Boolean o = disabledUsers.get(user);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForUser(conn, user) != -1;
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
    synchronized (LinuxAccountHandler.class) {
      Integer i = userServer;
      Boolean o = disabledUserServers.get(i);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForUserServer(conn, userServer) != -1;
      disabledUserServers.put(i, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isLinuxGroupAvailable(DatabaseConnection conn, Group.Name name) throws IOException, SQLException {
    return conn.queryBoolean("select (select name from linux.\"Group\" where name=?) is null", name);
  }

  public static int getGroupServer(DatabaseConnection conn, Group.Name group, int linuxServer) throws IOException, SQLException {
    int groupServer = conn.queryInt("select coalesce((select id from linux.\"GroupServer\" where name=? and ao_server=?), -1)", group, linuxServer);
    if (groupServer == -1) {
      throw new SQLException("Unable to find GroupServer " + group + " on " + linuxServer);
    }
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to check if a password is set for UserServer '" + User.MAIL + '\'');
    }

    int linuxServer = getServerForUserServer(conn, userServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
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
    if (DaemonHandler.isDaemonAvailable(linuxServer)) {
      try {
        AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
        conn.close(); // Don't hold database connection while connecting to the daemon
        return daemonConnector.isProcmailManual(userServer) ? AoservProtocol.TRUE : AoservProtocol.FALSE;
      } catch (IOException err) {
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to remove User with username '" + User.MAIL + '\'');
    }

    // Detach the linux account from its autoresponder address
    IntList linuxServers = getServersForUser(conn, user);
    for (int c = 0; c < linuxServers.size(); c++) {
      int linuxServer = linuxServers.getInt(c);
      conn.update("update linux.\"UserServer\" set autoresponder_from=null where username=? and ao_server=?", user, linuxServer);
    }
    // Delete any FTP guest user info attached to this account
    boolean ftpModified = conn.update("delete from ftp.\"GuestUser\" where username=?", user) > 0;
    // Delete the account from all servers
    // Get the values for later use
    for (int c = 0; c < linuxServers.size(); c++) {
      int linuxServer = linuxServers.getInt(c);
      int userServer = conn.queryInt("select id from linux.\"UserServer\" where username=? and ao_server=?", user, linuxServer);
      removeUserServer(conn, invalidateList, userServer);
    }
    // Delete the group relations for this account
    boolean groupAccountModified = conn.update("delete from linux.\"GroupUser\" where \"user\"=?", user) > 0;
    // Delete from the database
    conn.update("delete from linux.\"User\" where username=?", user);

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);

    if (ftpModified) {
      invalidateList.addTable(conn, Table.TableId.FTP_GUEST_USERS, account, linuxServers, false);
    }
    if (groupAccountModified) {
      invalidateList.addTable(conn, Table.TableId.LINUX_GROUP_ACCOUNTS, account, linuxServers, false);
    }
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
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
    if (
        group.equals(Group.FTPONLY)
            || group.equals(Group.MAIL)
            || group.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to remove Group named '" + group + "'");
    }

    // Must not be the primary group for any User
    int primaryCount = conn.queryInt("select count(*) from linux.\"GroupUser\" where \"group\"=? and \"isPrimary\"", group);
    if (primaryCount > 0) {
      throw new SQLException("linux_group.name=" + group + " is the primary group for " + primaryCount + " Linux " + (primaryCount == 1 ? "account" : "accounts"));
    }
    // Get the values for later use
    final Account.Name account = getAccountForGroup(conn, group);
    IntList linuxServers = getServersForGroup(conn, group);
    for (int c = 0; c < linuxServers.size(); c++) {
      int linuxServer = linuxServers.getInt(c);
      conn.update("delete from linux.\"GroupServer\" where name=? and ao_server=?", group, linuxServer);
    }
    // Delete the group relations for this group
    boolean groupAccountsModified = conn.queryInt("select count(*) from linux.\"GroupUser\" where \"group\"=? limit 1", group) > 0;
    if (groupAccountsModified) {
      conn.update("delete from linux.\"GroupUser\" where \"group\"=?", group);
    }
    // Delete from the database
    conn.update("delete from linux.\"Group\" where name=?", group);

    // Notify all clients of the update
    if (linuxServers.size() > 0) {
      invalidateList.addTable(conn, Table.TableId.LINUX_SERVER_GROUPS, account, linuxServers, false);
    }
    if (groupAccountsModified) {
      invalidateList.addTable(conn, Table.TableId.LINUX_GROUP_ACCOUNTS, account, linuxServers, false);
    }
    invalidateList.addTable(conn, Table.TableId.LINUX_GROUPS, account, linuxServers, false);
  }

  public static void removeGroupUser(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int groupUser
  ) throws IOException, SQLException {
    checkAccessGroupUser(conn, source, "removeGroupUser", groupUser);

    // Must not be a primary group
    boolean isPrimary = conn.queryBoolean("select \"isPrimary\" from linux.\"GroupUser\" where id=?", groupUser);
    if (isPrimary) {
      throw new SQLException("linux.GroupUser.id=" + groupUser + " is a primary group");
    }

    // Must be needingful not by SharedTomcatSite to be tying to SharedTomcat please
    int useCount = conn.queryInt(
        "select count(*) from linux.\"GroupUser\" lga, "
            + "linux.\"UserServer\" lsa, "
            + "\"web.tomcat\".\"SharedTomcat\" hst, "
            + "\"web.tomcat\".\"SharedTomcatSite\" htss, "
            + "web.\"Site\" hs "
            + "where lga.\"user\" = lsa.username and "
            + "lsa.id             = hst.linux_server_account and "
            + "htss.tomcat_site   = hs.id and "
            + "lga.\"group\"      = hs.linux_group and "
            + "hst.id             = htss.httpd_shared_tomcat and "
            + "lga.id = ?",
        groupUser
    );
    if (useCount == 0) {
      useCount = conn.queryInt(
          "select count(*) from linux.\"GroupUser\" lga, "
              + "linux.\"GroupServer\" lsg, "
              + "\"web.tomcat\".\"SharedTomcat\" hst, "
              + "\"web.tomcat\".\"SharedTomcatSite\" htss, "
              + "web.\"Site\" hs "
              + "where lga.\"group\" = lsg.name and "
              + "lsg.id              = hst.linux_server_group and "
              + "htss.tomcat_site    = hs.id and "
              + "lga.\"user\"        = hs.linux_account and "
              + "hst.id              = htss.httpd_shared_tomcat and "
              + "lga.id = ?",
          groupUser
      );
    }
    if (useCount > 0) {
      throw new SQLException("linux_group_account(" + groupUser + ") has been used by " + useCount + " web.tomcat.SharedTomcatSite.");
    }

    // Get the values for later use
    List<Account.Name> accounts = getAccountsForGroupUser(conn, groupUser);
    IntList linuxServers = getServersForGroupUser(conn, groupUser);
    // Delete the group relations for this group
    conn.update("delete from linux.\"GroupUser\" where id=?", groupUser);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_GROUP_ACCOUNTS, accounts, linuxServers, false);
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
    if (groupUser != -1) {
      // Get the values for later use
      List<Account.Name> accounts = getAccountsForGroupUser(conn, groupUser);
      IntList linuxServers = getServersForGroupUser(conn, groupUser);
      conn.update("delete from linux.\"GroupUser\" where id=?", groupUser);

      // Notify all clients of the update
      invalidateList.addTable(conn, Table.TableId.LINUX_GROUP_ACCOUNTS, accounts, linuxServers, false);
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to remove UserServer for user '" + User.MAIL + '\'');
    }

    int linuxServer = getServerForUserServer(conn, userServer);
    int uidMin = LinuxServerHandler.getUidMin(conn, linuxServer);

    // The UID must be a user UID
    int uid = getUidForUserServer(conn, userServer);
    if (uid < uidMin) {
      throw new SQLException("Not allowed to remove a system UserServer: id=" + userServer + ", uid=" + uid);
    }

    // Must not contain a CVS repository
    String home = conn.queryString("select home from linux.\"UserServer\" where id=?", userServer);
    int count = conn.queryInt(
        "select\n"
            + "  count(*)\n"
            + "from\n"
            + "  scm.\"CvsRepository\" cr\n"
            + "where\n"
            + "  linux_server_account=?\n"
            + "  and (\n"
            + "    path=?\n"
            + "    or substring(path from 1 for " + (home.length() + 1) + ")=?\n"
            + "  )",
        userServer,
        home,
        home + '/'
    );
    if (count > 0) {
      throw new SQLException("Home directory on " + linuxServer + " contains " + count + " CVS " + (count == 1 ? "repository" : "repositories") + ": " + home);
    }

    // Delete the email configurations that depend on this account
    final IntList addresses = conn.queryIntList("select email_address from email.\"InboxAddress\" where linux_server_account=?", userServer);
    final int size = addresses.size();
    final boolean addressesModified = size > 0;
    for (int c = 0; c < size; c++) {
      int address = addresses.getInt(c);
      conn.update("delete from email.\"InboxAddress\" where email_address=?", address);
      if (!EmailHandler.isAddressUsed(conn, address)) {
        conn.update("delete from email.\"Address\" where id=?", address);
      }
    }

    Account.Name account = getAccountForUserServer(conn, userServer);

    // Delete the attachment blocks
    if (conn.update("delete from email.\"AttachmentBlock\" where linux_server_account=?", userServer) > 0) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_ATTACHMENT_BLOCKS, account, linuxServer, false);
    }

    // Delete the account from the server
    conn.update("delete from linux.\"UserServer\" where id=?", userServer);
    invalidateList.addTable(conn, Table.TableId.LINUX_SERVER_ACCOUNTS, account, linuxServer, true);

    // Notify all clients of the update
    if (addressesModified) {
      invalidateList.addTable(conn, Table.TableId.LINUX_ACC_ADDRESSES, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
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
    if (
        group.equals(Group.FTPONLY)
            || group.equals(Group.MAIL)
            || group.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to remove GroupServer for group '" + group + "'");
    }

    // Get the server this group is on
    Account.Name account = getAccountForGroupServer(conn, groupServer);
    int linuxServer = getServerForGroupServer(conn, groupServer);
    // Must not be the primary group for any UserServer on the same server
    int primaryCount = conn.queryInt(
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

    if (primaryCount > 0) {
      throw new SQLException("linux_server_group.id=" + groupServer + " is the primary group for " + primaryCount
          + " Linux server " + (primaryCount == 1 ? "account" : "accounts") + " on " + linuxServer);
    }
    // Delete from the database
    conn.update("delete from linux.\"GroupServer\" where id=?", groupServer);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_SERVER_GROUPS, account, linuxServer, true);
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
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set autoresponder, UserServer disabled: " + userServer);
    }
    com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set autoresponder for user '" + User.MAIL + '\'');
    }
    String type = getTypeForUser(conn, user);
    if (
        !type.equals(UserType.EMAIL)
            && !type.equals(UserType.USER)
    ) {
      throw new SQLException("Not allowed to set autoresponder for this type of account: " + type);
    }

    // The from must be on this account
    if (from != -1) {
      int from_userServer = conn.queryInt("select linux_server_account from email.\"InboxAddress\" where id=?", from);
      if (from_userServer != userServer) {
        throw new SQLException("((linux_acc_address.id=" + from + ").linux_server_account=" + from_userServer + ") != ((linux_server_account.id=" + userServer + ").username=" + user + ")");
      }
    }

    final Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    final int linuxServer = getServerForUserServer(conn, userServer);
    final PosixPath path;
    if (content == null && !enabled) {
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
    if (!enabled) {
      uid = -1;
      gid = -1;
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
        if (from == -1) {
          pstmt.setNull(1, Types.INTEGER);
        } else {
          pstmt.setInt(1, from);
        }
        pstmt.setString(2, subject);
        pstmt.setString(3, Objects.toString(path, null));
        pstmt.setBoolean(4, enabled);
        pstmt.setInt(5, userServer);
        pstmt.executeUpdate();
      } catch (Error | RuntimeException | SQLException e) {
        ErrorPrinter.addSql(e, pstmt);
        throw e;
      }
    }

    // Store the content on the server
    if (path != null) {
      AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
      conn.commit();
      conn.close(); // Don't hold database connection while connecting to the daemon
      daemonConnector.setAutoresponderContent(path, content == null ? "" : content, uid, gid);
    }

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_SERVER_ACCOUNTS, account, linuxServer, false);
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
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set cron table, UserServer disabled: " + userServer);
    }
    com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the cron table for User named '" + User.MAIL + '\'');
    }
    String type = getTypeForUser(conn, user);
    if (
        !type.equals(UserType.USER)
    ) {
      throw new SQLException("Not allowed to set the cron table for LinuxAccounts of type '" + type + "', username=" + user);
    }
    int linuxServer = getServerForUserServer(conn, userServer);

    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
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
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set home phone number, User disabled: " + user);
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set home phone number for user '" + User.MAIL + '\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    IntList linuxServers = getServersForUser(conn, user);

    conn.update("update linux.\"User\" set home_phone=? where username=?", phone, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
  }

  public static void setUserFullName(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Gecos name
  ) throws IOException, SQLException {
    checkAccessUser(conn, source, "setUserFullName", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set full name, User disabled: " + user);
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set LinuxAccountName for user '" + User.MAIL + '\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    IntList linuxServers = getServersForUser(conn, user);

    conn.update("update linux.\"User\" set name=? where username=?", name, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
  }

  public static void setUserOfficeLocation(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Gecos location
  ) throws IOException, SQLException {
    checkAccessUser(conn, source, "setUserOfficeLocation", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set office location, User disabled: " + user);
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set office location for user '" + User.MAIL + '\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    IntList linuxServers = getServersForUser(conn, user);

    conn.update("update linux.\"User\" set office_location=? where username=?", location, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
  }

  public static void setUserOfficePhone(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Gecos phone
  ) throws IOException, SQLException {
    checkAccessUser(conn, source, "setUserOfficePhone", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set office phone number, User disabled: " + user);
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set office phone number for user '" + User.MAIL + '\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    IntList linuxServers = getServersForUser(conn, user);

    conn.update("update linux.\"User\" set office_phone=? where username=?", phone, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
  }

  public static void setUserShell(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      com.aoindustries.aoserv.client.linux.User.Name user,
      PosixPath shell
  ) throws IOException, SQLException {
    checkAccessUser(conn, source, "setUserShell", user);
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set shell, User disabled: " + user);
    }
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set shell for account named '" + User.MAIL + '\'');
    }
    String type = getTypeForUser(conn, user);
    if (!UserType.isAllowedShell(type, shell)) {
      throw new SQLException("Shell '" + shell + "' not allowed for Linux accounts with the type '" + type + '\'');
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    IntList linuxServers = getServersForUser(conn, user);

    conn.update("update linux.\"User\" set shell=? where username=?", shell, user);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACCOUNTS, account, linuxServers, false);
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
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set UserServer password, account disabled: " + userServer);
    }

    com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set password for UserServer named '" + User.MAIL + "': " + userServer);
    }
    String type = conn.queryString("select type from linux.\"User\" where username=?", user);

    // Make sure passwords can be set before doing a strength check
    if (!UserType.canSetPassword(type)) {
      throw new SQLException("Passwords may not be set for UserType=" + type);
    }

    if (password != null && password.length() > 0) {
      // Perform the password check here, too.
      List<PasswordChecker.Result> results = User.checkPassword(user, type, password);
      if (PasswordChecker.hasResults(results)) {
        throw new SQLException("Invalid password: " + PasswordChecker.getResultsString(results).replace('\n', '|'));
      }
    }

    Account.Name account = AccountUserHandler.getAccountForUser(conn, user);
    int linuxServer = getServerForUserServer(conn, userServer);
    try {
      AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
      conn.close(); // Don't hold database connection while connecting to the daemon
      daemonConnector.setLinuxServerAccountPassword(user, password);
    } catch (IOException | SQLException err) {
      System.err.println("Unable to set linux account password for " + user + " on " + linuxServer);
      throw err;
    }

    // Update the linux.Server table for emailmon and ftpmon
    /*if (username.equals(User.EMAILMON)) {
      conn.update("update linux.\"Server\" set emailmon_password=? where server=?", password == null || password.length() == 0?null:password, linuxServer);
      invalidateList.addTable(conn, Table.TableId.AO_SERVERS, ServerHandler.getAccountsForHost(conn, linuxServer), linuxServer, false);
    } else if (username.equals(User.FTPMON)) {
      conn.update("update linux.\"Server\" set ftpmon_password=? where server=?", password == null || password.length() == 0?null:password, linuxServer);
      invalidateList.addTable(conn, Table.TableId.AO_SERVERS, ServerHandler.getAccountsForHost(conn, linuxServer), linuxServer, false);
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
    if (password == null) {
      if (isUserServerDisabled(conn, userServer)) {
        throw new SQLException("Unable to clear UserServer predisable password, account disabled: " + userServer);
      }
    } else if (!isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to set UserServer predisable password, account not disabled: " + userServer);
    }

    // Update the database
    conn.update(
        "update linux.\"UserServer\" set predisable_password=? where id=?",
        password,
        userServer
    );

    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the junk email retention for User named '" + User.MAIL + '\'');
    }

    // Update the database
    if (days == -1) {
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
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the spam assassin integration mode for User named '" + User.MAIL + '\'');
    }

    // Update the database
    conn.update(
        "update linux.\"UserServer\" set sa_integration_mode=? where id=?",
        mode,
        userServer
    );

    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the spam assassin required score for User named '" + User.MAIL + '\'');
    }

    // Update the database
    conn.update(
        "update linux.\"UserServer\" set sa_required_score=? where id=?",
        requiredScore,
        userServer
    );

    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the spam assassin discard score for User named '" + User.MAIL + '\'');
    }

    // Update the database
    if (discardScore == -1) {
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
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the trash email retention for User named '" + User.MAIL + '\'');
    }

    // Update the database
    if (days == -1) {
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
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to set the use_inbox flag for User named '" + User.MAIL + '\'');
    }

    // Update the database
    conn.update(
        "update linux.\"UserServer\" set use_inbox=? where id=?",
        useInbox,
        userServer
    );

    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_SERVER_ACCOUNTS,
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
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
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
    if (!canGroupAccessServer(conn, source, group, linuxServer)) {
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
    if (userServer == -1) {
      throw new SQLException("Unable to find UserServer for " + user + " on " + linuxServer);
    }
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
    if (isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to compare password, UserServer disabled: " + userServer);
    }

    com.aoindustries.aoserv.client.linux.User.Name user = getUserForUserServer(conn, userServer);
    if (user.equals(User.MAIL)) {
      throw new SQLException("Not allowed to compare password for UserServer named '" + User.MAIL + "': " + userServer);
    }
    String type = conn.queryString("select type from linux.\"User\" where username=?", user);

    // Make sure passwords can be set before doing a comparison
    if (!UserType.canSetPassword(type)) {
      throw new SQLException("Passwords may not be compared for UserType=" + type);
    }

    // Perform the password comparison
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn,
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
    if (isUserDisabled(conn, user)) {
      throw new SQLException("Unable to set primary GroupUser, User disabled: " + user);
    }
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
        Table.TableId.LINUX_GROUP_ACCOUNTS,
        InvalidateList.getAccountCollection(AccountUserHandler.getAccountForUser(conn, user), getAccountForGroup(conn, group)),
        getServersForGroupUser(conn, groupUser),
        false
    );
  }
}
