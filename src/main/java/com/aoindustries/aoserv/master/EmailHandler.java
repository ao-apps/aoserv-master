/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2025  AO Industries, Inc.
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
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.lang.validation.ValidationResult;
import com.aoapps.net.DomainName;
import com.aoapps.net.Email;
import com.aoapps.net.HostAddress;
import com.aoindustries.aoserv.client.AoservObject;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.email.InboxAttributes;
import com.aoindustries.aoserv.client.email.List;
import com.aoindustries.aoserv.client.email.MajordomoList;
import com.aoindustries.aoserv.client.email.MajordomoServer;
import com.aoindustries.aoserv.client.email.SmtpRelay;
import com.aoindustries.aoserv.client.email.SpamMessage;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.GroupType;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AoservDaemonConnector;
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
public final class EmailHandler {

  /** Make no instances. */
  private EmailHandler() {
    throw new AssertionError();
  }

  private static final Logger logger = Logger.getLogger(EmailHandler.class.getName());

  private static final Map<Integer, Boolean> disabledLists = new HashMap<>();
  private static final Map<Integer, Boolean> disabledPipes = new HashMap<>();
  private static final Map<Integer, Boolean> disabledSmtpRelays = new HashMap<>();

  public static boolean canAccessDomain(DatabaseConnection conn, RequestSource source, int domain) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        return NetHostHandler.canAccessHost(conn, source, getLinuxServerForDomain(conn, domain));
      } else {
        return true;
      }
    } else {
      return PackageHandler.canAccessPackage(conn, source, getPackageForDomain(conn, domain));
    }
  }

  public static void checkAccessDomain(DatabaseConnection conn, RequestSource source, String action, int domain) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForDomain(conn, domain));
      }
    } else {
      PackageHandler.checkAccessPackage(conn, source, action, getPackageForDomain(conn, domain));
    }
  }

  public static void checkAccessSmtpRelay(DatabaseConnection conn, RequestSource source, String action, int smtpRelay) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForSmtpRelay(conn, smtpRelay));
      }
    } else {
      PackageHandler.checkAccessPackage(conn, source, action, getPackageForSmtpRelay(conn, smtpRelay));
    }
  }

  public static void checkAccessAddress(DatabaseConnection conn, RequestSource source, String action, int address) throws IOException, SQLException {
    checkAccessDomain(conn, source, action, getDomainForAddress(conn, address));
  }

  public static void checkAccessList(DatabaseConnection conn, RequestSource source, String action, int list) throws IOException, SQLException {
    LinuxAccountHandler.checkAccessGroupServer(conn, source, action, getLinuxGroupServerForList(conn, list));
  }

  public static void checkAccessListPath(DatabaseConnection conn, RequestSource source, String action, int linuxServer, PosixPath path) throws IOException, SQLException {
    if (
        !List.isValidRegularPath(
            path,
            NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer)
        )
    ) {
      String pathStr = path.toString();
      // Can also be a path in a majordomo server that they may access
      if (pathStr.startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString() + '/')) {
        int pos = pathStr.indexOf('/', MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString().length() + 1);
        if (pos != -1) {
          String domain = pathStr.substring(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString().length() + 1, pos);
          pathStr = pathStr.substring(pos + 1);
          if (pathStr.startsWith("lists/")) {
            String listName = pathStr.substring(6);
            if (MajordomoList.isValidListName(listName)) {
              int ed = getDomain(conn, linuxServer, domain);
              checkAccessMajordomoServer(conn, source, action, getMajordomoServer(conn, ed));
              return;
            }
          }
        }
      }
      String message = "email.List.path=" + path + " not allowed, '" + action + "'";
      throw new SQLException(message);
    }
  }

  public static void checkAccessPipe(DatabaseConnection conn, RequestSource source, String action, int pipe) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForPipe(conn, pipe));
      }
    } else {
      PackageHandler.checkAccessPackage(conn, source, action, getPackageForPipe(conn, pipe));
    }
  }

  public static void checkAccessPipeCommand(
      DatabaseConnection conn,
      RequestSource source,
      String action,
      String path
  ) throws IOException, SQLException {
    throw new SQLException("Method not implemented.");
  }

  public static void checkAccessMajordomoServer(DatabaseConnection conn, RequestSource source, String action, int majordomoServer) throws IOException, SQLException {
    checkAccessDomain(conn, source, action, majordomoServer);
  }

  public static int addAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      String address,
      int domain
  ) throws IOException, SQLException {
    checkAccessDomain(conn, source, "addAddress", domain);

    return addAddress0(conn, invalidateList, address, domain);
  }

  private static int addAddress0(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      String address,
      int domain
  ) throws IOException, SQLException {
    {
      ValidationResult result = Email.validate(
          address,
          getNetDomainForDomain(conn, domain)
      );
      if (!result.isValid()) {
        throw new SQLException("Invalid email address: " + result);
      }
    }

    int address_id = conn.updateInt(
        "INSERT INTO email.\"Address\" (address, \"domain\") VALUES (?,?) RETURNING id",
        address,
        domain
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_ADDRESSES,
        getAccountForAddress(conn, address_id),
        getLinuxServerForAddress(conn, address_id),
        false
    );
    return address_id;
  }

  public static int addForwarding(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address,
      Email destination
  ) throws IOException, SQLException {
    String destinationStr = destination.toString();
    if (destinationStr.toLowerCase().endsWith("@comcast.net")) {
      throw new SQLException(
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
    }

    checkAccessAddress(conn, source, "addForwarding", address);

    int forwarding = conn.updateInt(
        "INSERT INTO email.\"Forwarding\" (email_address, destination) VALUES (?,?) RETURNING id",
        address,
        AoservObject.USE_SQL_DATA_WRITE ? destination : destinationStr
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_FORWARDING,
        getAccountForAddress(conn, address),
        getLinuxServerForAddress(conn, address),
        false
    );

    return forwarding;
  }

  public static int addList(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      PosixPath path,
      int userServer,
      int groupServer
  ) throws IOException, SQLException {
    checkAccessListPath(conn, source, "addList", LinuxAccountHandler.getServerForUserServer(conn, userServer), path);

    // Allow the mail user
    com.aoindustries.aoserv.client.linux.User.Name user = LinuxAccountHandler.getUserForUserServer(conn, userServer);
    if (!user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      LinuxAccountHandler.checkAccessUserServer(conn, source, "addList", userServer);
    }
    // Check the group
    LinuxAccountHandler.checkAccessGroupServer(conn, source, "addList", groupServer);

    return addList0(
        conn,
        invalidateList,
        path,
        userServer,
        groupServer
    );
  }

  private static int addList0(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      PosixPath path,
      int linuxUserServer,
      int linuxGroupServer
  ) throws IOException, SQLException {
    if (LinuxAccountHandler.isUserServerDisabled(conn, linuxUserServer)) {
      throw new SQLException("Unable to add List, UserServer disabled: " + linuxUserServer);
    }
    final Account.Name packageName = LinuxAccountHandler.getPackageForGroupServer(conn, linuxGroupServer);
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to add List, Package disabled: " + packageName);
    }

    // The server for both account and group must be the same
    final int userServer_linuxServer = LinuxAccountHandler.getServerForUserServer(conn, linuxUserServer);
    final int groupServer_linuxServer = LinuxAccountHandler.getServerForGroupServer(conn, linuxGroupServer);
    if (userServer_linuxServer != groupServer_linuxServer) {
      throw new SQLException("(linux.UserServer.id=" + linuxUserServer + ").ao_server != (linux.GroupServer.id=" + linuxGroupServer + ").ao_server");
    }
    // Must not already have this path on this server
    if (
        conn.queryBoolean(
            "select\n"
                + "  (\n"
                + "    select\n"
                + "      el.id\n"
                + "    from\n"
                + "      email.\"List\" el,\n"
                + "      linux.\"GroupServer\" lsg\n"
                + "    where\n"
                + "      el.path=?\n"
                + "      and el.linux_server_group=lsg.id\n"
                + "      and lsg.ao_server=?\n"
                + "    limit 1\n"
                + "  ) is not null",
            path,
            groupServer_linuxServer
        )
    ) {
      throw new SQLException("List path already used: " + path + " on " + groupServer_linuxServer);
    }

    final int list = conn.updateInt(
        "INSERT INTO email.\"List\" (\n"
            + "  \"path\",\n"
            + "  linux_server_account,\n"
            + "  linux_server_group\n"
            + "values(\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?\n"
            + ") RETURNING id",
        path,
        linuxUserServer,
        linuxGroupServer
    );

    // Create the empty list file
    final int uid = LinuxAccountHandler.getUidForUserServer(conn, linuxUserServer);
    final int gid = LinuxAccountHandler.getGidForGroupServer(conn, linuxGroupServer);
    final int mode = path.toString().startsWith(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY.toString() + '/') ? 0644 : 0640;
    final AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, userServer_linuxServer);
    conn.commit();
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setEmailListFile(path, "", uid, gid, mode);

    // Notify all clients of the update
    invalidateList.addTable(conn,
        Table.TableId.EMAIL_LISTS,
        InvalidateList.allAccounts,
        userServer_linuxServer,
        false
    );

    return list;
  }

  public static int addListAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address,
      int list
  ) throws IOException, SQLException {
    checkAccessAddress(conn, source, "addListAddress", address);
    checkAccessList(conn, source, "addListAddress", list);

    return addListAddress0(
        conn,
        invalidateList,
        address,
        list
    );
  }

  private static int addListAddress0(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int address,
      int list
  ) throws IOException, SQLException {
    // The email_domain and the email_list must be on the same server
    int domainLinuxServer = getLinuxServerForAddress(conn, address);
    int list_linuxServer = getLinuxServerForList(conn, list);
    if (domainLinuxServer != list_linuxServer) {
      throw new SQLException("List server (" + list_linuxServer + ") != Email address server (" + domainLinuxServer + ')');
    }

    int listAddress = conn.updateInt(
        "INSERT INTO email.\"ListAddress\" (email_address, email_list) VALUES (?,?) RETURNING id",
        address,
        list
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_LIST_ADDRESSES,
        getAccountForAddress(conn, address),
        getLinuxServerForAddress(conn, address),
        false
    );

    return listAddress;
  }

  /**
   * Adds an email pipe.
   */
  public static int addPipe(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int linuxServer,
      String command,
      Account.Name packageName
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "addPipe", linuxServer);
    checkAccessPipeCommand(conn, source, "addPipe", command);
    PackageHandler.checkAccessPackage(conn, source, "addPipe", packageName);
    PackageHandler.checkPackageAccessHost(conn, source, "addPipe", packageName, linuxServer);

    return addPipe0(
        conn,
        invalidateList,
        linuxServer,
        command,
        packageName
    );
  }

  private static int addPipe0(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int linuxServer,
      String command,
      Account.Name packageName
  ) throws IOException, SQLException {
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to add Pipe, Package disabled: " + packageName);
    }

    int pipe = conn.updateInt("INSERT INTO email.\"Pipe\" VALUES (default,?,?,?,null) RETURNING id", linuxServer, command, packageName);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_PIPES,
        PackageHandler.getAccountForPackage(conn, packageName),
        linuxServer,
        false
    );
    return pipe;
  }

  public static int addPipeAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address,
      int pipe
  ) throws IOException, SQLException {
    checkAccessAddress(conn, source, "addPipeAddress", address);
    checkAccessPipe(conn, source, "addPipeAddress", pipe);

    return addPipeAddress0(
        conn,
        invalidateList,
        address,
        pipe
    );
  }

  private static int addPipeAddress0(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int address,
      int pipe
  ) throws IOException, SQLException {
    int pipeAddress = conn.updateInt("INSERT INTO email.\"PipeAddress\" VALUES (default,?,?) RETURNING id", address, pipe);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_PIPE_ADDRESSES,
        getAccountForAddress(conn, address),
        getLinuxServerForAddress(conn, address),
        false
    );

    return pipeAddress;
  }

  public static int addInboxAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address,
      int userServer
  ) throws IOException, SQLException {
    checkAccessAddress(conn, source, "addInboxAddress", address);
    LinuxAccountHandler.checkAccessUserServer(conn, source, "addInboxAddress", userServer);
    com.aoindustries.aoserv.client.linux.User.Name user = LinuxAccountHandler.getUserForUserServer(conn, userServer);
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add email addresses to User named '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }
    // TODO: Make sure they are on the same server

    int inboxAddress = conn.updateInt("INSERT INTO email.\"InboxAddress\" VALUES (default,?,?) RETURNING id", address, userServer);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.LINUX_ACC_ADDRESSES,
        getAccountForAddress(conn, address),
        getLinuxServerForAddress(conn, address),
        false
    );
    return inboxAddress;
  }

  public static int addDomain(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      DomainName netDomain,
      int linuxServer,
      Account.Name packageName
  ) throws IOException, SQLException {
    AoservMaster.checkAccessHostname(conn, source, "addDomain", netDomain.toString());
    NetHostHandler.checkAccessHost(conn, source, "addDomain", linuxServer);
    PackageHandler.checkAccessPackage(conn, source, "addDomain", packageName);
    PackageHandler.checkPackageAccessHost(conn, source, "addDomain", packageName, linuxServer);

    int domain = conn.updateInt("INSERT INTO email.\"Domain\" VALUES (default,?,?,?) RETURNING id", netDomain, linuxServer, packageName);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_DOMAINS,
        PackageHandler.getAccountForPackage(conn, packageName),
        linuxServer,
        false
    );
    return domain;
  }

  /**
   * Adds a email SMTP relay.
   */
  public static int addSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      Account.Name packageName,
      int linuxServer,
      HostAddress host,
      String type,
      long duration
  ) throws IOException, SQLException {
    // Only master users can add relays
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu == null) {
      throw new SQLException("Only master users may add SMTP relays.");
    }

    PackageHandler.checkAccessPackage(conn, source, "addSmtpRelay", packageName);
    if (linuxServer == -1) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        throw new SQLException("Only super-users may add global SMTP relays.");
      }
    } else {
      NetHostHandler.checkAccessHost(conn, source, "addSmtpRelay", linuxServer);
      PackageHandler.checkPackageAccessHost(conn, source, "addSmtpRelay", packageName, linuxServer);
    }
    if (duration != -1 && duration <= 0) {
      throw new SQLException("Duration must be positive: " + duration);
    }

    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to add SmtpRelay, Package disabled: " + packageName);
    }

    int smtpRelay;
    if (linuxServer == -1) {
      smtpRelay = conn.updateInt(
          "INSERT INTO email.\"SmtpRelay\" values(default,?,null,?,?,now(),now(),0,?,null) RETURNING id",
          packageName,
          host,
          type,
          duration == -1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(System.currentTimeMillis() + duration) // TODO: Timestamp nanosecond precision
      );
    } else {
      smtpRelay = conn.updateInt(
          "INSERT INTO email.\"SmtpRelay\" VALUES (default,?,?,?,?,now(),now(),0,?,null) RETURNING id",
          packageName,
          linuxServer,
          host,
          type,
          duration == -1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(System.currentTimeMillis() + duration) // TODO: Timestamp nanosecond precision
      );
    }

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_SMTP_RELAYS,
        PackageHandler.getAccountForPackage(conn, packageName),
        linuxServer,
        false
    );
    return smtpRelay;
  }

  public static int addSpamMessage(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int smtpRelay,
      String message
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = AoservMaster.getUser(conn, currentAdministrator);
    UserHost[] masterServers = masterUser == null ? null : AoservMaster.getUserHosts(conn, currentAdministrator);
    if (masterUser == null || masterServers.length != 0) {
      throw new SQLException("Only master users may add spam email messages.");
    }

    int spamMessage = conn.updateInt(
        "INSERT INTO email.\"SpamMessage\" VALUES(default,?,now(),?) RETURNING id",
        smtpRelay,
        message
    );

    // Notify all clients of the update
    invalidateList.addTable(conn,
        Table.TableId.SPAM_EMAIL_MESSAGES,
        InvalidateList.allAccounts,
        InvalidateList.allHosts,
        false
    );

    return spamMessage;
  }

  public static int addMajordomoList(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int majordomoServer,
      String listName
  ) throws IOException, SQLException {
    if (!MajordomoList.isValidListName(listName)) {
      throw new SQLException("Invalide Majordomo list name: " + listName);
    }

    checkAccessMajordomoServer(conn, source, "addMajordomoList", majordomoServer);

    final DomainName domainName = getNetDomainForDomain(conn, majordomoServer);
    final PosixPath msPath;
    final PosixPath listPath;
    final PosixPath infoPath;
    final PosixPath introPath;
    try {
      msPath = PosixPath.valueOf(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY + "/" + domainName);
      listPath = PosixPath.valueOf(msPath + "/lists/" + listName);
      infoPath = PosixPath.valueOf(listPath + ".info");
      introPath = PosixPath.valueOf(listPath + ".intro");
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    final int linuxServer = getLinuxServerForDomain(conn, majordomoServer);

    // Disabled checks
    final Account.Name packageName = getPackageForDomain(conn, majordomoServer);
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to add Majordomo list, Package for Majordomo server #" + majordomoServer + " is disabled: " + packageName);
    }

    // Find the email addresses
    final int ownerListnameAddress = getOrAddAddress(conn, invalidateList, "owner-" + listName, majordomoServer);
    final int listnameOwnerAddress = getOrAddAddress(conn, invalidateList, listName + "-owner", majordomoServer);
    final int listnameApprovalAddress = getOrAddAddress(conn, invalidateList, listName + "-approval", majordomoServer);

    // Add the email list
    final int lsa = getLinuxUserServerForMajordomoServer(conn, majordomoServer);
    final int lsg = getLinuxGroupServerForMajordomoServer(conn, majordomoServer);
    final int list = addList0(
        conn,
        invalidateList,
        listPath,
        lsa,
        lsg
    );

    // Add the listname email pipe and address
    final int listnamePipe = addPipe0(
        conn,
        invalidateList,
        linuxServer,
        msPath + "/wrapper resend -l " + listName + ' ' + listName + "-list@" + domainName,
        packageName
    );
    final int listnameAddress = getOrAddAddress(conn, invalidateList, listName, majordomoServer);
    final int listnamePipeAddress = addPipeAddress0(conn, invalidateList, listnameAddress, listnamePipe);

    // Add the listname-list email list address
    final int listnameListAddress = getOrAddAddress(conn, invalidateList, listName + "-list", majordomoServer);
    final int listnameListListAddress = addListAddress0(conn, invalidateList, listnameListAddress, list);

    // Add the listname-request email pipe and address
    int listnameRequestPipe = addPipe0(
        conn,
        invalidateList,
        linuxServer,
        msPath + "/wrapper majordomo -l " + listName,
        packageName
    );
    final int listnameRequestAddress = getOrAddAddress(conn, invalidateList, listName + "-request", majordomoServer);
    final int listnameRequestPipeAddress = addPipeAddress0(conn, invalidateList, listnameRequestAddress, listnameRequestPipe);

    // Add the majordomo_list
    conn.update(
        "insert into email.\"MajordomoList\" values(?,?,?,?,?,?,?,?,?)",
        list,
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
        Table.TableId.MAJORDOMO_LISTS,
        PackageHandler.getAccountForPackage(conn, packageName),
        linuxServer,
        false
    );

    // Create the empty info and intro files
    final String file = MajordomoList.getDefaultInfoFile(domainName, listName);
    final String introFile = MajordomoList.getDefaultIntroFile(domainName, listName);
    final int uid = LinuxAccountHandler.getUidForUserServer(conn, lsa);
    final int gid = LinuxAccountHandler.getGidForGroupServer(conn, lsg);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.commit();
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setEmailListFile(infoPath, file, uid, gid, 0664);
    daemonConnector.setEmailListFile(introPath, introFile, uid, gid, 0664);

    return list;
  }

  public static void addMajordomoServer(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int domain,
      int userServer,
      int groupServer,
      String version
  ) throws IOException, SQLException {
    // Security checks
    checkAccessDomain(conn, source, "addMajordomoServer", domain);
    LinuxAccountHandler.checkAccessUserServer(conn, source, "addMajordomoServer", userServer);
    com.aoindustries.aoserv.client.linux.User.Name lsaUsername = LinuxAccountHandler.getUserForUserServer(conn, userServer);
    if (lsaUsername.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Unable to add MajordomoServer with UserServer of '" + lsaUsername + '\'');
    }
    String lsaType = LinuxAccountHandler.getTypeForUserServer(conn, userServer);
    if (
        !lsaType.equals(UserType.APPLICATION)
            && !lsaType.equals(UserType.USER)
    ) {
      throw new SQLException("May only add Majordomo servers using Linux accounts of type '" + UserType.APPLICATION + "' or '" + UserType.USER + "', trying to use '" + lsaType + '\'');
    }
    LinuxAccountHandler.checkAccessGroupServer(conn, source, "addMajordomoServer", groupServer);
    Group.Name lsgName = LinuxAccountHandler.getGroupForGroupServer(conn, groupServer);
    if (
        lsgName.equals(Group.FTPONLY)
            || lsgName.equals(Group.MAIL)
            || lsgName.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Unable to add MajordomoServer with GroupServer of '" + lsgName + '\'');
    }
    String lsgType = LinuxAccountHandler.getTypeForGroupServer(conn, groupServer);
    if (
        !lsgType.equals(GroupType.APPLICATION)
            && !lsgType.equals(GroupType.USER)
    ) {
      throw new SQLException("May only add Majordomo servers using Linux groups of type '" + GroupType.APPLICATION + "' or '" + GroupType.USER + "', trying to use '" + lsgType + '\'');
    }

    // Data integrity checks
    int domain_linuxServer = getLinuxServerForDomain(conn, domain);
    int userServer_linuxServer = LinuxAccountHandler.getServerForUserServer(conn, userServer);
    if (domain_linuxServer != userServer_linuxServer) {
      throw new SQLException("((email.Domain.id=" + domain + ").ao_server='" + domain_linuxServer + "') != ((linux.UserServer.id=" + userServer + ").ao_server='" + userServer_linuxServer + "')");
    }
    int groupServer_linuxServer = LinuxAccountHandler.getServerForGroupServer(conn, groupServer);
    if (domain_linuxServer != groupServer_linuxServer) {
      throw new SQLException("((email.Domain.id=" + domain + ").ao_server='" + domain_linuxServer + "') != ((linux.GroupServer.id=" + groupServer + ").ao_server='" + groupServer_linuxServer + "')");
    }

    // Disabled checks
    Account.Name packageName = getPackageForDomain(conn, domain);
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to add Majordomo server: Package for domain #" + domain + " is disabled: " + packageName);
    }
    if (LinuxAccountHandler.isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to add Majordomo server: UserServer disabled: " + userServer);
    }
    Account.Name lgPackageName = LinuxAccountHandler.getPackageForGroupServer(conn, groupServer);
    if (PackageHandler.isPackageDisabled(conn, lgPackageName)) {
      throw new SQLException("Unable to add Majordomo server: Package for GroupServer #" + groupServer + " is disabled: " + lgPackageName);
    }

    // Create the majordomo email pipe
    DomainName domainName = getNetDomainForDomain(conn, domain);
    PosixPath majordomoServerPath;
    try {
      majordomoServerPath = PosixPath.valueOf(MajordomoServer.MAJORDOMO_SERVER_DIRECTORY + "/" + domainName);
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    int majordomoPipe = addPipe0(conn, invalidateList, domain_linuxServer, majordomoServerPath + "/wrapper majordomo", packageName);
    int majordomoAddress = getOrAddAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_ADDRESS, domain);
    int majordomoPipeAddress = addPipeAddress0(conn, invalidateList, majordomoAddress, majordomoPipe);

    int ownerMajordomoAddress = getOrAddAddress(conn, invalidateList, MajordomoServer.OWNER_MAJORDOMO_ADDRESS, domain);
    int majordomoOwnerAddress = getOrAddAddress(conn, invalidateList, MajordomoServer.MAJORDOMO_OWNER_ADDRESS, domain);

    conn.update(
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
        userServer,
        groupServer,
        version,
        majordomoPipeAddress,
        ownerMajordomoAddress,
        majordomoOwnerAddress
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.MAJORDOMO_SERVERS,
        PackageHandler.getAccountForPackage(conn, packageName),
        domain_linuxServer,
        false
    );
  }

  public static void disableList(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int list
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableList", disableLog, false);
    checkAccessList(conn, source, "disableList", list);
    if (isListDisabled(conn, list)) {
      throw new SQLException("List is already disabled: " + list);
    }

    conn.update(
        "update email.\"List\" set disable_log=? where id=?",
        disableLog,
        list
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_LISTS,
        getAccountForList(conn, list),
        getLinuxServerForList(conn, list),
        false
    );
  }

  public static void disablePipe(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int pipe
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disablePipe", disableLog, false);
    checkAccessPipe(conn, source, "disablePipe", pipe);
    if (isPipeDisabled(conn, pipe)) {
      throw new SQLException("Pipe is already disabled: " + pipe);
    }

    conn.update(
        "update email.\"Pipe\" set disable_log=? where id=?",
        disableLog,
        pipe
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_PIPES,
        getAccountForPipe(conn, pipe),
        getLinuxServerForPipe(conn, pipe),
        false
    );
  }

  public static void disableSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int smtpRelay
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableSmtpRelay", disableLog, false);
    checkAccessSmtpRelay(conn, source, "disableSmtpRelay", smtpRelay);
    if (isSmtpRelayDisabled(conn, smtpRelay)) {
      throw new SQLException("SmtpRelay is already disabled: " + smtpRelay);
    }

    conn.update(
        "update email.\"SmtpRelay\" set disable_log=? where id=?",
        disableLog,
        smtpRelay
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_SMTP_RELAYS,
        getAccountForSmtpRelay(conn, smtpRelay),
        getLinuxServerForSmtpRelay(conn, smtpRelay),
        false
    );
  }

  public static void enableList(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int list
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "enableList", list);
    int disableLog = getDisableLogForList(conn, list);
    if (disableLog == -1) {
      throw new SQLException("List is already enabled: " + list);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableList", disableLog, true);
    Account.Name pk = getPackageForList(conn, list);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable List #" + list + ", Package not enabled: " + pk);
    }

    conn.update(
        "update email.\"List\" set disable_log=null where id=?",
        list
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_LISTS,
        PackageHandler.getAccountForPackage(conn, pk),
        getLinuxServerForList(conn, list),
        false
    );
  }

  public static void enablePipe(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int pipe
  ) throws IOException, SQLException {
    checkAccessPipe(conn, source, "enablePipe", pipe);
    int disableLog = getDisableLogForPipe(conn, pipe);
    if (disableLog == -1) {
      throw new SQLException("Pipe is already enabled: " + pipe);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enablePipe", disableLog, true);
    Account.Name pk = getPackageForPipe(conn, pipe);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable Pipe #" + pipe + ", Package not enabled: " + pk);
    }

    conn.update(
        "update email.\"Pipe\" set disable_log=null where id=?",
        pipe
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_PIPES,
        PackageHandler.getAccountForPackage(conn, pk),
        getLinuxServerForPipe(conn, pipe),
        false
    );
  }

  public static void enableSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int smtpRelay
  ) throws IOException, SQLException {
    checkAccessSmtpRelay(conn, source, "enableSmtpRelay", smtpRelay);
    int disableLog = getDisableLogForSmtpRelay(conn, smtpRelay);
    if (disableLog == -1) {
      throw new SQLException("SmtpRelay is already enabled: " + smtpRelay);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableSmtpRelay", disableLog, true);
    Account.Name pk = getPackageForSmtpRelay(conn, smtpRelay);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable SmtpRelay #" + smtpRelay + ", Package not enabled: " + pk);
    }

    conn.update(
        "update email.\"SmtpRelay\" set disable_log=null where id=?",
        smtpRelay
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.EMAIL_SMTP_RELAYS,
        PackageHandler.getAccountForPackage(conn, pk),
        getLinuxServerForSmtpRelay(conn, smtpRelay),
        false
    );
  }

  public static int getDisableLogForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from email.\"List\" where id=?", list);
  }

  public static int getDisableLogForPipe(DatabaseConnection conn, int pipe) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from email.\"Pipe\" where id=?", pipe);
  }

  public static int getDisableLogForSmtpRelay(DatabaseConnection conn, int smtpRelay) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from email.\"SmtpRelay\" where id=?", smtpRelay);
  }

  public static String getListFile(
      DatabaseConnection conn,
      RequestSource source,
      int list
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "getListFile", list);

    PosixPath path = getPathForList(conn, list);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
        conn,
        getLinuxServerForList(conn, list)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.getEmailListFile(path);
  }

  public static IntList getListsForLinuxUserServer(
      DatabaseConnection conn,
      int linuxUserServer
  ) throws IOException, SQLException {
    return conn.queryIntList("select id from email.\"List\" where linux_server_account=?", linuxUserServer);
  }

  public static IntList getListsForPackage(
      DatabaseConnection conn,
      Account.Name packageName
  ) throws IOException, SQLException {
    return conn.queryIntList(
        "select\n"
            + "  el.id\n"
            + "from\n"
            + "  linux.\"Group\" lg,\n"
            + "  linux.\"GroupServer\" lsg,\n"
            + "  email.\"List\" el\n"
            + "where\n"
            + "  lg.package=?\n"
            + "  and lg.name=lsg.name\n"
            + "  and lsg.id=el.linux_server_group",
        packageName
    );
  }

  public static IntList getPipesForPackage(
      DatabaseConnection conn,
      Account.Name name
  ) throws IOException, SQLException {
    return conn.queryIntList("select id from email.\"Pipe\" where package=?", name);
  }

  public static long[] getImapFolderSizes(
      DatabaseConnection conn,
      RequestSource source,
      int userServer,
      String[] folderNames
  ) throws IOException, SQLException {
    LinuxAccountHandler.checkAccessUserServer(conn, source, "getImapFolderSizes", userServer);
    int linuxServer = LinuxAccountHandler.getServerForUserServer(conn, userServer);
    if (DaemonHandler.isDaemonAvailable(linuxServer)) {
      com.aoindustries.aoserv.client.linux.User.Name user = LinuxAccountHandler.getUserForUserServer(conn, userServer);
      try {
        AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
        conn.close(); // Don't hold database connection while connecting to the daemon
        return daemonConnector.getImapFolderSizes(user, folderNames);
      } catch (IOException err) {
        logger.log(Level.SEVERE, "userServer=" + userServer + ", linuxServer=" + linuxServer + ", username=" + user + ", folderNames=" + Arrays.asList(folderNames), err);
        DaemonHandler.flagDaemonAsDown(linuxServer);
      }
    }
    long[] sizes = new long[folderNames.length];
    Arrays.fill(sizes, -1);
    return sizes;
  }

  public static InboxAttributes getInboxAttributes(
      DatabaseConnection conn,
      RequestSource source,
      int userServer
  ) throws IOException, SQLException {
    LinuxAccountHandler.checkAccessUserServer(conn, source, "getInboxAttributes", userServer);
    int linuxServer = LinuxAccountHandler.getServerForUserServer(conn, userServer);
    if (DaemonHandler.isDaemonAvailable(linuxServer)) {
      com.aoindustries.aoserv.client.linux.User.Name user = LinuxAccountHandler.getUserForUserServer(conn, userServer);
      try {
        AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
        conn.close(); // Don't hold database connection while connecting to the daemon
        return daemonConnector.getInboxAttributes(user);
      } catch (IOException err) {
        logger.log(Level.SEVERE, "linux_server_account=" + userServer + ", linuxServer=" + linuxServer + ", username=" + user, err);
        DaemonHandler.flagDaemonAsDown(linuxServer);
      }
    }
    return null;
  }

  public static IntList getSmtpRelaysForPackage(
      DatabaseConnection conn,
      Account.Name name
  ) throws IOException, SQLException {
    return conn.queryIntList("select id from email.\"SmtpRelay\" where package=?", name);
  }

  public static int getDomain(
      DatabaseConnection conn,
      int linuxServer,
      String path
  ) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  el.id\n"
            + "from\n"
            + "  email.\"List\" el,\n"
            + "  linux.\"GroupServer\" lsg\n"
            + "where\n"
            + "  el.path=path\n"
            + "  and el.linux_server_group=lsg.id\n"
            + "  and lsg.ao_server=?",
        path,
        linuxServer
    );
  }

  public static int getDomainForAddress(
      DatabaseConnection conn,
      int address
  ) throws IOException, SQLException {
    return conn.queryInt("select domain from email.\"Address\" where id=?", address);
  }

  public static String getMajordomoInfoFile(
      DatabaseConnection conn,
      RequestSource source,
      int majordomoList
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "getMajordomoInfoFile", majordomoList);
    PosixPath infoPath;
    try {
      infoPath = PosixPath.valueOf(getPathForList(conn, majordomoList) + ".info");
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
        conn,
        getLinuxServerForList(conn, majordomoList)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.getEmailListFile(infoPath);
  }

  public static String getMajordomoIntroFile(
      DatabaseConnection conn,
      RequestSource source,
      int majordomoList
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "getMajordomoIntroFile", majordomoList);
    PosixPath introPath;
    try {
      introPath = PosixPath.valueOf(getPathForList(conn, majordomoList) + ".intro");
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
        conn,
        getLinuxServerForList(conn, majordomoList)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.getEmailListFile(introPath);
  }

  public static int getMajordomoServer(
      DatabaseConnection conn,
      int domain
  ) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  id\n"
            + "from\n"
            + "  email.\"MajordomoServer\"\n"
            + "where\n"
            + "  domain=?",
        domain
    );
  }

  @SuppressWarnings("deprecation")
  public static void getSpamMessagesForSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      StreamableOutput out,
      boolean provideProgress,
      int esr
  ) throws IOException, SQLException {
    com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
    User masterUser = AoservMaster.getUser(conn, currentAdministrator);
    UserHost[] masterServers = masterUser == null ? null : AoservMaster.getUserHosts(conn, currentAdministrator);
    if (masterUser != null && masterServers.length == 0) {
      // TODO: release conn before writing to out
      AoservMaster.writeObjects(
          conn,
          source,
          out,
          provideProgress,
          CursorMode.FETCH,
          new SpamMessage(),
          "select * from email.\"SpamMessage\" where email_relay=?",
          esr
      );
    } else {
      throw new SQLException("Only master users may access email.SpamMessage.");
    }
  }

  public static void invalidateTable(Table.TableId tableId) {
    if (tableId == Table.TableId.EMAIL_LISTS) {
      synchronized (EmailHandler.class) {
        disabledLists.clear();
      }
    } else if (tableId == Table.TableId.EMAIL_PIPES) {
      synchronized (EmailHandler.class) {
        disabledPipes.clear();
      }
    } else if (tableId == Table.TableId.EMAIL_SMTP_RELAYS) {
      synchronized (EmailHandler.class) {
        disabledSmtpRelays.clear();
      }
    }
  }

  public static boolean isListDisabled(DatabaseConnection conn, int list) throws IOException, SQLException {
    synchronized (EmailHandler.class) {
      Integer i = list;
      Boolean o = disabledLists.get(i);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForList(conn, list) != -1;
      disabledLists.put(i, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isPipeDisabled(DatabaseConnection conn, int pipe) throws IOException, SQLException {
    synchronized (EmailHandler.class) {
      Integer i = pipe;
      Boolean o = disabledPipes.get(i);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForPipe(conn, pipe) != -1;
      disabledPipes.put(i, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isSmtpRelayDisabled(DatabaseConnection conn, int smtpRelay) throws IOException, SQLException {
    synchronized (EmailHandler.class) {
      Integer i = smtpRelay;
      Boolean o = disabledSmtpRelays.get(i);
      if (o != null) {
        return o;
      }
      boolean isDisabled = getDisableLogForSmtpRelay(conn, smtpRelay) != -1;
      disabledSmtpRelays.put(i, isDisabled);
      return isDisabled;
    }
  }

  /**
   * Refreshes a email SMTP relay.
   */
  public static void refreshSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int smtpRelay,
      long minDuration
  ) throws IOException, SQLException {
    checkAccessSmtpRelay(conn, source, "refreshSmtpRelay", smtpRelay);

    if (isSmtpRelayDisabled(conn, smtpRelay)) {
      throw new SQLException("Unable to refresh SmtpRelay, SmtpRelay disabled: " + smtpRelay);
    }

    Account.Name packageName = getPackageForSmtpRelay(conn, smtpRelay);
    Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
    int linuxServer = getLinuxServerForSmtpRelay(conn, smtpRelay);
    Timestamp expiration = conn.queryTimestampNullable("select expiration from email.\"SmtpRelay\" where id=?", smtpRelay);
    long exp = expiration == null ? -1 : expiration.getTime();
    long min = minDuration == -1 ? -1 : (System.currentTimeMillis() + minDuration); // TODO: Timestamp nanosecond precision
    conn.update(
        "update email.\"SmtpRelay\" set last_refreshed=now(), refresh_count=refresh_count+1, expiration=? where id=?",
        exp == -1 || min == -1
            ? null
            : new Timestamp(Math.max(exp, min)),
        smtpRelay
    );

    // Delete any old entries
    conn.update(
        "delete from email.\"SmtpRelay\" where package=? and (ao_server is null or ao_server=?) and expiration is not null and now()::date-expiration::date>" + SmtpRelay.HISTORY_DAYS,
        packageName,
        linuxServer
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.EMAIL_SMTP_RELAYS, account, linuxServer, false);
  }

  public static void removeBlackholeAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address
  ) throws IOException, SQLException {
    checkAccessAddress(conn, source, "removeBlackholeAddress", address);

    // Get stuff for use after the try block
    Account.Name account = getAccountForAddress(conn, address);
    int linuxServer = getLinuxServerForAddress(conn, address);

    // Delete from the database
    conn.update("delete from email.\"BlackholeAddress\" where email_address=?", address);

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.BLACKHOLE_EMAIL_ADDRESSES,
        account,
        linuxServer,
        false
    );
  }

  public static void removeAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int address
  ) throws IOException, SQLException {
    checkAccessAddress(conn, source, "removeAddress", address);

    // Get stuff for use after the try block
    final Account.Name account = getAccountForAddress(conn, address);
    final int linuxServer = getLinuxServerForAddress(conn, address);

    // Delete the objects that depend on this one first
    final boolean isBlackhole = conn.queryBoolean("select (select email_address from email.\"BlackholeAddress\" where email_address=?) is not null", address);
    if (isBlackhole) {
      conn.update("delete from email.\"BlackholeAddress\" where email_address=?", address);
    }

    final IntList ids = conn.queryIntList("select id from email.\"InboxAddress\" where email_address=?", address);
    final boolean isLinuxAccAddress = ids.size() > 0;
    if (isLinuxAccAddress) {
      for (int d = 0; d < ids.size(); d++) {
        int laaPkey = ids.getInt(d);
        conn.update("update linux.\"UserServer\" set autoresponder_from=null where autoresponder_from=?", laaPkey);
        conn.update("delete from email.\"InboxAddress\" where id=?", laaPkey);
      }
    }

    final boolean isEmailForwarding = conn.update("delete from email.\"Forwarding\" where email_address=?", address) > 0;
    final boolean isEmailListAddress = conn.update("delete from email.\"ListAddress\" where email_address=?", address) > 0;
    final boolean isEmailPipeAddress = conn.update("delete from email.\"PipeAddress\" where email_address=?", address) > 0;

    // Delete from the database
    conn.update("delete from email.\"Address\" where id=?", address);

    // Notify all clients of the update
    if (isBlackhole) {
      invalidateList.addTable(conn, Table.TableId.BLACKHOLE_EMAIL_ADDRESSES, account, linuxServer, false);
    }
    if (isLinuxAccAddress) {
      invalidateList.addTable(conn, Table.TableId.LINUX_ACC_ADDRESSES, account, linuxServer, false);
    }
    if (isEmailForwarding) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_FORWARDING, account, linuxServer, false);
    }
    if (isEmailListAddress) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_LIST_ADDRESSES, account, linuxServer, false);
    }
    if (isEmailPipeAddress) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
    }
    invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
  }

  public static void removeForwarding(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int forwarding
  ) throws IOException, SQLException {
    int address = conn.queryInt("select email_address from email.\"Forwarding\" where id=?", forwarding);
    checkAccessAddress(conn, source, "removeForwarding", address);

    // Get stuff for use after the try block
    Account.Name account = getAccountForAddress(conn, address);
    int linuxServer = getLinuxServerForAddress(conn, address);

    // Delete from the database
    conn.update("delete from email.\"Forwarding\" where id=?", forwarding);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.EMAIL_FORWARDING, account, linuxServer, false);
  }

  public static void removeListAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int listAddress
  ) throws IOException, SQLException {
    int address = conn.queryInt("select email_address from email.\"ListAddress\" where id=?", listAddress);
    checkAccessAddress(conn, source, "removeListAddress", address);

    // Get stuff for use after the try block
    Account.Name account = getAccountForAddress(conn, address);
    int linuxServer = getLinuxServerForAddress(conn, address);

    // Delete from the database
    conn.update("delete from email.\"ListAddress\" where id=?", listAddress);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.EMAIL_LIST_ADDRESSES, account, linuxServer, false);
  }

  public static void removeList(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int list
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "removeList", list);

    removeList(conn, invalidateList, list);
  }

  public static void removeList(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int list
  ) throws IOException, SQLException {
    // Get the values for later use
    final Account.Name account = getAccountForList(conn, list);
    final int linuxServer = getLinuxServerForList(conn, list);
    final PosixPath path = conn.queryObject(
        ObjectFactories.posixPathFactory,
        "select path from email.\"List\" where id=?",
        list
    );
    // Delete the majordomo_list that is attached to this email list
    if (isMajordomoList(conn, list)) {
      // Get the listname_pipe_add and details
      final int listnamePipeAdd = conn.queryInt("select listname_pipe_add from email.\"MajordomoList\" where email_list=?", list);
      final int listnamePipeAdd_emailAddress = conn.queryInt("select email_address from email.\"PipeAddress\" where id=?", listnamePipeAdd);
      final int listnamePipeAdd_emailPipe = conn.queryInt("select email_pipe from email.\"PipeAddress\" where id=?", listnamePipeAdd);

      // Get the listname_list_add and details
      final int listnameList_listnameListAdd = conn.queryInt("select listname_list_add from email.\"MajordomoList\" where email_list=?", list);
      final int listnameList_emailAddress = conn.queryInt("select email_address from email.\"ListAddress\" where id=?", listnameList_listnameListAdd);

      // Get the listname_request_pipe_add and details
      final int listnameRequestPipeAddress = conn.queryInt("select listname_request_pipe_add from email.\"MajordomoList\" where email_list=?", list);
      final int listnameRequest_address = conn.queryInt("select email_address from email.\"PipeAddress\" where id=?", listnameRequestPipeAddress);
      final int listnameRequest_pipe = conn.queryInt("select email_pipe from email.\"PipeAddress\" where id=?", listnameRequestPipeAddress);

      // Other direct email addresses
      final int ownerListnameAddress = conn.queryInt("select owner_listname_add from email.\"MajordomoList\" where email_list=?", list);
      final int listnameOwnerAddress = conn.queryInt("select listname_owner_add from email.\"MajordomoList\" where email_list=?", list);
      final int listnameApprovalAddress = conn.queryInt("select listname_approval_add from email.\"MajordomoList\" where email_list=?", list);

      conn.update("delete from email.\"MajordomoList\" where email_list=?", list);
      invalidateList.addTable(conn, Table.TableId.MAJORDOMO_LISTS, account, linuxServer, false);

      // Delete the listname_pipe_add
      conn.update("delete from email.\"PipeAddress\" where id=?", listnamePipeAdd);
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
      if (!isAddressUsed(conn, listnamePipeAdd_emailAddress)) {
        conn.update("delete from email.\"Address\" where id=?", listnamePipeAdd_emailAddress);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }
      conn.update("delete from email.\"Pipe\" where id=?", listnamePipeAdd_emailPipe);
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPES, account, linuxServer, false);

      // Delete the listname_list_add
      conn.update("delete from email.\"ListAddress\" where id=?", listnameList_listnameListAdd);
      invalidateList.addTable(conn, Table.TableId.EMAIL_LIST_ADDRESSES, account, linuxServer, false);
      if (!isAddressUsed(conn, listnameList_emailAddress)) {
        conn.update("delete from email.\"Address\" where id=?", listnameList_emailAddress);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }

      // Delete the listname_pipe_add
      conn.update("delete from email.\"PipeAddress\" where id=?", listnameRequestPipeAddress);
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
      if (!isAddressUsed(conn, listnameRequest_address)) {
        conn.update("delete from email.\"Address\" where id=?", listnameRequest_address);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }
      conn.update("delete from email.\"Pipe\" where id=?", listnameRequest_pipe);
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPES, account, linuxServer, false);

      // Other direct email addresses
      if (!isAddressUsed(conn, ownerListnameAddress)) {
        conn.update("delete from email.\"Address\" where id=?", ownerListnameAddress);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }
      if (!isAddressUsed(conn, listnameOwnerAddress)) {
        conn.update("delete from email.\"Address\" where id=?", listnameOwnerAddress);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }
      if (!isAddressUsed(conn, listnameApprovalAddress)) {
        conn.update("delete from email.\"Address\" where id=?", listnameApprovalAddress);
        invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
      }
    }

    // Delete the objects that depend on this one first
    IntList addresses = conn.queryIntList("select email_address from email.\"ListAddress\" where email_list=?", list);
    int size = addresses.size();
    boolean addressesModified = size > 0;
    for (int c = 0; c < size; c++) {
      int address = addresses.getInt(c);
      conn.update("delete from email.\"ListAddress\" where email_address=? and email_list=?", address, list);
      if (!isAddressUsed(conn, address)) {
        conn.update("delete from email.\"Address\" where id=?", address);
      }
    }

    // Delete from the database
    conn.update("delete from email.\"List\" where id=?", list);

    // Notify all clients of the update
    if (addressesModified) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_LIST_ADDRESSES, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
    invalidateList.addTable(conn, Table.TableId.EMAIL_LISTS, account, linuxServer, false);

    // Remove the list file from the server
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.commit();
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.removeEmailList(path);
  }

  public static void removeInboxAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int inboxAddress
  ) throws IOException, SQLException {
    int ea = conn.queryInt("select email_address from email.\"InboxAddress\" where id=?", inboxAddress);
    checkAccessAddress(conn, source, "removeInboxAddress", ea);

    // Get stuff for use after the try block
    Account.Name account = getAccountForAddress(conn, ea);
    int linuxServer = getLinuxServerForAddress(conn, ea);

    // Delete from the database
    conn.update("update linux.\"UserServer\" set autoresponder_from=null where autoresponder_from=?", inboxAddress);
    conn.update("delete from email.\"InboxAddress\" where id=?", inboxAddress);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.LINUX_ACC_ADDRESSES, account, linuxServer, false);
  }

  public static void removePipe(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int pipe
  ) throws IOException, SQLException {
    checkAccessPipe(conn, source, "removePipe", pipe);

    removePipe(conn, invalidateList, pipe);
  }

  public static void removePipe(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int pipe
  ) throws IOException, SQLException {
    // Get the values for later use
    Account.Name account = getAccountForPipe(conn, pipe);
    int linuxServer = getLinuxServerForPipe(conn, pipe);

    // Delete the objects that depend on this one first
    IntList addresses = conn.queryIntList("select email_address from email.\"PipeAddress\" where email_pipe=?", pipe);
    int size = addresses.size();
    boolean addressesModified = size > 0;
    for (int c = 0; c < size; c++) {
      int address = addresses.getInt(c);
      conn.update("delete from email.\"PipeAddress\" where email_address=? and email_pipe=?", address, pipe);
      if (!isAddressUsed(conn, address)) {
        conn.update("delete from email.\"Address\" where id=?", address);
      }
    }

    // Delete from the database
    conn.update("delete from email.\"Pipe\" where id=?", pipe);

    // Notify all clients of the update
    if (addressesModified) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
    invalidateList.addTable(conn, Table.TableId.EMAIL_PIPES, account, linuxServer, false);
  }

  public static void removePipeAddress(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int pipeAddress
  ) throws IOException, SQLException {
    int address = conn.queryInt("select email_address from email.\"PipeAddress\" where id=?", pipeAddress);
    checkAccessAddress(conn, source, "removePipeAddress", address);

    // Get stuff for use after the try block
    Account.Name account = getAccountForAddress(conn, address);
    int linuxServer = getLinuxServerForAddress(conn, address);

    // Delete from the database
    conn.update("delete from email.\"PipeAddress\" where id=?", pipeAddress);

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
  }

  public static void removeDomain(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int domain
  ) throws IOException, SQLException {
    checkAccessDomain(conn, source, "removeDomain", domain);

    removeDomain(conn, invalidateList, domain);
  }

  public static void removeDomain(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int domain
  ) throws IOException, SQLException {
    boolean beaMod = false;
    boolean laaMod = false;
    boolean efMod = false;
    boolean elaMod = false;
    boolean epaMod = false;
    final Account.Name account = getAccountForDomain(conn, domain);
    final int linuxServer = getLinuxServerForDomain(conn, domain);

    // Remove any majordomo server
    final int ms = conn.queryInt("select coalesce((select domain from email.\"MajordomoServer\" where domain=?), -1)", domain);
    if (ms != -1) {
      removeMajordomoServer(conn, invalidateList, domain);
    }

    // Get the list of all email addresses in the domain
    final IntList addresses = conn.queryIntList("select id from email.\"Address\" where domain=?", domain);

    final int len = addresses.size();
    final boolean eaMod = len > 0;
    for (int c = 0; c < len; c++) {
      int address = addresses.getInt(c);

      if (
          conn.queryBoolean(
              "select (select email_address from email.\"BlackholeAddress\" where email_address=?) is not null",
              address
          )
      ) {
        conn.update("delete from email.\"BlackholeAddress\" where email_address=?", address);
        beaMod = true;
      }

      // Delete any email.InboxAddress used by this email address
      IntList ids = conn.queryIntList("select id from email.\"InboxAddress\" where email_address=?", address);
      if (ids.size() > 0) {
        for (int d = 0; d < ids.size(); d++) {
          int laaPkey = ids.getInt(d);
          conn.update("update linux.\"UserServer\" set autoresponder_from=null where autoresponder_from=?", laaPkey);
          conn.update("delete from email.\"InboxAddress\" where id=?", laaPkey);
        }
        laaMod = true;
      }

      if (
          conn.queryBoolean(
              "select (select id from email.\"Forwarding\" where email_address=? limit 1) is not null",
              address
          )
      ) {
        conn.update("delete from email.\"Forwarding\" where email_address=?", address);
        efMod = true;
      }


      if (
          conn.queryBoolean(
              "select (select id from email.\"ListAddress\" where email_address=? limit 1) is not null",
              address
          )
      ) {
        conn.update("delete from email.\"ListAddress\" where email_address=?", address);
        elaMod = true;
      }

      if (
          conn.queryBoolean(
              "select (select id from email.\"PipeAddress\" where email_address=? limit 1) is not null",
              address
          )
      ) {
        conn.update("delete from email.\"PipeAddress\" where email_address=?", address);
        epaMod = true;
      }

      // Delete from the database
      conn.update("delete from email.\"Address\" where id=?", address);
    }

    // Remove the domain from the database
    conn.update("delete from email.\"Domain\" where id=?", domain);

    // Notify all clients of the update
    if (beaMod) {
      invalidateList.addTable(conn, Table.TableId.BLACKHOLE_EMAIL_ADDRESSES, account, linuxServer, false);
    }
    if (laaMod) {
      invalidateList.addTable(conn, Table.TableId.LINUX_ACC_ADDRESSES, account, linuxServer, false);
    }
    if (efMod) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_FORWARDING, account, linuxServer, false);
    }
    if (elaMod) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_LIST_ADDRESSES, account, linuxServer, false);
    }
    if (epaMod) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
    }
    if (eaMod) {
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
    invalidateList.addTable(conn, Table.TableId.EMAIL_DOMAINS, account, linuxServer, false);
  }

  /**
   * Removes a email SMTP relay.
   */
  public static void removeSmtpRelay(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int smtpRelay
  ) throws IOException, SQLException {
    checkAccessSmtpRelay(conn, source, "removeSmtpRelay", smtpRelay);

    removeSmtpRelay(conn, invalidateList, smtpRelay);
  }

  /**
   * Removes a email SMTP relay.
   */
  public static void removeSmtpRelay(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int smtpRelay
  ) throws IOException, SQLException {
    Account.Name account = getAccountForSmtpRelay(conn, smtpRelay);
    int linuxServer = getLinuxServerForSmtpRelay(conn, smtpRelay);

    conn.update(
        "delete from email.\"SmtpRelay\" where id=?",
        smtpRelay
    );

    // Notify all clients of the update
    invalidateList.addTable(conn, Table.TableId.EMAIL_SMTP_RELAYS, account, linuxServer, false);
  }

  public static void removeMajordomoServer(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int majordomoServer
  ) throws IOException, SQLException {
    checkAccessMajordomoServer(conn, source, "removeMajordomoServer", majordomoServer);

    removeMajordomoServer(conn, invalidateList, majordomoServer);
  }

  public static void removeMajordomoServer(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int majordomoServer
  ) throws IOException, SQLException {
    final Account.Name account = getAccountForDomain(conn, majordomoServer);
    final int linuxServer = getLinuxServerForDomain(conn, majordomoServer);

    // Remove any majordomo lists
    final IntList mls = conn.queryIntList("select email_list from email.\"MajordomoList\" where majordomo_server=?", majordomoServer);
    if (mls.size() > 0) {
      for (int c = 0; c < mls.size(); c++) {
        removeList(conn, invalidateList, mls.getInt(c));
      }
    }

    // Get the majordomo_pipe_address and details
    final int epa = conn.queryInt("select majordomo_pipe_address from email.\"MajordomoServer\" where domain=?", majordomoServer);
    final int ea = conn.queryInt("select email_address from email.\"PipeAddress\" where id=?", epa);
    final int ep = conn.queryInt("select email_pipe from email.\"PipeAddress\" where id=?", epa);

    // Get the other email addresses referenced
    final int omAddress = conn.queryInt("select owner_majordomo_add from email.\"MajordomoServer\" where domain=?", majordomoServer);
    final int moAddress = conn.queryInt("select majordomo_owner_add from email.\"MajordomoServer\" where domain=?", majordomoServer);

    // Remove the domain from the database
    conn.update("delete from email.\"MajordomoServer\" where domain=?", majordomoServer);
    invalidateList.addTable(conn, Table.TableId.MAJORDOMO_SERVERS, account, linuxServer, false);

    // Remove the majordomo pipe and address
    conn.update("delete from email.\"PipeAddress\" where id=?", epa);
    invalidateList.addTable(conn, Table.TableId.EMAIL_PIPE_ADDRESSES, account, linuxServer, false);
    if (!isAddressUsed(conn, ea)) {
      conn.update("delete from email.\"Address\" where id=?", ea);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
    conn.update("delete from email.\"Pipe\" where id=?", ep);
    invalidateList.addTable(conn, Table.TableId.EMAIL_PIPES, account, linuxServer, false);

    // Remove the referenced email addresses if not used
    if (!isAddressUsed(conn, omAddress)) {
      conn.update("delete from email.\"Address\" where id=?", omAddress);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
    if (!isAddressUsed(conn, moAddress)) {
      conn.update("delete from email.\"Address\" where id=?", moAddress);
      invalidateList.addTable(conn, Table.TableId.EMAIL_ADDRESSES, account, linuxServer, false);
    }
  }

  public static void setListFile(
      DatabaseConnection conn,
      RequestSource source,
      int list,
      String addresses
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "setListFile", list);

    PosixPath path = getPathForList(conn, list);
    int uid = LinuxAccountHandler.getUidForUserServer(conn, getLinuxUserServerForList(conn, list));
    int gid = LinuxAccountHandler.getGidForGroupServer(conn, getLinuxGroupServerForList(conn, list));
    int mode = isMajordomoList(conn, list) ? 0644 : 0640;
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
        conn,
        getLinuxServerForList(conn, list)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setEmailListFile(path, addresses, uid, gid, mode);
  }

  public static Account.Name getAccountForAddress(DatabaseConnection conn, int address) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from email.\"Address\" ea, email.\"Domain\" sd, billing.\"Package\" pk where ea.domain=sd.id and sd.package=pk.name and ea.id=?",
        address
    );
  }

  public static Account.Name getAccountForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from email.\"List\" el, linux.\"GroupServer\" lsg, linux.\"Group\" lg, billing.\"Package\" pk"
            + " where el.linux_server_group=lsg.id and lsg.name=lg.name and lg.package=pk.name and el.id=?",
        list
    );
  }

  public static Account.Name getAccountForPipe(DatabaseConnection conn, int pipe) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from email.\"Pipe\" ep, billing.\"Package\" pk where ep.package=pk.name and ep.id=?",
        pipe
    );
  }

  public static Account.Name getAccountForDomain(DatabaseConnection conn, int domain) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from email.\"Domain\" sd, billing.\"Package\" pk where sd.package=pk.name and sd.id=?",
        domain
    );
  }

  public static DomainName getNetDomainForDomain(DatabaseConnection conn, int domain) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.domainNameFactory,
        "select domain from email.\"Domain\" where id=?",
        domain
    );
  }

  public static Account.Name getAccountForSmtpRelay(DatabaseConnection conn, int smtpRelay) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select pk.accounting from email.\"SmtpRelay\" esr, billing.\"Package\" pk where esr.package=pk.name and esr.id=?",
        smtpRelay
    );
  }

  public static int getAddress(DatabaseConnection conn, String address, int domain) throws IOException, SQLException {
    return conn.queryInt(
        "select coalesce((select id from email.\"Address\" where address=? and domain=?), -1)",
        address,
        domain
    );
  }

  public static int getOrAddAddress(DatabaseConnection conn, InvalidateList invalidateList, String address, int domain) throws IOException, SQLException {
    int address_id = getAddress(conn, address, domain);
    if (address_id == -1) {
      address_id = addAddress0(conn, invalidateList, address, domain);
    }
    return address_id;
  }

  public static int getLinuxUserServerForMajordomoServer(DatabaseConnection conn, int majordomoServer) throws IOException, SQLException {
    return conn.queryInt("select linux_server_account from email.\"MajordomoServer\" where domain=?", majordomoServer);
  }

  public static int getLinuxGroupServerForMajordomoServer(DatabaseConnection conn, int majordomoServer) throws IOException, SQLException {
    return conn.queryInt("select linux_server_group from email.\"MajordomoServer\" where domain=?", majordomoServer);
  }

  public static Account.Name getPackageForDomain(DatabaseConnection conn, int domain) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select package from email.\"Domain\" where id=?",
        domain
    );
  }

  public static Account.Name getPackageForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  lg.package\n"
            + "from\n"
            + "  email.\"List\" el,\n"
            + "  linux.\"GroupServer\" lsg,\n"
            + "  linux.\"Group\" lg\n"
            + "where\n"
            + "  el.id=?\n"
            + "  and el.linux_server_group=lsg.id\n"
            + "  and lsg.name=lg.name",
        list
    );
  }

  public static Account.Name getPackageForPipe(DatabaseConnection conn, int pipe) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select package from email.\"Pipe\" where id=?",
        pipe
    );
  }

  public static Account.Name getPackageForSmtpRelay(DatabaseConnection conn, int smtpRelay) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select package from email.\"SmtpRelay\" where id=?",
        smtpRelay
    );
  }

  public static PosixPath getPathForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.posixPathFactory,
        "select path from email.\"List\" where id=?",
        list
    );
  }

  public static int getLinuxServerForAddress(DatabaseConnection conn, int address) throws IOException, SQLException {
    return conn.queryInt("select ed.ao_server from email.\"Address\" ea, email.\"Domain\" ed where ea.domain=ed.id and ea.id=?", address);
  }

  public static int getLinuxServerForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  lsg.ao_server\n"
            + "from\n"
            + "  email.\"List\" el,\n"
            + "  linux.\"GroupServer\" lsg\n"
            + "where\n"
            + "  el.id=?\n"
            + "  and el.linux_server_group=lsg.id",
        list
    );
  }

  public static int getLinuxUserServerForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryInt("select linux_server_account from email.\"List\" where id=?", list);
  }

  public static int getLinuxGroupServerForList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryInt("select linux_server_group from email.\"List\" where id=?", list);
  }

  public static boolean isAddressUsed(DatabaseConnection conn, int address) throws IOException, SQLException {
    return
        conn.queryBoolean("select (select email_address from email.\"BlackholeAddress\" where email_address=? limit 1) is not null", address)
            || conn.queryBoolean("select (select id from email.\"Forwarding\" where email_address=? limit 1) is not null", address)
            || conn.queryBoolean("select (select id from email.\"ListAddress\" where email_address=? limit 1) is not null", address)
            || conn.queryBoolean("select (select id from email.\"PipeAddress\" where email_address=? limit 1) is not null", address)
            || conn.queryBoolean("select (select id from email.\"InboxAddress\" where email_address=? limit 1) is not null", address)
            || conn.queryBoolean(
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
                + "      ml.listname_pipe_add=epa1.id\n"
                + "      and ml.listname_list_add=ela.id\n"
                + "      and ml.listname_request_pipe_add=epa2.id\n"
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
            address,
            address,
            address,
            address,
            address,
            address
        ) || conn.queryBoolean(
            "select\n"
                + "  (\n"
                + "    select\n"
                + "      ms.domain\n"
                + "    from\n"
                + "      email.\"MajordomoServer\" ms,\n"
                + "      email.\"PipeAddress\" epa\n"
                + "    where\n"
                + "      ms.majordomo_pipe_address=epa.id\n"
                + "      and (\n"
                + "        epa.email_address=?\n"
                + "        or ms.owner_majordomo_add=?\n"
                + "        or ms.majordomo_owner_add=?\n"
                + "      )\n"
                + "    limit 1\n"
                + "  ) is not null",
            address,
            address,
            address
        );
  }

  public static int getLinuxServerForPipe(DatabaseConnection conn, int pipe) throws IOException, SQLException {
    return conn.queryInt("select ao_server from email.\"Pipe\" where id=?", pipe);
  }

  public static int getLinuxServerForDomain(DatabaseConnection conn, int domain) throws IOException, SQLException {
    return conn.queryInt("select ao_server from email.\"Domain\" where id=?", domain);
  }

  public static int getLinuxServerForSmtpRelay(DatabaseConnection conn, int smtpRelay) throws IOException, SQLException {
    return conn.queryInt("select ao_server from email.\"SmtpRelay\" where id=?", smtpRelay);
  }

  public static boolean isDomainAvailable(DatabaseConnection conn, RequestSource source, int linuxServer, DomainName netDomain) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "isEmailDomainAvailable", linuxServer);

    return conn.queryBoolean(
        "select (select id from email.\"Domain\" where ao_server=? and domain=?) is null",
        linuxServer,
        netDomain
    );
  }

  public static boolean isMajordomoList(DatabaseConnection conn, int list) throws IOException, SQLException {
    return conn.queryBoolean("select (select email_list from email.\"MajordomoList\" where email_list=?) is not null", list);
  }

  public static void setMajordomoInfoFile(
      DatabaseConnection conn,
      RequestSource source,
      int majordomoList,
      String file
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "setMajordomoInfoFile", majordomoList);
    PosixPath infoPath;
    try {
      infoPath = PosixPath.valueOf(getPathForList(conn, majordomoList) + ".info");
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    int uid = LinuxAccountHandler.getUidForUserServer(conn, getLinuxUserServerForList(conn, majordomoList));
    int gid = LinuxAccountHandler.getGidForGroupServer(conn, getLinuxGroupServerForList(conn, majordomoList));
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn,
        getLinuxServerForList(conn, majordomoList)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setEmailListFile(infoPath, file, uid, gid, 0664);
  }

  public static void setMajordomoIntroFile(
      DatabaseConnection conn,
      RequestSource source,
      int majordomoList,
      String file
  ) throws IOException, SQLException {
    checkAccessList(conn, source, "setMajordomoIntroFile", majordomoList);
    PosixPath introPath;
    try {
      introPath = PosixPath.valueOf(getPathForList(conn, majordomoList) + ".intro");
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    int uid = LinuxAccountHandler.getUidForUserServer(conn, getLinuxUserServerForList(conn, majordomoList));
    int gid = LinuxAccountHandler.getGidForGroupServer(conn, getLinuxGroupServerForList(conn, majordomoList));
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn,
        getLinuxServerForList(conn, majordomoList)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.setEmailListFile(introPath, file, uid, gid, 0664);
  }
}
