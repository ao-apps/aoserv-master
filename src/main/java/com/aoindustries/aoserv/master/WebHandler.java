/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2025  AO Industries, Inc.
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
import com.aoapps.collections.SortedArrayList;
import com.aoapps.collections.SortedIntArrayList;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseAccess.Null;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.util.ErrorPrinter;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoapps.net.Email;
import com.aoapps.net.Port;
import com.aoapps.security.Identifier;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.distribution.OperatingSystemVersion;
import com.aoindustries.aoserv.client.distribution.Software;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.net.AppProtocol;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.Location;
import com.aoindustries.aoserv.client.web.Site;
import com.aoindustries.aoserv.client.web.tomcat.Context;
import com.aoindustries.aoserv.client.web.tomcat.JkMount;
import com.aoindustries.aoserv.client.web.tomcat.JkProtocol;
import com.aoindustries.aoserv.client.web.tomcat.PrivateTomcatSite;
import com.aoindustries.aoserv.client.web.tomcat.SharedTomcat;
import com.aoindustries.aoserv.client.web.tomcat.Version;
import com.aoindustries.aoserv.daemon.client.AoservDaemonConnector;
import com.aoindustries.aoserv.master.dns.DnsService;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The <code>HttpdHandler</code> handles all the accesses to the HTTPD tables.
 *
 * @author  AO Industries, Inc.
 */
public final class WebHandler {

  /** Make no instances. */
  private WebHandler() {
    throw new AssertionError();
  }

  /**
   * The first port number that may be used for automatic port allocations.
   */
  public static final int MINIMUM_AUTO_PORT_NUMBER = 16384;

  private static final Map<Integer, Boolean> disabledSharedTomcats = new HashMap<>();
  private static final Map<Integer, Boolean> disabledVirtualHosts = new HashMap<>();
  private static final Map<Integer, Boolean> disabledSites = new HashMap<>();

  public static void addTomcatWorker(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int bind,
      int site
  ) throws IOException, SQLException {
    int host = NetBindHandler.getHostForBind(conn, bind);
    if (!NetHostHandler.isLinuxServer(conn, host)) {
      throw new SQLException("Host is not a Linux server: " + host);
    }
    int linusServer = host;
    if (site == -1) {
      conn.update(
          "INSERT INTO\n"
              + "  \"web.tomcat\".\"Worker\"\n"
              + "VALUES (\n"
              + "  ?,\n"
              + "  (\n"
              + "    select\n"
              + "      hjc.code\n"
              + "    from\n"
              + "      \"web.tomcat\".\"WorkerName\" hjc\n"
              + "    where\n"
              + "      (\n"
              + "        select\n"
              + "          hw.\"name\"\n"
              + "        from\n"
              + "          \"web.tomcat\".\"Worker\" hw,\n"
              + "          net.\"Bind\" nb\n"
              + "        where\n"
              + "          hw.bind=nb.id\n"
              + "          and nb.server=?\n"
              + "          and hjc.code=hw.\"name\"\n"
              + "        limit 1\n"
              + "      ) is null\n"
              + "    order by\n"
              + "      code\n"
              + "    limit 1\n"
              + "  ),\n"
              + "  null\n"
              + ")",
          bind,
          linusServer
      );
    } else {
      conn.update(
          "INSERT INTO\n"
              + "  \"web.tomcat\".\"Worker\"\n"
              + "VALUES (\n"
              + "  ?,\n"
              + "  (\n"
              + "    select\n"
              + "      hjc.code\n"
              + "    from\n"
              + "      \"web.tomcat\".\"WorkerName\" hjc\n"
              + "    where\n"
              + "      (\n"
              + "        select\n"
              + "          hw.\"name\"\n"
              + "        from\n"
              + "          \"web.tomcat\".\"Worker\" hw,\n"
              + "          net.\"Bind\" nb\n"
              + "        where\n"
              + "          hw.bind=nb.id\n"
              + "          and nb.server=?\n"
              + "          and hjc.code=hw.\"name\"\n"
              + "        limit 1\n"
              + "      ) is null\n"
              + "    order by\n"
              + "      code\n"
              + "    limit 1\n"
              + "  ),\n"
              + "  ?\n"
              + ")",
          bind,
          linusServer,
          site
      );
    }
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_WORKERS,
        NetBindHandler.getAccountForBind(conn, bind),
        linusServer,
        false
    );
  }

  public static int addVirtualHostName(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHost,
      DomainName hostname
  ) throws IOException, SQLException {
    int site = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "addVirtualHostName", site);
    if (isVirtualHostDisabled(conn, virtualHost)) {
      throw new SQLException("Unable to add VirtualHostName, VirtualHost disabled: " + virtualHost);
    }
    AoservMaster.checkAccessHostname(conn, source, "addVirtualHostName", hostname.toString());

    int virtualHostName = conn.updateInt(
        "INSERT INTO\n"
            + "  web.\"VirtualHostName\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  (select id from web.\"VirtualHostName\" where httpd_site_bind=? and is_primary limit 1) is null\n"
            + ") RETURNING id",
        virtualHost,
        hostname,
        virtualHost
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITE_URLS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );

    return virtualHostName;
  }

  public static void checkAccessSharedTomcat(DatabaseConnection conn, RequestSource source, String action, int sharedTomcat) throws IOException, SQLException {
    if (
        !LinuxAccountHandler.canAccessGroupServer(
            conn,
            source,
            conn.queryInt("select linux_server_group from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat)
        )
    ) {
      String message =
          "currentAdministrator="
              + source.getCurrentAdministrator()
              + " is not allowed to access httpd_shared_tomcat: action='"
              + action
              + "', id="
              + sharedTomcat;
      throw new SQLException(message);
    }
  }

  public static boolean canAccessSite(DatabaseConnection conn, RequestSource source, int site) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        return NetHostHandler.canAccessHost(conn, source, getLinuxServerForSite(conn, site));
      } else {
        return true;
      }
    } else {
      return PackageHandler.canAccessPackage(conn, source, getPackageForSite(conn, site));
    }
  }

  public static void checkAccessHttpdServer(DatabaseConnection conn, RequestSource source, String action, int httpdServer) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForHttpdServer(conn, httpdServer));
      }
    } else {
      PackageHandler.checkAccessPackage(conn, source, action, getPackageForHttpdServer(conn, httpdServer));
    }
  }

  public static void checkAccessSite(DatabaseConnection conn, RequestSource source, String action, int site) throws IOException, SQLException {
    User mu = AoservMaster.getUser(conn, source.getCurrentAdministrator());
    if (mu != null) {
      if (AoservMaster.getUserHosts(conn, source.getCurrentAdministrator()).length != 0) {
        NetHostHandler.checkAccessHost(conn, source, action, getLinuxServerForSite(conn, site));
      }
    } else {
      PackageHandler.checkAccessPackage(conn, source, action, getPackageForSite(conn, site));
    }
  }

  public static int getHttpdServerConcurrency(
      DatabaseConnection conn,
      RequestSource source,
      int httpdServer
  ) throws IOException, SQLException {
    checkAccessHttpdServer(conn, source, "getHttpdServerConcurrency", httpdServer);

    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(
        conn,
        getLinuxServerForHttpdServer(conn, httpdServer)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.getHttpdServerConcurrency(httpdServer);
  }

  /**
   * Gets the id of a Site given its server and name or {@code -1} if not found.
   */
  public static int getSite(DatabaseConnection conn, int linuxServer, String name) throws IOException, SQLException {
    return conn.queryInt(
        "select coalesce(\n"
            + "  (select id from web.\"Site\" where (ao_server, \"name\")=(?,?)),\n"
            + "  -1\n"
            + ")",
        linuxServer,
        name
    );
  }

  /**
   * Gets the id of a SharedTomcat given its server and name or {@code -1} if not found.
   */
  public static int getSharedTomcat(DatabaseConnection conn, int linuxServer, String name) throws IOException, SQLException {
    return conn.queryInt(
        "select coalesce(\n"
            + "  (select id from \"web.tomcat\".\"SharedTomcat\" where (ao_server, name)=(?,?)),\n"
            + "  -1\n"
            + ")",
        linuxServer,
        name
    );
  }

  public static int addLocation(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      String path,
      boolean isRegularExpression,
      String authName,
      PosixPath authGroupFile,
      PosixPath authUserFile,
      String require,
      String handler
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "addLocation", site);
    if (isSiteDisabled(conn, site)) {
      throw new SQLException("Unable to add Location, Site disabled: " + site);
    }
    String error = Location.validatePath(path);
    if (error == null) {
      error = Location.validateAuthName(authName);
    }
    if (error == null) {
      error = Location.validateAuthGroupFile(authGroupFile);
    }
    if (error == null) {
      error = Location.validateAuthUserFile(authUserFile);
    }
    if (error == null) {
      error = Location.validateRequire(require);
    }
    if (error != null) {
      throw new SQLException("Unable to add Location: " + error);
    }

    int location = conn.updateInt(
        "INSERT INTO\n"
            + "  web.\"Location\"\n"
            + "VALUES (\n"
            + "  default,\n" // id
            + "  ?,\n" // httpd_site
            + "  ?,\n" // path
            + "  ?,\n" // is_regular_expression
            + "  ?,\n" // auth_name
            + "  ?,\n" // auth_group_file
            + "  ?,\n" // auth_user_file
            + "  ?,\n" // require
            + "  ?\n"  // handler
            + ") RETURNING id",
        site,
        path,
        isRegularExpression,
        authName,
        authGroupFile == null ? "" : authGroupFile.toString(),
        authUserFile == null ? "" : authUserFile.toString(),
        require,
        handler
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );

    return location;
  }

  /**
   * Checks a JkMount/JkUnMount path for validity, throwing SQLException if invalid.
   *
   * @see  JkMount#isValidPath(java.lang.String)
   */
  public static String checkJkMountPath(String path) throws SQLException {
    if (!JkMount.isValidPath(path)) {
      throw new SQLException("Invalid path: " + path);
    }
    return path;
  }

  public static int addContext(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int tomcatSite,
      String className,
      boolean cookies,
      boolean crossContext,
      PosixPath docBase,
      boolean override,
      String path,
      boolean privileged,
      boolean reloadable,
      boolean useNaming,
      String wrapperClass,
      int debug,
      PosixPath workDir,
      boolean serverXmlConfigured
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "addHttpdTomcatContext", tomcatSite);
    if (isSiteDisabled(conn, tomcatSite)) {
      throw new SQLException("Unable to add Context, Site disabled: " + tomcatSite);
    }
    checkContext(
        conn,
        source,
        tomcatSite,
        className,
        crossContext,
        docBase,
        override,
        path,
        privileged,
        wrapperClass,
        workDir,
        serverXmlConfigured
    );

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    int context = conn.updateInt(
        "INSERT INTO\n"
            + "  \"web.tomcat\".\"Context\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?\n"
            + ") RETURNING id",
        tomcatSite,
        className,
        cookies,
        crossContext,
        docBase.toString(),
        override,
        path,
        privileged,
        reloadable,
        useNaming,
        wrapperClass,
        debug,
        Objects.toString(workDir, null),
        serverXmlConfigured
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_CONTEXTS,
        account,
        linuxServer,
        false
    );

    // Initial HttpdTomcatSiteJkMounts
    boolean useApache = conn.queryBoolean(
        "select (\n"
            + "  select id from \"web.tomcat\".\"JkMount\"\n"
            + "  where (httpd_tomcat_site, path)=(?, '/*')\n"
            + ") is null",
        tomcatSite
    );

    if (useApache) {
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          tomcatSite,
          checkJkMountPath(path + "/j_security_check")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          tomcatSite,
          checkJkMountPath(path + "/servlet/*")
      );
      invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS, account, linuxServer, false);
    } else {
      boolean enableCgi = conn.queryBoolean("select enable_cgi from web.\"Site\" where id=?", tomcatSite);
      if (enableCgi) {
        conn.update(
            "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
            tomcatSite,
            checkJkMountPath(path + "/cgi-bin/*")
        );
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS, account, linuxServer, false);
      }
    }

    return context;
  }

  public static int addContextDataSource(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int context,
      String name,
      String driverClassName,
      String url,
      String username,
      String password,
      int maxActive,
      int maxIdle,
      int maxWait,
      String validationQuery
  ) throws IOException, SQLException {
    int tomcat_site = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "addContextDataSource", tomcat_site);
    if (isSiteDisabled(conn, tomcat_site)) {
      throw new SQLException("Unable to add ContextDataSource, Site disabled: " + tomcat_site);
    }

    int contextDataSource = conn.updateInt(
        "INSERT INTO\n"
            + "  \"web.tomcat\".\"ContextDataSource\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?\n"
            + ") RETURNING id",
        context,
        name,
        driverClassName,
        url,
        username,
        password,
        maxActive,
        maxIdle,
        maxWait,
        validationQuery
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_DATA_SOURCES,
        getAccountForSite(conn, tomcat_site),
        getLinuxServerForSite(conn, tomcat_site),
        false
    );

    return contextDataSource;
  }

  public static int addContextParameter(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int context,
      String name,
      String value,
      boolean override,
      String description
  ) throws IOException, SQLException {
    int tomcat_site = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "addContextParameter", tomcat_site);
    if (isSiteDisabled(conn, tomcat_site)) {
      throw new SQLException("Unable to add ContextParameter, Site disabled: " + tomcat_site);
    }

    int contextParameter = conn.updateInt(
        "INSERT INTO\n"
            + "  \"web.tomcat\".\"ContextParameter\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?,\n"
            + "  ?\n"
            + ") RETURNING id",
        context,
        name,
        value,
        override,
        description
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_PARAMETERS,
        getAccountForSite(conn, tomcat_site),
        getLinuxServerForSite(conn, tomcat_site),
        false
    );

    return contextParameter;
  }

  public static int addJkMount(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int tomcatSite,
      String path,
      boolean mount
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "addHttpdTomcatSiteJkMount", tomcatSite);

    int jkMount = conn.updateInt(
        "INSERT INTO \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, \"path\", mount) VALUES (?,?,?) RETURNING id",
        tomcatSite,
        checkJkMountPath(path),
        mount
    );

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
        getAccountForSite(conn, tomcatSite),
        getLinuxServerForSite(conn, tomcatSite),
        false
    );

    return jkMount;
  }

  public static void removeJkMount(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int jkMount
  ) throws IOException, SQLException {
    int tomcatSite = conn.queryInt("select httpd_tomcat_site from \"web.tomcat\".\"JkMount\" where id=?", jkMount);
    checkAccessSite(conn, source, "removeJkMount", tomcatSite);

    conn.update("delete from \"web.tomcat\".\"JkMount\" where id=?", jkMount);

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
        getAccountForSite(conn, tomcatSite),
        getLinuxServerForSite(conn, tomcatSite),
        false
    );
  }

  public static void checkContext(
      DatabaseConnection conn,
      RequestSource source,
      int tomcatSite,
      String className,
      boolean crossContext,
      PosixPath docBase,
      boolean override,
      String path,
      boolean privileged,
      String wrapperClass,
      PosixPath workDir,
      boolean serverXmlConfigured
  ) throws IOException, SQLException {
    if (!Context.isValidDocBase(docBase)) {
      throw new SQLException("Invalid docBase: " + docBase);
    }
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    // OperatingSystem settings
    int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    PosixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
    PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

    String docBaseStr = docBase.toString();
    if (docBaseStr.startsWith("/home/")) {
      // Must be able to access one of the linux.UserServer with that home directory
      //
      // This means there must be an accessible account that has a home directory that is a prefix of this docbase.
      // Such as /home/e/example/ being a prefix of /home/e/example/my-webapp
      IntList lsas = conn.queryIntList(
          "select\n"
              + "  id\n"
              + "from\n"
              + "  linux.\"UserServer\"\n"
              + "where\n"
              + "  ao_server=?\n"
              + "  and (home || '/')=substring(? from 1 for (length(home) + 1))",
          linuxServer,
          docBaseStr
      );
      boolean found = false;
      for (int c = 0; c < lsas.size(); c++) {
        if (LinuxAccountHandler.canAccessUserServer(conn, source, lsas.getInt(c))) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new SQLException("Home directory not allowed for path: " + docBaseStr);
      }
    } else if (docBaseStr.startsWith(httpdSitesDir + "/")) {
      int slashPos = docBaseStr.indexOf('/', httpdSitesDir.toString().length() + 1);
      if (slashPos == -1) {
        slashPos = docBaseStr.length();
      }
      String siteName = docBaseStr.substring(httpdSitesDir.toString().length() + 1, slashPos);
      int hs = conn.queryInt("select id from web.\"Site\" where ao_server=? and \"name\"=?", linuxServer, siteName);
      WebHandler.checkAccessSite(conn, source, "addCvsRepository", hs);
    } else if (docBaseStr.startsWith(httpdSharedTomcatsDir + "/")) {
      int slashPos = docBaseStr.indexOf('/', httpdSharedTomcatsDir.toString().length() + 1);
      if (slashPos == -1) {
        slashPos = docBaseStr.length();
      }
      String tomcatName = docBaseStr.substring(httpdSharedTomcatsDir.toString().length() + 1, slashPos);
      int groupUser = conn.queryInt("select linux_server_account from \"web.tomcat\".\"SharedTomcat\" where name=? and ao_server=?", tomcatName, linuxServer);
      LinuxAccountHandler.checkAccessUserServer(conn, source, "addCvsRepository", groupUser);
    } else {
      // Allow the example directories
      List<PosixPath> tomcats = conn.queryList(
          ObjectFactories.posixPathFactory,
          "select\n"
              + "  htv.install_dir || '/webapps/examples'\n"
              + "from\n"
              + "  \"web.tomcat\".\"Site\" hts\n"
              + "  inner join \"web.tomcat\".\"Version\" htv on hts.version=htv.version\n"
              + "where\n"
              + "  hts.httpd_site=?\n"
              + "union select\n"
              + "  htv.install_dir || '/webapps/manager'\n"
              + "from\n"
              + "  \"web.tomcat\".\"Site\" hts\n"
              + "  inner join \"web.tomcat\".\"Version\" htv on hts.version=htv.version\n"
              + "where\n"
              + "  hts.httpd_site=?\n",
          tomcatSite,
          tomcatSite
      );
      boolean found = false;
      for (PosixPath tomcat : tomcats) {
        if (docBase.equals(tomcat)) {
          found = true;
          break;
        }
      }
      // TODO: It would be better to make sure the
      if (!found) {
        throw new SQLException("Invalid docBase: " + docBase);
      }
    }

    if (!Context.isValidPath(path)) {
      throw new SQLException("Invalid path: " + path);
    }
    if (!Context.isValidWorkDir(workDir)) {
      throw new SQLException("Invalid workDir: " + workDir);
    }
  }

  /**
   * Creates a new Tomcat site with the standard configuration.
   */
  public static int addJbossSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int linuxServer,
      String siteName,
      Account.Name packageName,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Group.Name group,
      Email serverAdmin,
      boolean useApache,
      int ipAddress,
      DomainName primaryHttpHostname,
      DomainName[] altHttpHostnames,
      int jbossVersion,
      int phpVersion,
      boolean enableCgi,
      boolean enableSsi,
      boolean enableHtaccess,
      boolean enableIndexes,
      boolean enableFollowSymlinks
  ) throws IOException, SQLException {
    return addHttpdJvmSite(
        "addJbossSite",
        conn,
        source,
        invalidateList,
        linuxServer,
        siteName,
        packageName,
        user,
        group,
        serverAdmin,
        useApache,
        ipAddress,
        primaryHttpHostname,
        altHttpHostnames,
        "jboss",
        jbossVersion,
        -1,
        "",
        phpVersion,
        enableCgi,
        enableSsi,
        enableHtaccess,
        enableIndexes,
        enableFollowSymlinks
    );
  }

  private static int addHttpdJvmSite(
      String methodName,
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int linuxServer,
      String siteName,
      Account.Name packageName,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Group.Name group,
      Email serverAdmin,
      boolean useApache,
      int ipAddress,
      DomainName primaryHttpHostname,
      DomainName[] altHttpHostnames,
      String siteType,
      int jbossVersion,
      int tomcatVersion,
      String sharedTomcatName,
      int phpVersion,
      boolean enableCgi,
      boolean enableSsi,
      boolean enableHtaccess,
      boolean enableIndexes,
      boolean enableFollowSymlinks
  ) throws IOException, SQLException {
    // Perform the security checks on the input
    NetHostHandler.checkAccessHost(conn, source, methodName, linuxServer);
    if (!Site.isValidSiteName(siteName)) {
      throw new SQLException("Invalid site name: " + siteName);
    }
    PackageHandler.checkAccessPackage(conn, source, methodName, packageName);
    if (PackageHandler.isPackageDisabled(conn, packageName)) {
      throw new SQLException("Unable to " + methodName + ", Package disabled: " + packageName);
    }
    LinuxAccountHandler.checkAccessUser(conn, source, methodName, user);
    int lsa = LinuxAccountHandler.getUserServer(conn, user, linuxServer);
    if (LinuxAccountHandler.isUserServerDisabled(conn, lsa)) {
      throw new SQLException("Unable to " + methodName + ", UserServer disabled: " + lsa);
    }
    LinuxAccountHandler.checkAccessGroup(conn, source, methodName, group);
    int lsg = LinuxAccountHandler.getGroupServer(conn, group, linuxServer);
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to " + methodName + " for user '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }
    LinuxAccountHandler.checkAccessGroup(conn, source, methodName, group);
    final int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    if (osv == -1) {
      throw new SQLException("Unknown operating system version for server #" + linuxServer);
    }
    if (
        group.equals(Group.FTPONLY)
            || group.equals(Group.MAIL)
            || group.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to " + methodName + " for group '" + group + '\'');
    }
    int sharedTomcatPkey = 0;
    if ("jboss".equals(siteType)) {
      if (tomcatVersion != -1) {
        throw new SQLException("TomcatVersion cannot be supplied for a JBoss site: " + tomcatVersion);
      }
      tomcatVersion = conn.queryInt("select tomcat_version from \"web.jboss\".\"Version\" where version=?", jbossVersion);
    } else if ("tomcat_shared".equals(siteType)) {
      // Get shared Tomcat id
      sharedTomcatPkey = conn.queryInt(
          "select id from \"web.tomcat\".\"SharedTomcat\" where ao_server=? and name=?",
          linuxServer,
          sharedTomcatName
      );

      // Check for ties between jvm and site in linux.GroupUser
      String sharedTomcat_username = conn.queryString("select lsa.username from \"web.tomcat\".\"SharedTomcat\" hst, linux.\"UserServer\" lsa"
          + " where hst.linux_server_account = lsa.id and hst.id=?", sharedTomcatPkey);
      String sharedTomcat_linuxGroup = conn.queryString("select lsg.name from \"web.tomcat\".\"SharedTomcat\" hst, linux.\"GroupServer\" lsg"
          + " where hst.linux_server_group = lsg.id and hst.id=?", sharedTomcatPkey);
      boolean hasAccess = conn.queryBoolean(
          "select (\n"
              + "  select\n"
              + "    id\n"
              + "  from\n"
              + "    linux.\"GroupUser\"\n"
              + "  where\n"
              + "    \"group\"=?\n"
              + "    and \"user\"=?\n"
              + "    and (\n"
              + "      \"operatingSystemVersion\" is null\n"
              + "      or \"operatingSystemVersion\"=?\n"
              + "    )\n"
              + ") is not null",
          sharedTomcat_linuxGroup,
          user,
          osv
      );
      if (!hasAccess) {
        throw new SQLException("linux.User (" + user + ") does not have access to linux.Group (" + sharedTomcat_linuxGroup + ")");
      }
      hasAccess = conn.queryBoolean(
          "select (\n"
              + "  select\n"
              + "    id\n"
              + "  from\n"
              + "    linux.\"GroupUser\"\n"
              + "  where\n"
              + "    \"group\"=?\n"
              + "    and \"user\"=?\n"
              + "    and (\n"
              + "      \"operatingSystemVersion\" is null\n"
              + "      or \"operatingSystemVersion\"=?\n"
              + "    )\n"
              + ") is not null",
          group,
          sharedTomcat_username,
          osv
      );
      if (!hasAccess) {
        throw new SQLException("linux.User (" + sharedTomcatName + ") does not have access to linux.Group (" + group + ")");
      }

      if (tomcatVersion != -1) {
        throw new SQLException("TomcatVersion cannot be supplied for a TomcatShared site: " + tomcatVersion);
      }
      tomcatVersion = conn.queryInt("select version from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcatPkey);
    }
    final String tomcatVersionStr = conn.queryString("select version from distribution.\"SoftwareVersion\" where id=?", tomcatVersion);
    final boolean isTomcat4 =
        !tomcatVersionStr.equals(Version.VERSION_3_1)
            && !tomcatVersionStr.equals(Version.VERSION_3_2_4);
    if (ipAddress != -1) {
      IpAddressHandler.checkAccessIpAddress(conn, source, methodName, ipAddress);
      // The IP must be on the provided server
      int ipHost = IpAddressHandler.getHostForIpAddress(conn, ipAddress);
      if (ipHost != linuxServer) {
        throw new SQLException("IP address " + ipAddress + " is not hosted on Server #" + linuxServer);
      }
    } else {
      ipAddress = IpAddressHandler.getSharedHttpdIpAddress(conn, linuxServer);
      if (ipAddress == -1) {
        throw new SQLException("Unable to find shared IP address for Server #" + linuxServer);
      }
    }
    if (phpVersion != -1) {
      // Version must be correct for this server
      int tvOsv = conn.queryInt(
          "select coalesce(\n"
              + "  (select operating_system_version from distribution.\"SoftwareVersion\" where id=? and name=?),\n"
              + "  -1\n"
              + ")",
          phpVersion,
          Software.PHP
      );
      if (tvOsv == -1) {
        throw new SQLException("Requested PHP version is not a PHP version: #" + phpVersion);
      }
      if (tvOsv != osv) {
        throw new SQLException("Requested PHP version is for the wrong operating system version: #" + phpVersion + ": " + tvOsv + " != " + osv);
      }
    }

    PackageHandler.checkPackageAccessHost(conn, source, methodName, packageName, linuxServer);

    final Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);

    final Port httpPort;
    try {
      httpPort = Port.valueOf(80, com.aoapps.net.Protocol.TCP);
    } catch (ValidationException e) {
      throw new SQLException(e);
    }

    final List<DomainName> tlds = AoservMaster.getService(DnsService.class).getTopLevelDomains(conn);
    //DomainName testUrl;
    //try {
    //  testUrl = DomainName.valueOf(siteName + "." + ServerHandler.getHostnameForLinuxServer(conn, linuxServer));
    //} catch (ValidationException e) {
    //  throw new SQLException(e);
    //}
    //DNSHandler.addDnsRecord(
    //  conn,
    //  invalidateList,
    //  testUrl,
    //  IpAddressHandler.getInetAddressForIpAddress(conn, ipAddress),
    //  tlds
    //);

    // Finish up the security checks with the Connection
    AoservMaster.checkAccessHostname(conn, source, methodName, primaryHttpHostname.toString(), tlds);
    for (DomainName altHttpHostname : altHttpHostnames) {
      AoservMaster.checkAccessHostname(conn, source, methodName, altHttpHostname.toString(), tlds);
    }

    // Create and/or get the HttpdBind info
    final int httpNetBind = getHttpdBind(conn, invalidateList, packageName, linuxServer, ipAddress, httpPort, AppProtocol.HTTP);

    // Create the Site
    final int site_id = conn.updateInt(
        "INSERT INTO web.\"Site\" (\n"
            + "  ao_server,\n"
            + "  \"name\",\n"
            + "  package,\n"
            + "  linux_account,\n"
            + "  linux_group,\n"
            + "  server_admin,\n"
            + "  php_version,\n"
            + "  enable_cgi,\n"
            + "  enable_ssi,\n"
            + "  enable_htaccess,\n"
            + "  enable_indexes,\n"
            + "  enable_follow_symlinks\n"
            + ") VALUES (\n"
            + "  ?,\n" // ao_server
            + "  ?,\n" // name
            + "  ?,\n" // package
            + "  ?,\n" // linux_account
            + "  ?,\n" // linux_group
            + "  ?,\n" // server_admin
            + "  ?,\n" // php_version
            + "  ?,\n" // enable_cgi
            + "  ?,\n" // enable_ssi
            + "  ?,\n" // enable_htaccess
            + "  ?,\n" // enable_indexes
            + "  ?\n" // enable_follow_symlinks
            + ") RETURNING id",
        linuxServer,
        siteName,
        packageName.toString(),
        user.toString(),
        group.toString(),
        serverAdmin.toString(),
        (phpVersion == -1) ? Null.INTEGER : phpVersion,
        enableCgi,
        enableSsi,
        enableHtaccess,
        enableIndexes,
        enableFollowSymlinks
    );
    invalidateList.addTable(conn, Table.TableId.HTTPD_SITES, account, linuxServer, false);

    // Create the Site
    conn.update(
        "INSERT INTO \"web.tomcat\".\"Site\" (httpd_site, version) VALUES (?,?)",
        site_id,
        tomcatVersion
    );
    invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SITES, account, linuxServer, false);

    // OperatingSystem settings
    PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);
    PosixPath docBase;
    try {
      docBase = PosixPath.valueOf(httpdSitesDir + "/" + siteName + "/webapps/" + Context.ROOT_DOC_BASE);
    } catch (ValidationException e) {
      throw new SQLException(e);
    }

    // Add the default httpd_tomcat_context
    conn.update(
        "INSERT INTO\n"
            + "  \"web.tomcat\".\"Context\"\n"
            + "VALUES (\n"
            + "  default,\n"
            + "  ?,\n"
            + "  " + Context.DEFAULT_CLASS_NAME + ",\n"
            + "  " + Context.DEFAULT_COOKIES + ",\n"
            + "  " + Context.DEFAULT_CROSS_CONTEXT + ",\n"
            + "  ?,\n"
            + "  " + Context.DEFAULT_OVERRIDE + ",\n"
            + "  '" + Context.ROOT_PATH + "',\n"
            + "  " + Context.DEFAULT_PRIVILEGED + ",\n"
            + "  " + Context.DEFAULT_RELOADABLE + ",\n"
            + "  " + Context.DEFAULT_USE_NAMING + ",\n"
            + "  " + Context.DEFAULT_WRAPPER_CLASS + ",\n"
            + "  " + Context.DEFAULT_DEBUG + ",\n"
            + "  " + Context.DEFAULT_WORK_DIR + ",\n"
            + "  " + Context.DEFAULT_SERVER_XML_CONFIGURED + "\n"
            + ")",
        site_id,
        docBase
    );
    invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_CONTEXTS, account, linuxServer, false);

    if (!isTomcat4) {
      conn.update(
          "INSERT INTO\n"
              + "  \"web.tomcat\".\"Context\"\n"
              + "VALUES (\n"
              + "  default,\n"
              + "  ?,\n"
              + "  " + Context.DEFAULT_CLASS_NAME + ",\n"
              + "  " + Context.DEFAULT_COOKIES + ",\n"
              + "  " + Context.DEFAULT_CROSS_CONTEXT + ",\n"
              + "  ?,\n"
              + "  " + Context.DEFAULT_OVERRIDE + ",\n"
              + "  '/examples',\n"
              + "  " + Context.DEFAULT_PRIVILEGED + ",\n"
              + "  " + Context.DEFAULT_RELOADABLE + ",\n"
              + "  " + Context.DEFAULT_USE_NAMING + ",\n"
              + "  " + Context.DEFAULT_WRAPPER_CLASS + ",\n"
              + "  " + Context.DEFAULT_DEBUG + ",\n"
              + "  " + Context.DEFAULT_WORK_DIR + ",\n"
              + "  " + Context.DEFAULT_SERVER_XML_CONFIGURED + "\n"
              + ")",
          site_id,
          conn.queryString("select install_dir from \"web.tomcat\".\"Version\" where version=?", tomcatVersion) + "/webapps/examples"
      );
      invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_CONTEXTS, account, linuxServer, false);
    }

    if ("jboss".equals(siteType)) {
      // Create the Site
      int wildcardIp = IpAddressHandler.getWildcardIpAddress(conn);
      int jnpBind = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          wildcardIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.JNP,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      int webserverBind = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          wildcardIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.WEBSERVER,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      int rmiBind = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          wildcardIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.RMI,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      int hypersonicBind = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          wildcardIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.HYPERSONIC,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      int jmxBind = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          wildcardIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.JMX,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      try (PreparedStatement pstmt = conn.getConnection().prepareStatement("insert into \"web.jboss\".\"Site\" values(?,?,?,?,?,?,?)")) {
        try {
          pstmt.setInt(1, site_id);
          pstmt.setInt(2, jbossVersion);
          pstmt.setInt(3, jnpBind);
          pstmt.setInt(4, webserverBind);
          pstmt.setInt(5, rmiBind);
          pstmt.setInt(6, hypersonicBind);
          pstmt.setInt(7, jmxBind);
          pstmt.executeUpdate();
        } catch (Error | RuntimeException | SQLException e) {
          ErrorPrinter.addSql(e, pstmt);
          throw e;
        }
      }
      invalidateList.addTable(conn, Table.TableId.HTTPD_JBOSS_SITES, account, linuxServer, false);
    } else if ("tomcat_shared".equals(siteType)) {
      // Create the SharedTomcatSite
      conn.update(
          "insert into \"web.tomcat\".\"SharedTomcatSite\" values(?,?)",
          site_id,
          sharedTomcatPkey
      );
      invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SHARED_SITES, account, linuxServer, false);
    } else if ("tomcat_standard".equals(siteType)) {
      // Create the PrivateTomcatSite
      if (isTomcat4) {
        int shutdownPort = NetBindHandler.allocateBind(
            conn,
            invalidateList,
            linuxServer,
            IpAddressHandler.getLoopbackIpAddress(conn, linuxServer),
            com.aoapps.net.Protocol.TCP,
            AppProtocol.TOMCAT4_SHUTDOWN,
            packageName,
            MINIMUM_AUTO_PORT_NUMBER
        );
        conn.update(
            "insert into \"web.tomcat\".\"PrivateTomcatSite\" (tomcat_site, tomcat4_shutdown_port, tomcat4_shutdown_key, \"maxParameterCount\", max_post_size) values(?,?,?,?,?)",
            site_id,
            shutdownPort,
            new Identifier(AoservMaster.getSecureRandom()).toString(),
            PrivateTomcatSite.DEFAULT_MAX_PARAMETER_COUNT,
            PrivateTomcatSite.DEFAULT_MAX_POST_SIZE
        );
      } else {
        conn.update(
            "insert into \"web.tomcat\".\"PrivateTomcatSite\" (tomcat_site, \"maxParameterCount\", max_post_size) values(?,?,?)",
            site_id,
            SharedTomcat.DEFAULT_MAX_PARAMETER_COUNT,
            SharedTomcat.DEFAULT_MAX_POST_SIZE
        );
      }
      invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_STD_SITES, account, linuxServer, false);
    }

    if (!isTomcat4 || !"tomcat_shared".equals(siteType)) {
      // Allocate a Bind for the worker
      int netBind_id = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          IpAddressHandler.getLoopbackIpAddress(conn, linuxServer),
          com.aoapps.net.Protocol.TCP,
          isTomcat4 ? JkProtocol.AJP13 : JkProtocol.AJP12,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );
      // Create the Worker
      addTomcatWorker(
          conn,
          invalidateList,
          netBind_id,
          site_id
      );
    }

    // Create the HTTP VirtualHost
    String siteLogsDir = OperatingSystemVersion.getHttpdSiteLogsDirectory(osv).toString();
    int virtualHost_id = conn.updateInt(
        "INSERT INTO web.\"VirtualHost\" (httpd_site, httpd_bind, access_log, error_log) VALUES (?,?,?,?) RETURNING id",
        site_id,
        httpNetBind,
        siteLogsDir + '/' + siteName + "/http/access_log",
        siteLogsDir + '/' + siteName + "/http/error_log"
    );
    invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_BINDS, account, linuxServer, false);

    conn.update(
        "insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,true)",
        virtualHost_id,
        primaryHttpHostname
    );
    for (DomainName altHttpHostname : altHttpHostnames) {
      conn.update(
          "insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,false)",
          virtualHost_id,
          altHttpHostname
      );
    }
    //conn.update(
    //  "insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,false)",
    //  httpSiteBindPKey,
    //  testUrl
    //);
    invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_URLS, account, linuxServer, false);

    // Initial HttpdTomcatSiteJkMounts
    if (useApache) {
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/j_security_check")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/servlet/*")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.do")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.jsp")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.jspa")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.jspx")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.vm")
      );
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*.xml")
      );
    } else {
      conn.update(
          "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
          site_id,
          checkJkMountPath("/*")
      );
      if (enableCgi) {
        conn.update(
            "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
            site_id,
            checkJkMountPath("/cgi-bin/*")
        );
      }
      boolean hasPhp;
      if (phpVersion != -1) {
        // CGI-based PHP
        hasPhp = true;
      } else {
        // Check for mod_php
        hasPhp = conn.queryBoolean(
            "select\n"
                + "  hs.mod_php_version is not null\n"
                + "from\n"
                + "  web.\"HttpdBind\" hb\n"
                + "  inner join web.\"HttpdServer\" hs on hb.httpd_server=hs.id\n"
                + "where\n"
                + "  hb.net_bind=?",
            httpNetBind
        );
      }
      if (hasPhp) {
        conn.update(
            "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,'/*.php',FALSE)",
            site_id
        );
      }
    }
    invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS, account, linuxServer, false);

    return site_id;
  }

  public static int addSharedTomcat(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      String name,
      int linuxServer,
      int version,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Group.Name group,
      boolean skipSecurityChecks
  ) throws IOException, SQLException {
    if (!SharedTomcat.isValidSharedTomcatName(name)) {
      throw new SQLException("Invalid shared Tomcat name: " + name);
    }
    if (user.equals(com.aoindustries.aoserv.client.linux.User.MAIL)) {
      throw new SQLException("Not allowed to add SharedTomcat for user '" + com.aoindustries.aoserv.client.linux.User.MAIL + '\'');
    }
    int userServer = LinuxAccountHandler.getUserServer(conn, user, linuxServer);
    int groupServer = LinuxAccountHandler.getGroupServer(conn, group, linuxServer);
    if (!skipSecurityChecks) {
      NetHostHandler.checkAccessHost(conn, source, "addHttpdSharedTomcat", linuxServer);
      LinuxAccountHandler.checkAccessUser(conn, source, "addHttpdSharedTomcat", user);
      if (LinuxAccountHandler.isUserServerDisabled(conn, userServer)) {
        throw new SQLException("Unable to add SharedTomcat, UserServer disabled: " + userServer);
      }
      LinuxAccountHandler.checkAccessGroup(conn, source, "addHttpdSharedTomcat", group);
    }
    if (
        group.equals(Group.FTPONLY)
            || group.equals(Group.MAIL)
            || group.equals(Group.MAILONLY)
    ) {
      throw new SQLException("Not allowed to add SharedTomcat for group '" + group + '\'');
    }

    // Tomcat 4 version will start with "4."
    String versionStr = conn.queryString("select version from distribution.\"SoftwareVersion\" where id=?", version);
    boolean isTomcat4 =
        !versionStr.equals(Version.VERSION_3_1)
            && !versionStr.equals(Version.VERSION_3_2_4);

    int sharedTomcat;
    if (isTomcat4) {
      Account.Name packageName = LinuxAccountHandler.getPackageForGroup(conn, group);
      int loopbackIp = IpAddressHandler.getLoopbackIpAddress(conn, linuxServer);

      // Allocate a Bind for the worker
      int hwBindId = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          loopbackIp,
          com.aoapps.net.Protocol.TCP,
          JkProtocol.AJP13,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );

      // Create the Worker
      addTomcatWorker(
          conn,
          invalidateList,
          hwBindId,
          -1
      );

      // Allocate the shutdown port
      int shutdownBindId = NetBindHandler.allocateBind(
          conn,
          invalidateList,
          linuxServer,
          loopbackIp,
          com.aoapps.net.Protocol.TCP,
          AppProtocol.TOMCAT4_SHUTDOWN,
          packageName,
          MINIMUM_AUTO_PORT_NUMBER
      );

      sharedTomcat = conn.updateInt(
          "INSERT INTO\n"
              + "  \"web.tomcat\".\"SharedTomcat\"\n"
              + "(\n"
              + "  name,\n"
              + "  ao_server,\n"
              + "  version,\n"
              + "  linux_server_account,\n"
              + "  linux_server_group,\n"
              + "  tomcat4_worker,\n"
              + "  tomcat4_shutdown_port,\n"
              + "  tomcat4_shutdown_key,\n"
              + "  \"maxParameterCount\",\n"
              + "  max_post_size\n"
              + ") VALUES(\n"
              + "  ?,\n" // name
              + "  ?,\n" // ao_server
              + "  ?,\n" // version
              + "  ?,\n" // linux_server_account
              + "  ?,\n" // linux_server_group
              + "  ?,\n" // tomcat4_worker
              + "  ?,\n" // tomcat4_shutdown_port
              + "  ?,\n" // tomcat4_shutdown_key
              + "  ?,\n" // maxParameterCount
              + "  ?\n"  // max_post_size
              + ") RETURNING id",
          name,
          linuxServer,
          version,
          userServer,
          groupServer,
          hwBindId,
          shutdownBindId,
          new Identifier(AoservMaster.getSecureRandom()).toString(),
          SharedTomcat.DEFAULT_MAX_PARAMETER_COUNT,
          SharedTomcat.DEFAULT_MAX_POST_SIZE
      );
    } else {
      sharedTomcat = conn.updateInt(
          "INSERT INTO\n"
              + "  \"web.tomcat\".\"SharedTomcat\"\n"
              + "(\n"
              + "  name,\n"
              + "  ao_server,\n"
              + "  version,\n"
              + "  linux_server_account,\n"
              + "  linux_server_group,\n"
              + "  \"maxParameterCount\",\n"
              + "  max_post_size\n"
              + ") VALUES(\n"
              + "  ?,\n" // name
              + "  ?,\n" // ao_server
              + "  ?,\n" // version
              + "  ?,\n" // linux_server_account
              + "  ?,\n" // linux_server_group
              + "  ?,\n" // maxParameterCount
              + "  ?\n"  // max_post_size
              + ") RETURNING id",
          name,
          linuxServer,
          version,
          userServer,
          groupServer,
          SharedTomcat.DEFAULT_MAX_PARAMETER_COUNT,
          SharedTomcat.DEFAULT_MAX_POST_SIZE
      );
    }
    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        AccountUserHandler.getAccountForUser(conn, user),
        linuxServer,
        false
    );
    return sharedTomcat;
  }

  /**
   * Creates a new Tomcat site with the standard configuration.
   */
  public static int addSharedTomcatSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int linuxServer,
      String siteName,
      Account.Name packageName,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Group.Name group,
      Email serverAdmin,
      boolean useApache,
      int ipAddress,
      DomainName primaryHttpHostname,
      DomainName[] altHttpHostnames,
      String sharedTomcatName,
      int phpVersion,
      boolean enableCgi,
      boolean enableSsi,
      boolean enableHtaccess,
      boolean enableIndexes,
      boolean enableFollowSymlinks
  ) throws IOException, SQLException {
    return addHttpdJvmSite(
        "addSharedTomcatSite",
        conn,
        source,
        invalidateList,
        linuxServer,
        siteName,
        packageName,
        user,
        group,
        serverAdmin,
        useApache,
        ipAddress,
        primaryHttpHostname,
        altHttpHostnames,
        "tomcat_shared",
        -1,
        -1,
        sharedTomcatName,
        phpVersion,
        enableCgi,
        enableSsi,
        enableHtaccess,
        enableIndexes,
        enableFollowSymlinks
    );
  }

  /**
   * Creates a new Tomcat site with the standard configuration.
   */
  public static int addPrivateTomcatSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int linuxServer,
      String siteName,
      Account.Name packageName,
      com.aoindustries.aoserv.client.linux.User.Name user,
      Group.Name group,
      Email serverAdmin,
      boolean useApache,
      int ipAddress,
      DomainName primaryHttpHostname,
      DomainName[] altHttpHostnames,
      int tomcatVersion,
      int phpVersion,
      boolean enableCgi,
      boolean enableSsi,
      boolean enableHtaccess,
      boolean enableIndexes,
      boolean enableFollowSymlinks
  ) throws IOException, SQLException {
    return addHttpdJvmSite(
        "addPrivateTomcatSite",
        conn,
        source,
        invalidateList,
        linuxServer,
        siteName,
        packageName,
        user,
        group,
        serverAdmin,
        useApache,
        ipAddress,
        primaryHttpHostname,
        altHttpHostnames,
        "tomcat_standard",
        -1,
        tomcatVersion,
        "",
        phpVersion,
        enableCgi,
        enableSsi,
        enableHtaccess,
        enableIndexes,
        enableFollowSymlinks
    );
  }

  public static void disableSharedTomcat(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int sharedTomcat
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableSharedTomcat", disableLog, false);
    checkAccessSharedTomcat(conn, source, "disableSharedTomcat", sharedTomcat);
    if (isSharedTomcatDisabled(conn, sharedTomcat)) {
      throw new SQLException("SharedTomcat is already disabled: " + sharedTomcat);
    }

    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set disable_log=? where id=?",
        disableLog,
        sharedTomcat
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void disableSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int site
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableSite", disableLog, false);
    checkAccessSite(conn, source, "disableSite", site);
    if (isSiteDisabled(conn, site)) {
      throw new SQLException("Site is already disabled: " + site);
    }
    IntList httpdSiteBinds = getVirtualHostsForSite(conn, site);
    for (int c = 0; c < httpdSiteBinds.size(); c++) {
      int hsb = httpdSiteBinds.getInt(c);
      if (!isVirtualHostDisabled(conn, hsb)) {
        throw new SQLException("Cannot disable Site #" + site + ": VirtualHost not disabled: " + hsb);
      }
    }

    conn.update(
        "update web.\"Site\" set disable_log=? where id=?",
        disableLog,
        site
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void disableVirtualHost(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int disableLog,
      int virtualHost
  ) throws IOException, SQLException {
    AccountHandler.checkAccessDisableLog(conn, source, "disableVirtualHost", disableLog, false);
    int site = conn.queryInt("select httpd_site from web.\"VirtualHost\" where id=?", virtualHost);
    checkAccessSite(conn, source, "disableVirtualHost", site);
    if (isVirtualHostDisabled(conn, virtualHost)) {
      throw new SQLException("VirtualHost is already disabled: " + virtualHost);
    }

    conn.update(
        "update web.\"VirtualHost\" set disable_log=? where id=?",
        disableLog,
        virtualHost
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITE_BINDS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void enableSharedTomcat(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "enableSharedTomcat", sharedTomcat);
    int disableLog = getDisableLogForSharedTomcat(conn, sharedTomcat);
    if (disableLog == -1) {
      throw new SQLException("SharedTomcat is already enabled: " + sharedTomcat);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableSharedTomcat", disableLog, true);
    Account.Name pk = getPackageForSharedTomcat(conn, sharedTomcat);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable SharedTomcat #" + sharedTomcat + ", Package not enabled: " + pk);
    }
    int userServer = getLinuxUserServerForSharedTomcat(conn, sharedTomcat);
    if (LinuxAccountHandler.isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to enable SharedTomcat #" + sharedTomcat + ", UserServer not enabled: " + userServer);
    }

    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set disable_log=null where id=?",
        sharedTomcat
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        PackageHandler.getAccountForPackage(conn, pk),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void enableSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "enableSite", site);
    int disableLog = getDisableLogForSite(conn, site);
    if (disableLog == -1) {
      throw new SQLException("Site is already enabled: " + site);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableSite", disableLog, true);
    Account.Name pk = getPackageForSite(conn, site);
    if (PackageHandler.isPackageDisabled(conn, pk)) {
      throw new SQLException("Unable to enable Site #" + site + ", Package not enabled: " + pk);
    }
    int userServer = getLinuxUserServerForSite(conn, site);
    if (LinuxAccountHandler.isUserServerDisabled(conn, userServer)) {
      throw new SQLException("Unable to enable Site #" + site + ", UserServer not enabled: " + userServer);
    }

    conn.update(
        "update web.\"Site\" set disable_log=null where id=?",
        site
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITES,
        PackageHandler.getAccountForPackage(conn, pk),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void enableVirtualHost(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHost
  ) throws IOException, SQLException {
    int hs = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "enableVirtualHost", hs);
    int disableLog = getDisableLogForVirtualHost(conn, virtualHost);
    if (disableLog == -1) {
      throw new SQLException("VirtualHost is already enabled: " + virtualHost);
    }
    AccountHandler.checkAccessDisableLog(conn, source, "enableVirtualHost", disableLog, true);
    if (isSiteDisabled(conn, hs)) {
      throw new SQLException("Unable to enable VirtualHost #" + virtualHost + ", Site not enabled: " + hs);
    }

    conn.update(
        "update web.\"VirtualHost\" set disable_log=null where id=?",
        virtualHost
    );

    // Notify all clients of the update
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITE_BINDS,
        getAccountForSite(conn, hs),
        getLinuxServerForSite(conn, hs),
        false
    );
  }

  public static String generateSharedTomcatName(DatabaseConnection conn, String template) throws SQLException, IOException {
    // Load the entire list of site names
    List<String> names = conn.queryStringList("select name from \"web.tomcat\".\"SharedTomcat\" group by name");
    int size = names.size();

    // Sort them
    List<String> sorted = new SortedArrayList<>(size);
    sorted.addAll(names);

    // OperatingSystem settings
    PosixPath httpdSharedTomcatsDirCentos5 = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(OperatingSystemVersion.CENTOS_5_I686_AND_X86_64);
    PosixPath httpdSharedTomcatsDirCentos7 = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(OperatingSystemVersion.CENTOS_7_X86_64);

    // Find one that is not used
    String goodOne = null;
    for (int c = 1; c < Integer.MAX_VALUE; c++) {
      String name = template + c;
      if (!SharedTomcat.isValidSharedTomcatName(name)) {
        throw new SQLException("Invalid shared Tomcat name: " + name);
      }
      if (!sorted.contains(name)) {
        PosixPath wwwgroupDirCentos5;
        PosixPath wwwgroupDirCentos7;
        try {
          wwwgroupDirCentos5 = PosixPath.valueOf(httpdSharedTomcatsDirCentos5 + "/" + name);
          wwwgroupDirCentos7 = PosixPath.valueOf(httpdSharedTomcatsDirCentos7 + "/" + name);
        } catch (ValidationException e) {
          throw new SQLException(e);
        }
        if (
            // Must also not be found in linux.UserServer.home
            conn.queryInt(
                "select\n"
                    + "  count(*)\n"
                    + "from\n"
                    + "  linux.\"UserServer\"\n"
                    + "where\n"
                    + "  home=?\n"
                    + "  or substring(home from 1 for " + (wwwgroupDirCentos5.toString().length() + 1) + ")=?\n"
                    + "  or home=?\n"
                    + "  or substring(home from 1 for " + (wwwgroupDirCentos7.toString().length() + 1) + ")=?",
                wwwgroupDirCentos5,
                wwwgroupDirCentos5 + "/",
                wwwgroupDirCentos7,
                wwwgroupDirCentos7 + "/"
            ) == 0
                // Must also not be found in account.User.username
                && conn.queryInt("select count(*) from account.\"User\" where username=?", name) == 0
                // Must also not be found in linux.Group.name
                && conn.queryInt("select count(*) from linux.\"Group\" where name=?", name) == 0
        ) {
          goodOne = name;
          break;
        }
      }
    }

    // If could not find one, report and error
    if (goodOne == null) {
      throw new SQLException("Unable to find available shared Tomcat name for template: " + template);
    } else {
      return goodOne;
    }
  }

  public static String generateSiteName(
      DatabaseConnection conn,
      String template
  ) throws IOException, SQLException {
    // Load the entire list of site names
    List<String> names = conn.queryStringList("select \"name\" from web.\"Site\" group by \"name\"");
    int size = names.size();

    // Sort them
    List<String> sorted = new SortedArrayList<>(size);
    sorted.addAll(names);

    // OperatingSystem settings
    PosixPath httpdSitesDirCentos5 = OperatingSystemVersion.getHttpdSitesDirectory(OperatingSystemVersion.CENTOS_5_I686_AND_X86_64);
    PosixPath httpdSitesDirCentos7 = OperatingSystemVersion.getHttpdSitesDirectory(OperatingSystemVersion.CENTOS_7_X86_64);

    // Find one that is not used
    String goodOne = null;
    for (int c = 1; c < Integer.MAX_VALUE; c++) {
      String name = template + c;
      if (!Site.isValidSiteName(name)) {
        throw new SQLException("Invalid site name: " + name);
      }
      if (!sorted.contains(name)) {
        // Must also not be found in linux.UserServer.home on CentOS 5 or CentOS 7
        PosixPath wwwDirCentos5;
        PosixPath wwwDirCentos7;
        try {
          wwwDirCentos5 = PosixPath.valueOf(httpdSitesDirCentos5 + "/" + name);
          wwwDirCentos7 = PosixPath.valueOf(httpdSitesDirCentos7 + "/" + name);
        } catch (ValidationException e) {
          throw new SQLException(e);
        }
        int count = conn.queryInt(
            "select\n"
                + "  count(*)\n"
                + "from\n"
                + "  linux.\"UserServer\"\n"
                + "where\n"
                + "  home=?\n"
                + "  or substring(home from 1 for " + (wwwDirCentos5.toString().length() + 1) + ")=?\n"
                + "  or home=?\n"
                + "  or substring(home from 1 for " + (wwwDirCentos7.toString().length() + 1) + ")=?",
            wwwDirCentos5,
            wwwDirCentos5 + "/",
            wwwDirCentos7,
            wwwDirCentos7 + "/"
        );
        if (count == 0) {
          goodOne = name;
          break;
        }
      }
    }

    // If could not find one, report and error
    if (goodOne == null) {
      throw new SQLException("Unable to find available site name for template: " + template);
    }
    return goodOne;
  }

  public static int getDisableLogForSharedTomcat(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);
  }

  public static int getDisableLogForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from web.\"Site\" where id=?", site);
  }

  public static int getDisableLogForVirtualHost(DatabaseConnection conn, int virtualHost) throws IOException, SQLException {
    return conn.queryInt("select coalesce(disable_log, -1) from web.\"VirtualHost\" where id=?", virtualHost);
  }

  public static IntList getVirtualHostsForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryIntList("select id from web.\"VirtualHost\" where httpd_site=?", site);
  }

  public static int getSiteForVirtualHostName(DatabaseConnection conn, int virtualHostName) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  hsb.httpd_site\n"
            + "from\n"
            + "  web.\"VirtualHostName\" hsu,\n"
            + "  web.\"VirtualHost\" hsb\n"
            + "where\n"
            + "  hsu.id=?\n"
            + "  and hsu.httpd_site_bind=hsb.id",
        virtualHostName
    );
  }

  public static IntList getSharedTomcatsForLinuxUserServer(DatabaseConnection conn, int linuxUserServer) throws IOException, SQLException {
    return conn.queryIntList("select id from \"web.tomcat\".\"SharedTomcat\" where linux_server_account=?", linuxUserServer);
  }

  public static IntList getHttpdSharedTomcatsForPackage(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
    return conn.queryIntList(
        "select\n"
            + "  hst.id\n"
            + "from\n"
            + "  linux.\"Group\" lg,\n"
            + "  linux.\"GroupServer\" lsg,\n"
            + "  \"web.tomcat\".\"SharedTomcat\" hst\n"
            + "where\n"
            + "  lg.package=?\n"
            + "  and lg.name=lsg.name\n"
            + "  and lsg.id=hst.linux_server_group",
        packageName
    );
  }

  public static IntList getHttpdSitesForPackage(
      DatabaseConnection conn,
      Account.Name packageName
  ) throws IOException, SQLException {
    return conn.queryIntList("select id from web.\"Site\" where package=?", packageName);
  }

  public static IntList getSitesForLinuxUserServer(DatabaseConnection conn, int linuxUserServer) throws IOException, SQLException {
    return conn.queryIntList(
        "select\n"
            + "  hs.id\n"
            + "from\n"
            + "  linux.\"UserServer\" lsa,\n"
            + "  web.\"Site\" hs\n"
            + "where\n"
            + "  lsa.id=?\n"
            + "  and lsa.username=hs.linux_account\n"
            + "  and lsa.ao_server=hs.ao_server",
        linuxUserServer
    );
  }

  public static Account.Name getAccountForSharedTomcat(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  \"web.tomcat\".\"SharedTomcat\" hst,\n"
            + "  linux.\"GroupServer\" lsg,\n"
            + "  linux.\"Group\" lg,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  hst.id=?\n"
            + "  and hst.linux_server_group=lsg.id\n"
            + "  and lsg.name=lg.name\n"
            + "  and lg.package=pk.name",
        sharedTomcat
    );
  }

  public static Account.Name getAccountForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  web.\"Site\" hs,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  hs.id=?\n"
            + "  and hs.package=pk.name",
        site
    );
  }

  public static Account.Name getAccountForHttpdServer(DatabaseConnection conn, int httpdServer) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  pk.accounting\n"
            + "from\n"
            + "  web.\"HttpdServer\" hs,\n"
            + "  billing.\"Package\" pk\n"
            + "where\n"
            + "  hs.id=?\n"
            + "  and hs.package=pk.id",
        httpdServer
    );
  }

  public static int getSiteForVirtualHost(DatabaseConnection conn, int virtualHost) throws IOException, SQLException {
    return conn.queryInt("select httpd_site from web.\"VirtualHost\" where id=?", virtualHost);
  }

  public static int getLinuxUserServerForSharedTomcat(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    return conn.queryInt("select linux_server_account from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);
  }

  public static int getLinuxUserServerForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryInt(
        "select\n"
            + "  lsa.id\n"
            + "from\n"
            + "  web.\"Site\" hs,\n"
            + "  linux.\"UserServer\" lsa\n"
            + "where\n"
            + "  hs.id=?\n"
            + "  and hs.linux_account=lsa.username\n"
            + "  and hs.ao_server=lsa.ao_server",
        site
    );
  }

  public static Account.Name getPackageForHttpdServer(
      DatabaseConnection conn,
      int httpdServer
  ) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  pk.name\n"
            + "from\n"
            + "  web.\"HttpdServer\" hs\n"
            + "  inner join billing.\"Package\" pk on hs.package=pk.id\n"
            + "where\n"
            + "  hs.id=?",
        httpdServer
    );
  }

  public static Account.Name getPackageForSharedTomcat(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select\n"
            + "  lg.package\n"
            + "from\n"
            + "  \"web.tomcat\".\"SharedTomcat\" hst,\n"
            + "  linux.\"GroupServer\" lsg,\n"
            + "  linux.\"Group\" lg\n"
            + "where\n"
            + "  hst.id=?\n"
            + "  and hst.linux_server_group=lsg.id\n"
            + "  and lsg.name=lg.name",
        sharedTomcat
    );
  }

  public static Account.Name getPackageForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryObject(
        ObjectFactories.accountNameFactory,
        "select package from web.\"Site\" where id=?",
        site
    );
  }

  public static int getLinuxServerForSharedTomcat(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    return conn.queryInt("select ao_server from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);
  }

  public static int getLinuxServerForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryInt("select ao_server from web.\"Site\" where id=?", site);
  }

  public static int getLinuxServerForHttpdServer(DatabaseConnection conn, int httpdServer) throws IOException, SQLException {
    return conn.queryInt("select ao_server from web.\"HttpdServer\" where id=?", httpdServer);
  }

  public static String getNameForSite(DatabaseConnection conn, int site) throws IOException, SQLException {
    return conn.queryString("select \"name\" from web.\"Site\" where id=?", site);
  }

  /**
   * Creates the default conf/passwd file.
   */
  /*
  public static void initializeHttpdSitePasswdFile(
    DatabaseConnection conn,
    RequestSource source,
    int sitePKey,
    String username,
    String encPassword
  ) throws IOException, SQLException {
    checkAccessHttpdSite(conn, source, "initializeHttpdSitePasswdFile", sitePKey);
    if (isHttpdSiteDisabled(conn, sitePKey)) {
      throw new SQLException("Unable to initialize Site passwd file, Site disabled: "+sitePKey);
    }

    DaemonHandler.getDaemonConnector(
      conn,
      getLinuxServerForHttpdSite(conn, sitePKey)
    ).initializeHttpdSitePasswdFile(sitePKey, username, encPassword);
  }*/

  public static void invalidateTable(Table.TableId tableId) {
    if (tableId == Table.TableId.HTTPD_SHARED_TOMCATS) {
      synchronized (WebHandler.class) {
        disabledSharedTomcats.clear();
      }
    } else if (tableId == Table.TableId.HTTPD_SITE_BINDS) {
      synchronized (WebHandler.class) {
        disabledVirtualHosts.clear();
      }
    } else if (tableId == Table.TableId.HTTPD_SITES) {
      synchronized (WebHandler.class) {
        disabledSites.clear();
      }
    }
  }

  public static boolean isSharedTomcatDisabled(DatabaseConnection conn, int sharedTomcat) throws IOException, SQLException {
    Integer idObj = sharedTomcat;
    synchronized (WebHandler.class) {
      Boolean isDisabledObj = disabledSharedTomcats.get(idObj);
      if (isDisabledObj != null) {
        return isDisabledObj;
      }
      boolean isDisabled = getDisableLogForSharedTomcat(conn, sharedTomcat) != -1;
      disabledSharedTomcats.put(idObj, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isVirtualHostDisabled(DatabaseConnection conn, int virtualHost) throws IOException, SQLException {
    Integer idObj = virtualHost;
    synchronized (WebHandler.class) {
      Boolean isDisabledObj = disabledVirtualHosts.get(idObj);
      if (isDisabledObj != null) {
        return isDisabledObj;
      }
      boolean isDisabled = getDisableLogForVirtualHost(conn, virtualHost) != -1;
      disabledVirtualHosts.put(idObj, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isSiteDisabled(DatabaseConnection conn, int site) throws IOException, SQLException {
    Integer idObj = site;
    synchronized (WebHandler.class) {
      Boolean isDisabledObj = disabledSites.get(idObj);
      if (isDisabledObj != null) {
        return isDisabledObj;
      }
      boolean isDisabled = getDisableLogForSite(conn, site) != -1;
      disabledSites.put(idObj, isDisabled);
      return isDisabled;
    }
  }

  public static boolean isSharedTomcatNameAvailable(DatabaseConnection conn, String name) throws IOException, SQLException {
    return conn.queryBoolean("select (select id from \"web.tomcat\".\"SharedTomcat\" where name=? limit 1) is null", name);
  }

  public static boolean isSiteNameAvailable(DatabaseConnection conn, String siteName) throws IOException, SQLException {
    return conn.queryBoolean("select (select id from web.\"Site\" where \"name\"=? limit 1) is null", siteName);
  }

  /**
   * Starts up a Java VM.
   */
  public static String startJvm(
      DatabaseConnection conn,
      RequestSource source,
      int tomcatSite
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "startJvm", tomcatSite);
    if (isSiteDisabled(conn, tomcatSite)) {
      throw new SQLException("Unable to start JVM, Site disabled: " + tomcatSite);
    }

    // Get the server and siteName for the site
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    // Contact the daemon and start the JVM
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.startJvm(tomcatSite);
  }

  /**
   * Stops up a Java VM.
   */
  public static String stopJvm(
      DatabaseConnection conn,
      RequestSource source,
      int tomcatSite
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "stopJvm", tomcatSite);
    // Can only stop the daemon if can access the shared linux account
    if (conn.queryBoolean("select (select tomcat_site from \"web.tomcat\".\"SharedTomcatSite\" where tomcat_site=?) is not null", tomcatSite)) {
      int userServer = conn.queryInt(
          "select\n"
              + "  hst.linux_server_account\n"
              + "from\n"
              + "  \"web.tomcat\".\"SharedTomcatSite\" htss,\n"
              + "  \"web.tomcat\".\"SharedTomcat\" hst\n"
              + "where\n"
              + "  htss.tomcat_site=?\n"
              + "  and htss.httpd_shared_tomcat=hst.id",
          tomcatSite
      );
      LinuxAccountHandler.checkAccessUserServer(conn, source, "stopJvm", userServer);
    }

    // Get the server and siteName for the site
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    // Contact the daemon and start the JVM
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    return daemonConnector.stopJvm(tomcatSite);
  }

  /**
   * Waits for pending or processing updates to complete.
   */
  public static void waitForHttpdSiteRebuild(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    NetHostHandler.checkAccessHost(conn, source, "waitForHttpdSiteRebuild", linuxServer);
    NetHostHandler.waitForInvalidates(linuxServer);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.waitForHttpdSiteRebuild();
  }

  public static int getHttpdBind(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      Account.Name packageName,
      int linuxServer,
      int ipAddress,
      Port httpPort,
      String protocol
  ) throws IOException, SQLException {
    // First, find the net_bind
    int netBind;
    try (
        PreparedStatement pstmt = conn.getConnection(true).prepareStatement(
            "select id, app_protocol from net.\"Bind\" where server=? and \"ipAddress\"=? and port=?::\"com.aoapps.net\".\"Port\" and net_protocol=?::\"com.aoapps.net\".\"Protocol\""
        )
        ) {
      try {
        pstmt.setInt(1, linuxServer);
        pstmt.setInt(2, ipAddress);
        pstmt.setInt(3, httpPort.getPort());
        pstmt.setString(4, httpPort.getProtocol().name());
        try (ResultSet results = pstmt.executeQuery()) {
          if (results.next()) {
            netBind = results.getInt(1);
            String bindProtocol = results.getString(2);
            if (!protocol.equals(bindProtocol)) {
              throw new SQLException(
                  "Protocol mismatch on net.\"Bind\"(id="
                      + netBind
                      + " ao_server="
                      + linuxServer
                      + " ipAddress="
                      + ipAddress
                      + " port="
                      + httpPort
                      + " net_protocol=tcp), app_protocol is "
                      + bindProtocol
                      + ", requested protocol is "
                      + protocol
              );
            }
          } else {
            netBind = -1;
          }
        }
      } catch (Error | RuntimeException | SQLException e) {
        ErrorPrinter.addSql(e, pstmt);
        throw e;
      }
    }

    // Allocate the net_bind, if needed
    if (netBind == -1) {
      netBind = conn.updateInt(
          "INSERT INTO net.\"Bind\" VALUES (default,?,?,?,?::\"com.aoapps.net\".\"Port\",?::\"com.aoapps.net\".\"Protocol\",?,true) RETURNING id",
          packageName.toString(),
          linuxServer,
          ipAddress,
          httpPort.getPort(),
          httpPort.getProtocol().name(),
          protocol
      );
      Account.Name business = PackageHandler.getAccountForPackage(conn, packageName);
      invalidateList.addTable(
          conn,
          Table.TableId.NET_BINDS,
          business,
          linuxServer,
          false
      );
      // Default to open in public firewalld zone
      conn.update(
          "insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
              + "  ?,\n"
              + "  (select id from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
              + ")",
          netBind,
          linuxServer,
          FirewallZone.PUBLIC
      );
      invalidateList.addTable(
          conn,
          Table.TableId.NET_BIND_FIREWALLD_ZONES,
          business,
          linuxServer,
          false
      );
    }

    // Allocate the httpd_bind if needed
    if (
        conn.queryBoolean(
            "select\n"
                + "  (\n"
                + "    select\n"
                + "      net_bind\n"
                + "    from\n"
                // TODO: Rename web."Bind", and web."HttpdServer" to web."Server"
                + "      web.\"HttpdBind\"\n"
                + "    where\n"
                + "      net_bind=?\n"
                + "    limit 1\n"
                + "  ) is null",
            netBind
        )
    ) {
      // Get the list of web.HttpdServer and how many web.VirtualHost there are
      int lowestId = -1;
      int lowestCount = Integer.MAX_VALUE;
      try (
          PreparedStatement pstmt = conn.getConnection(true).prepareStatement(
              "select\n"
                  + "  hs.id,\n"
                  + "  (\n"
                  + "    select\n"
                  + "      count(*)\n"
                  + "    from\n"
                  + "      web.\"HttpdBind\" hb,\n"
                  + "      web.\"VirtualHost\" hsb\n"
                  + "    where\n"
                  + "      hs.id=hb.httpd_server\n"
                  + "      and hb.net_bind=hsb.httpd_bind\n"
                  + "  )\n"
                  + "from\n"
                  + "  web.\"HttpdServer\" hs\n"
                  + "where\n"
                  + "  hs.can_add_sites\n"
                  + "  and hs.ao_server=?\n"
                  + "  and (\n"
                  + "    hs.is_shared\n"
                  + "    or (\n"
                  + "      account.is_account_or_parent(\n"
                  + "        (select pk1.accounting from billing.\"Package\" pk1 where hs.package=pk1.id),\n"
                  + "        (select accounting from billing.\"Package\" where name=?)\n"
                  + "      )\n"
                  + "    )\n"
                  + "  )"
          )
          ) {
        try {
          pstmt.setInt(1, linuxServer);
          pstmt.setString(2, packageName.toString());
          try (ResultSet results = pstmt.executeQuery()) {
            while (results.next()) {
              int site = results.getInt(1);
              int useCount = results.getInt(2);
              if (useCount < lowestCount) {
                lowestId = site;
                lowestCount = useCount;
              }
            }
          }
        } catch (Error | RuntimeException | SQLException e) {
          ErrorPrinter.addSql(e, pstmt);
          throw e;
        }
      }
      if (lowestId == -1) {
        throw new SQLException("Unable to determine which httpd_server to add the new httpd_bind to");
      }
      // Insert into the DB
      conn.update(
          "insert into web.\"HttpdBind\" values(?,?)",
          netBind,
          lowestId
      );
    }
    // Always invalidate these tables because adding site may grant permissions to these rows
    Account.Name business = PackageHandler.getAccountForPackage(conn, packageName);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_BINDS,
        business,
        linuxServer,
        false
    );
    invalidateList.addTable(
        conn,
        Table.TableId.NET_BINDS,
        business,
        linuxServer,
        false
    );
    invalidateList.addTable(
        conn,
        Table.TableId.NET_BIND_FIREWALLD_ZONES,
        business,
        linuxServer,
        false
    );

    return netBind;
  }

  public static void removeSharedTomcat(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "removeSharedTomcat", sharedTomcat);

    removeSharedTomcat(conn, invalidateList, sharedTomcat);
  }

  public static void removeSharedTomcat(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int sharedTomcat
  ) throws IOException, SQLException {
    final Account.Name account = getAccountForSharedTomcat(conn, sharedTomcat);
    final int linuxServer = getLinuxServerForSharedTomcat(conn, sharedTomcat);

    final int tomcat4Worker = conn.queryInt("select coalesce(tomcat4_worker, -1) from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);
    final int tomcat4ShutdownPort = conn.queryInt("select coalesce(tomcat4_shutdown_port, -1) from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);

    conn.update("delete from \"web.tomcat\".\"SharedTomcat\" where id=?", sharedTomcat);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        account,
        linuxServer,
        false
    );

    if (tomcat4Worker != -1) {
      conn.update("delete from \"web.tomcat\".\"Worker\" where bind=?", tomcat4Worker);
      invalidateList.addTable(conn, Table.TableId.HTTPD_WORKERS, account, linuxServer, false);

      conn.update("delete from net.\"Bind\" where id=?", tomcat4Worker);
      invalidateList.addTable(conn, Table.TableId.NET_BINDS, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.NET_BIND_FIREWALLD_ZONES, account, linuxServer, false);
    }

    if (tomcat4ShutdownPort != -1) {
      conn.update("delete from net.\"Bind\" where id=?", tomcat4ShutdownPort);
      invalidateList.addTable(conn, Table.TableId.NET_BINDS, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.NET_BIND_FIREWALLD_ZONES, account, linuxServer, false);
    }
  }

  public static void removeSite(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "removeSite", site);

    removeSite(conn, invalidateList, site);
  }

  public static boolean isStaticSite(DatabaseConnection conn, int site) throws SQLException {
    return conn.queryBoolean("select (select httpd_site from web.\"StaticSite\" where httpd_site=?) is not null", site);
  }

  /**
   * web.Site
   *           + web.VirtualHost
   *           |                + web.Header
   *           |                + web.RewriteRule
   *           |                + web.VirtualHostName
   *           |                |               + dns.Record
   *           |                + web.HttpdBind
   *           |                            + net.Bind
   *           + web.tomcat.Site
   *           |                  + web.tomcat.Context
   *           |                                        + web.tomcat.ContextDataSource
   *           |                                        + web.tomcat.ContextParameter
   *           |                  + web.tomcat.Worker
   *           |                  |             + net.Bind
   *           |                  + web.tomcat.SharedTomcatSite
   *           |                  |             + linux.GroupUser
   *           |                  + web.tomcat.PrivateTomcatSite
   *           |                                         + net.Bind
   *           |                  + web.jboss.Site
   *           |                                   + net.Bind
   *           + web.StaticSite
   */
  public static void removeSite(
      DatabaseConnection conn,
      InvalidateList invalidateList,
      int site
  ) throws IOException, SQLException {
    Account.Name account = getAccountForSite(conn, site);
    int linuxServer = getLinuxServerForSite(conn, site);
    String siteName = conn.queryString("select \"name\" from web.\"Site\" where id=?", site);

    // OperatingSystem settings
    int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    PosixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

    // Must not contain a CVS repository
    PosixPath siteDir;
    try {
      siteDir = PosixPath.valueOf(httpdSitesDir + "/" + siteName);
    } catch (ValidationException e) {
      throw new SQLException(e);
    }
    int count = conn.queryInt(
        "select\n"
            + "  count(*)\n"
            + "from\n"
            + "  scm.\"CvsRepository\" cr,\n"
            + "  linux.\"UserServer\" lsa\n"
            + "where\n"
            + "  cr.linux_server_account=lsa.id\n"
            + "  and lsa.ao_server=?\n"
            + "  and (\n"
            + "    cr.path=?\n"
            + "    or substring(cr.path from 1 for " + (siteDir.toString().length() + 1) + ")=?\n"
            + "  )",
        linuxServer,
        siteDir,
        siteDir + "/"
    );
    if (count > 0) {
      throw new SQLException(
          "Site directory on Server #" + linuxServer + " contains "
              + count + " CVS " + (count == 1 ? "repository" : "repositories")
              + ": " + siteDir
      );
    }

    // web.Location
    if (conn.update("delete from web.\"Location\" where httpd_site=?", site) > 0) {
      invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_AUTHENTICATED_LOCATIONS, account, linuxServer, false);
    }

    // web.VirtualHost
    IntList httpdSiteBinds = conn.queryIntList("select id from web.\"VirtualHost\" where httpd_site=?", site);
    if (!httpdSiteBinds.isEmpty()) {
      DnsService dnsService = AoservMaster.getService(DnsService.class);
      List<DomainName> tlds = dnsService.getTopLevelDomains(conn);
      SortedIntArrayList httpdBinds = new SortedIntArrayList();
      for (int c = 0; c < httpdSiteBinds.size(); c++) {
        int httpdSiteBind = httpdSiteBinds.getInt(c);

        // web.VirtualHostName
        IntList virtualHostNames = conn.queryIntList("select id from web.\"VirtualHostName\" where httpd_site_bind=?", httpdSiteBind);
        for (int d = 0; d < virtualHostNames.size(); d++) {
          int httpdSiteUrl = virtualHostNames.getInt(d);

          // dns.Record
          DomainName hostname = conn.queryObject(
              ObjectFactories.domainNameFactory,
              "select hostname from web.\"VirtualHostName\" where id=?",
              httpdSiteUrl
          );
          conn.update("delete from web.\"VirtualHostName\" where id=?", httpdSiteUrl);
          invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_URLS, account, linuxServer, false);
          dnsService.removeUnusedDnsRecord(conn, invalidateList, hostname, tlds);
        }

        int hb = conn.queryInt("select httpd_bind from web.\"VirtualHost\" where id=?", httpdSiteBind);
        if (!httpdBinds.contains(hb)) {
          httpdBinds.add(hb);
        }
      }
      conn.update("delete from web.\"VirtualHost\" where httpd_site=?", site);
      invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_BINDS, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.HTTPD_SITE_BIND_HEADERS, account, linuxServer, false);
      invalidateList.addTable(conn, Table.TableId.RewriteRule, account, linuxServer, false);

      for (int c = 0; c < httpdBinds.size(); c++) {
        int httpdBind = httpdBinds.getInt(c);
        if (
            conn.queryBoolean(
                "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      id\n"
                    + "    from\n"
                    + "      web.\"VirtualHost\"\n"
                    + "    where\n"
                    + "      httpd_bind=?\n"
                    + "    limit 1\n"
                    + "  ) is null",
                httpdBind
            )
        ) {
          // Do not clear up the web.HttpdBind for overflow IPs
          if (
              conn.queryBoolean(
                  "select\n"
                      + "  not ia.\"isOverflow\"\n"
                      + "from\n"
                      + "  net.\"Bind\" nb,\n"
                      + "  net.\"IpAddress\" ia\n"
                      + "where\n"
                      + "  nb.id=?\n"
                      + "  and nb.\"ipAddress\"=ia.id",
                  httpdBind
              )
          ) {
            conn.update("delete from web.\"HttpdBind\" where net_bind=?", httpdBind);
            conn.update("delete from net.\"Bind\" where id=?", httpdBind);
          }
        }
      }
    }

    // web.tomcat.Site
    if (conn.queryBoolean("select (select httpd_site from \"web.tomcat\".\"Site\" where httpd_site=? limit 1) is not null", site)) {
      // web.tomcat.ContextDataSource
      IntList htdss = conn.queryIntList("select htds.id from \"web.tomcat\".\"Context\" htc, \"web.tomcat\".\"ContextDataSource\" htds where htc.tomcat_site=? and htc.id=htds.tomcat_context", site);
      if (!htdss.isEmpty()) {
        for (int c = 0; c < htdss.size(); c++) {
          conn.update("delete from \"web.tomcat\".\"ContextDataSource\" where id=?", htdss.getInt(c));
        }
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_DATA_SOURCES, account, linuxServer, false);
      }

      // web.tomcat.ContextParameter
      IntList htps = conn.queryIntList("select htp.id from \"web.tomcat\".\"Context\" htc, \"web.tomcat\".\"ContextParameter\" htp where htc.tomcat_site=? and htc.id=htp.tomcat_context", site);
      if (!htps.isEmpty()) {
        for (int c = 0; c < htps.size(); c++) {
          conn.update("delete from \"web.tomcat\".\"ContextParameter\" where id=?", htps.getInt(c));
        }
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_PARAMETERS, account, linuxServer, false);
      }

      // web.tomcat.Context
      IntList htcs = conn.queryIntList("select id from \"web.tomcat\".\"Context\" where tomcat_site=?", site);
      if (!htcs.isEmpty()) {
        for (int c = 0; c < htcs.size(); c++) {
          conn.update("delete from \"web.tomcat\".\"Context\" where id=?", htcs.getInt(c));
        }
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_CONTEXTS, account, linuxServer, false);
      }

      // web.tomcat.Worker
      IntList httpdWorkers = conn.queryIntList("select bind from \"web.tomcat\".\"Worker\" where \"tomcatSite\"=?", site);
      if (!httpdWorkers.isEmpty()) {
        for (int c = 0; c < httpdWorkers.size(); c++) {
          int bind = httpdWorkers.getInt(c);
          conn.update("delete from \"web.tomcat\".\"Worker\" where bind=?", bind);
          NetBindHandler.removeBind(conn, invalidateList, bind);
        }
        invalidateList.addTable(conn, Table.TableId.HTTPD_WORKERS, account, linuxServer, false);
      }

      // web.tomcat.SharedTomcatSite
      if (conn.update("delete from \"web.tomcat\".\"SharedTomcatSite\" where tomcat_site=?", site) > 0) {
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SHARED_SITES, account, linuxServer, false);
      }

      // web.tomcat.PrivateTomcatSite
      if (conn.queryBoolean("select (select tomcat_site from \"web.tomcat\".\"PrivateTomcatSite\" where tomcat_site=? limit 1) is not null", site)) {
        int tomcat4ShutdownPort = conn.queryInt("select coalesce(tomcat4_shutdown_port, -1) from \"web.tomcat\".\"PrivateTomcatSite\" where tomcat_site=?", site);

        conn.update("delete from \"web.tomcat\".\"PrivateTomcatSite\" where tomcat_site=?", site);
        invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_STD_SITES, account, linuxServer, false);

        if (tomcat4ShutdownPort != -1) {
          conn.update("delete from net.\"Bind\" where id=?", tomcat4ShutdownPort);
          invalidateList.addTable(conn, Table.TableId.NET_BINDS, account, linuxServer, false);
          invalidateList.addTable(conn, Table.TableId.NET_BIND_FIREWALLD_ZONES, account, linuxServer, false);
        }
      }

      // web.jboss.Site
      if (conn.queryBoolean("select (select tomcat_site from \"web.jboss\".\"Site\" where tomcat_site=? limit 1) is not null", site)) {
        // net.Bind
        final int jnp_bind = conn.queryInt("select jnp_bind from \"web.jboss\".\"Site\" where tomcat_site=?", site);
        final int webserver_bind = conn.queryInt("select webserver_bind from \"web.jboss\".\"Site\" where tomcat_site=?", site);
        final int rmi_bind = conn.queryInt("select rmi_bind from \"web.jboss\".\"Site\" where tomcat_site=?", site);
        final int hypersonic_bind = conn.queryInt("select hypersonic_bind from \"web.jboss\".\"Site\" where tomcat_site=?", site);
        final int jmx_bind = conn.queryInt("select jmx_bind from \"web.jboss\".\"Site\" where tomcat_site=?", site);

        conn.update("delete from \"web.jboss\".\"Site\" where tomcat_site=?", site);
        invalidateList.addTable(conn, Table.TableId.HTTPD_JBOSS_SITES, account, linuxServer, false);
        NetBindHandler.removeBind(conn, invalidateList, jnp_bind);
        NetBindHandler.removeBind(conn, invalidateList, webserver_bind);
        NetBindHandler.removeBind(conn, invalidateList, rmi_bind);
        NetBindHandler.removeBind(conn, invalidateList, hypersonic_bind);
        NetBindHandler.removeBind(conn, invalidateList, jmx_bind);
      }

      conn.update("delete from \"web.tomcat\".\"Site\" where httpd_site=?", site);
      invalidateList.addTable(conn, Table.TableId.HTTPD_TOMCAT_SITES, account, linuxServer, false);
    }

    // web.StaticSite
    if (conn.update("delete from web.\"StaticSite\" where httpd_site=?", site) != 0) {
      invalidateList.addTable(conn, Table.TableId.HTTPD_STATIC_SITES, account, linuxServer, false);
    }

    // web.Site
    conn.update("delete from web.\"Site\" where id=?", site);
    invalidateList.addTable(conn, Table.TableId.HTTPD_SITES, account, linuxServer, false);
  }

  public static void removeHttpdServer(DatabaseConnection conn, InvalidateList invalidateList, int httpdServer) throws IOException, SQLException {
    Account.Name account = getAccountForHttpdServer(conn, httpdServer);
    int linuxServer = getLinuxServerForHttpdServer(conn, httpdServer);

    // web.Site
    conn.update("delete from web.\"HttpdServer\" where id=?", httpdServer);
    invalidateList.addTable(conn, Table.TableId.HTTPD_SERVERS, account, linuxServer, false);
  }

  public static void removeLocation(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int location
  ) throws IOException, SQLException {
    int site = conn.queryInt("select httpd_site from web.\"Location\" where id=?", location);
    checkAccessSite(conn, source, "removeLocation", site);

    Account.Name account = getAccountForSite(conn, site);
    int linuxServer = getLinuxServerForSite(conn, site);

    conn.update("delete from web.\"Location\" where id=?", location);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
        account,
        linuxServer,
        false
    );
  }

  public static void removeVirtualHostName(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHostName
  ) throws IOException, SQLException {
    int hs = getSiteForVirtualHostName(conn, virtualHostName);
    checkAccessSite(conn, source, "removeVirtualHostName", hs);
    if (conn.queryBoolean("select is_primary from web.\"VirtualHostName\" where id=?", virtualHostName)) {
      throw new SQLException("Not allowed to remove the primary hostname: " + virtualHostName);
    }
    if (
        conn.queryBoolean(
            "select\n"
                + "  (\n"
                + "    select hostname from web.\"VirtualHostName\" where id=?\n"
                + "  )=(\n"
                + "    select hs.\"name\" || '.' || ao.hostname from web.\"Site\" hs, linux.\"Server\" ao where hs.id=? and hs.ao_server=ao.server\n"
                + "  )",
            virtualHostName,
            hs
        )
    ) {
      throw new SQLException("Not allowed to remove a test URL: " + virtualHostName);
    }

    conn.update("delete from web.\"VirtualHostName\" where id=?", virtualHostName);
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_URLS,
        getAccountForSite(conn, hs),
        getLinuxServerForSite(conn, hs),
        false
    );
  }

  public static void removeContext(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int context
  ) throws IOException, SQLException {
    int tomcat_site = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "removeContext", tomcat_site);
    String path = conn.queryString("select path from \"web.tomcat\".\"Context\" where id=?", context);
    if (path.isEmpty()) {
      throw new SQLException("Not allowed to remove the default context: " + context);
    }

    Account.Name account = getAccountForSite(conn, tomcat_site);
    int linuxServer = getLinuxServerForSite(conn, tomcat_site);

    if (conn.update("delete from \"web.tomcat\".\"ContextDataSource\" where tomcat_context=?", context) > 0) {
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_TOMCAT_DATA_SOURCES,
          account,
          linuxServer,
          false
      );
    }

    if (conn.update("delete from \"web.tomcat\".\"ContextParameter\" where tomcat_context=?", context) > 0) {
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_TOMCAT_PARAMETERS,
          account,
          linuxServer,
          false
      );
    }

    conn.update("delete from \"web.tomcat\".\"Context\" where id=?", context);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_CONTEXTS,
        account,
        linuxServer,
        false
    );

    if (
        conn.update(
            "delete from \"web.tomcat\".\"JkMount\" where httpd_tomcat_site=? and substring(path from 1 for ?)=?",
            tomcat_site,
            path.length() + 1,
            path + '/'
        ) > 0
    ) {
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
          account,
          linuxServer,
          false
      );
    }
  }

  public static void removeContextDataSource(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int contextDataSource
  ) throws IOException, SQLException {
    int context = conn.queryInt("select tomcat_context from \"web.tomcat\".\"ContextDataSource\" where id=?", contextDataSource);
    int tomcatSite = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "removeContextDataSource", tomcatSite);

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    conn.update("delete from \"web.tomcat\".\"ContextDataSource\" where id=?", contextDataSource);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_DATA_SOURCES,
        account,
        linuxServer,
        false
    );
  }

  public static void removeContextParameter(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int contextParameter
  ) throws IOException, SQLException {
    int context = conn.queryInt("select tomcat_context from \"web.tomcat\".\"ContextParameter\" where id=?", contextParameter);
    int tomcatSite = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "removeContextParameter", tomcatSite);

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    conn.update("delete from \"web.tomcat\".\"ContextParameter\" where id=?", contextParameter);
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_PARAMETERS,
        account,
        linuxServer,
        false
    );
  }

  public static void updateContextDataSource(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int contextDataSource,
      String name,
      String driverClassName,
      String url,
      String username,
      String password,
      int maxActive,
      int maxIdle,
      int maxWait,
      String validationQuery
  ) throws IOException, SQLException {
    int context = conn.queryInt("select tomcat_context from \"web.tomcat\".\"ContextDataSource\" where id=?", contextDataSource);
    int tomcatSite = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "updateContextDataSource", tomcatSite);

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    conn.update(
        "update \"web.tomcat\".\"ContextDataSource\" set name=?, driver_class_name=?, url=?, username=?, password=?, max_active=?, max_idle=?, max_wait=?, validation_query=? where id=?",
        name,
        driverClassName,
        url,
        username,
        password,
        maxActive,
        maxIdle,
        maxWait,
        validationQuery,
        contextDataSource
    );
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_DATA_SOURCES,
        account,
        linuxServer,
        false
    );
  }

  public static void updateContextParameter(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int contextParameter,
      String name,
      String value,
      boolean override,
      String description
  ) throws IOException, SQLException {
    int context = conn.queryInt("select tomcat_context from \"web.tomcat\".\"ContextParameter\" where id=?", contextParameter);
    int tomcatSite = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "updateContextParameter", tomcatSite);

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    conn.update(
        "update \"web.tomcat\".\"ContextParameter\" set name=?, value=?, override=?, description=? where id=?",
        name,
        value,
        override,
        description,
        contextParameter
    );
    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_PARAMETERS,
        account,
        linuxServer,
        false
    );
  }

  public static void restartApache(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_apache");
    if (!canControl) {
      throw new SQLException("Not allowed to restart Apache on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.restartApache();
  }

  public static void setSharedTomcatIsManual(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      boolean isManual
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatIsManual", sharedTomcat);
    if (isSharedTomcatDisabled(conn, sharedTomcat)) {
      throw new SQLException("Unable to set is_manual flag: SharedTomcat disabled: " + sharedTomcat);
    }

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set is_manual=? where id=?",
        isManual,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatMaxParameterCount(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      int maxParameterCount
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatMaxParameterCount", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set \"maxParameterCount\"=? where id=?",
        maxParameterCount == -1 ? DatabaseAccess.Null.INTEGER : maxParameterCount,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatMaxPostSize(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      int maxPostSize
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatMaxPostSize", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set max_post_size=? where id=?",
        maxPostSize == -1 ? DatabaseAccess.Null.INTEGER : maxPostSize,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatUnpackWars(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      boolean unpackWars
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatUnpackWars", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set unpack_wars=? where id=?",
        unpackWars,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatAutoDeploy(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      boolean autoDeploy
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatAutoDeploy", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set auto_deploy=? where id=?",
        autoDeploy,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatUndeployOldVersions(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      boolean undeployOldVersions
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatUndeployOldVersions", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set \"undeployOldVersions\"=? where id=?",
        undeployOldVersions,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  public static void setSharedTomcatTomcatAuthentication(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      boolean tomcatAuthentication
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatTomcatAuthentication", sharedTomcat);

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set \"tomcatAuthentication\"=? where id=?",
        tomcatAuthentication,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        getLinuxServerForSharedTomcat(conn, sharedTomcat),
        false
    );
  }

  private static void checkUpgradeFrom(String fromVersion) throws SQLException {
    if (!Version.canUpgradeFrom(fromVersion)) {
      throw new SQLException("In-place Tomcat upgrades and downgrades are only supported from Tomcat 4.1 and newer, not supported from version \"" + fromVersion + "\".");
    }
  }

  private static void checkUpgradeTo(String toVersion) throws SQLException {
    if (!Version.canUpgradeTo(toVersion)) {
      throw new SQLException("In-place Tomcat upgrades and downgrades are only supported to Tomcat 8.5 and newer, not supported to version \"" + toVersion + "\".");
    }
  }

  public static void setSharedTomcatVersion(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int sharedTomcat,
      int version
  ) throws IOException, SQLException {
    checkAccessSharedTomcat(conn, source, "setSharedTomcatVersion", sharedTomcat);

    // Make sure the version change is acceptable
    checkUpgradeFrom(
        conn.queryString(
            "select\n"
                + "  tv.version\n"
                + "from\n"
                + "  \"web.tomcat\".\"SharedTomcat\" hst\n"
                + "  inner join distribution.\"SoftwareVersion\" tv on hst.version=tv.id\n"
                + "where hst.id=?",
            sharedTomcat
        )
    );
    checkUpgradeTo(
        conn.queryString(
            "select\n"
                + "  tv.version\n"
                + "from\n"
                + "  \"web.tomcat\".\"Version\" htv\n"
                + "  inner join distribution.\"SoftwareVersion\" tv on htv.version=tv.id\n"
                + "where htv.version=?",
            version
        )
    );

    // Make sure operating system version matches
    int linuxServer = getLinuxServerForSharedTomcat(conn, sharedTomcat);
    int fromOsv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    int toOsv = conn.queryInt(
        "select operating_system_version from distribution.\"SoftwareVersion\" where id=?",
        version
    );
    if (fromOsv != toOsv) {
      throw new SQLException("OperatingSystemVersion mismatch: " + fromOsv + " != " + toOsv);
    }
    // TODO: Check osv match on adding new sites (Tomcat versions)

    // TODO: Check if disabled in distribution.SoftwareVersion (do this in set PHP version, too)
    // TODO: Add update of this in end-of-life tasks
    // TODO: Check this "disable_time" in control panels, too.
    // TODO: Check this on add site (both PHP and Tomcat versions)

    // Update the database
    conn.update(
        "update \"web.tomcat\".\"SharedTomcat\" set version=? where id=?",
        version,
        sharedTomcat
    );
    // TODO: Update the context paths to an webapps in /opt/apache-tomcat.../webpaps to the new version
    // TODO: See web.tomcat.Version table
    conn.update(
        "update \"web.tomcat\".\"Site\" set version=? where httpd_site in (\n"
            + "  select tomcat_site from \"web.tomcat\".\"SharedTomcatSite\" where httpd_shared_tomcat=?\n"
            + ")",
        version,
        sharedTomcat
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SHARED_TOMCATS,
        getAccountForSharedTomcat(conn, sharedTomcat),
        linuxServer,
        false
    );
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_SITES,
        InvalidateList.allAccounts, // TODO: Could be more selective here
        linuxServer,
        false
    );
  }

  public static void setLocationAttributes(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int location,
      String path,
      boolean isRegularExpression,
      String authName,
      PosixPath authGroupFile,
      PosixPath authUserFile,
      String require,
      String handler
  ) throws IOException, SQLException {
    int httpd_site = conn.queryInt("select httpd_site from web.\"Location\" where id=?", location);
    checkAccessSite(conn, source, "setLocationAttributes", httpd_site);
    if (isSiteDisabled(conn, httpd_site)) {
      throw new SQLException("Unable to set Location attributes, Site disabled: " + httpd_site);
    }
    String error = Location.validatePath(path);
    if (error == null) {
      error = Location.validateAuthName(authName);
    }
    if (error == null) {
      error = Location.validateAuthGroupFile(authGroupFile);
    }
    if (error == null) {
      error = Location.validateAuthUserFile(authUserFile);
    }
    if (error == null) {
      error = Location.validateRequire(require);
    }
    if (error != null) {
      throw new SQLException("Unable to add Location: " + error);
    }
    if (Location.Handler.CURRENT.equals(handler)) {
      conn.update(
          "update\n"
              + "  web.\"Location\"\n"
              + "set\n"
              + "  path=?,\n"
              + "  is_regular_expression=?,\n"
              + "  auth_name=?,\n"
              + "  auth_group_file=?,\n"
              + "  auth_user_file=?,\n"
              + "  require=?\n"
              + "where\n"
              + "  id=?",
          path,
          isRegularExpression,
          authName,
          authGroupFile == null ? "" : authGroupFile.toString(),
          authUserFile == null ? "" : authUserFile.toString(),
          require,
          location
      );
    } else {
      conn.update(
          "update\n"
              + "  web.\"Location\"\n"
              + "set\n"
              + "  path=?,\n"
              + "  is_regular_expression=?,\n"
              + "  auth_name=?,\n"
              + "  auth_group_file=?,\n"
              + "  auth_user_file=?,\n"
              + "  require=?,\n"
              + "  handler=?\n"
              + "where\n"
              + "  id=?",
          path,
          isRegularExpression,
          authName,
          authGroupFile == null ? "" : authGroupFile.toString(),
          authUserFile == null ? "" : authUserFile.toString(),
          require,
          handler,
          location
      );
    }

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
        getAccountForSite(conn, httpd_site),
        getLinuxServerForSite(conn, httpd_site),
        false
    );
  }

  public static void setVirtualHostIsManual(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHost,
      boolean isManual
  ) throws IOException, SQLException {
    int site = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "setVirtualHostIsManual", site);
    if (isVirtualHostDisabled(conn, virtualHost)) {
      throw new SQLException("Unable to set is_manual flag: VirtualHost disabled: " + virtualHost);
    }

    // Update the database
    conn.update(
        "update web.\"VirtualHost\" set is_manual=? where id=?",
        isManual,
        virtualHost
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_BINDS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setVirtualHostRedirectToPrimaryHostname(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHost,
      boolean redirectToPrimaryHostname
  ) throws IOException, SQLException {
    int hs = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "setVirtualHostRedirectToPrimaryHostname", hs);
    if (isVirtualHostDisabled(conn, virtualHost)) {
      throw new SQLException("Unable to set redirect_to_primary_hostname flag: VirtualHost disabled: " + virtualHost);
    }

    // Update the database
    conn.update(
        "update web.\"VirtualHost\" set redirect_to_primary_hostname=? where id=?",
        redirectToPrimaryHostname,
        virtualHost
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_BINDS,
        getAccountForSite(conn, hs),
        getLinuxServerForSite(conn, hs),
        false
    );
  }

  public static void setVirtualHostPredisableConfig(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHost,
      String config
  ) throws IOException, SQLException {
    int site = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "setVirtualHostPredisableConfig", site);
    if (config == null) {
      if (isVirtualHostDisabled(conn, virtualHost)) {
        throw new SQLException("Unable to clear VirtualHost predisable config, bind disabled: " + virtualHost);
      }
    } else if (!isVirtualHostDisabled(conn, virtualHost)) {
      throw new SQLException("Unable to set VirtualHost predisable config, bind not disabled: " + virtualHost);
    }

    // Update the database
    conn.update(
        "update web.\"VirtualHost\" set predisable_config=? where id=?",
        config,
        virtualHost
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_BINDS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteIsManual(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean isManual
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteIsManual", site);
    if (isSiteDisabled(conn, site)) {
      throw new SQLException("Unable to set is_manual flag: Site disabled: " + site);
    }

    // Update the database
    conn.update(
        "update web.\"Site\" set is_manual=? where id=?",
        isManual,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteServerAdmin(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      // TODO: Support a reference to email.Address as an alternative?  Could help identify what used by
      Email serverAdmin
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteServerAdmin", site);
    if (isSiteDisabled(conn, site)) {
      throw new SQLException("Unable to set server administrator: Site disabled: " + site);
    }

    // Update the database
    conn.update(
        "update web.\"Site\" set server_admin=? where id=?",
        serverAdmin.toString(),
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSitePhpVersion(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      int phpVersion
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSitePhpVersion", site);
    int linuxServer = getLinuxServerForSite(conn, site);
    if (phpVersion != -1) {
      if (isStaticSite(conn, site)) {
        // TODO: This would be better modeled by not having php_version on the web.Site table, but rather more specialized types of sites
        // TODO: How to enable PHP on a per-site basis, so static site under mod_php apache doesn't get php?
        throw new SQLException("May not enable PHP on a static site");
      }
      int osv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
      // Version must be correct for this server
      int tvOsv = conn.queryInt(
          "select coalesce(\n"
              + "  (select operating_system_version from distribution.\"SoftwareVersion\" where id=? and name=?),\n"
              + "  -1\n"
              + ")",
          phpVersion,
          Software.PHP
      );
      if (tvOsv == -1) {
        throw new SQLException("Requested PHP version is not a PHP version: #" + phpVersion);
      }
      if (tvOsv != osv) {
        throw new SQLException("Requested PHP version is for the wrong operating system version: #" + phpVersion + ": " + tvOsv + " != " + osv);
      }
    }
    // Update the database
    int updateCount;
    if (phpVersion == -1) {
      updateCount = conn.update(
          "update web.\"Site\" set php_version=null where id=? and php_version is not null",
          site
      );
    } else {
      updateCount = conn.update(
          "update web.\"Site\" set php_version=? where id=? and php_version != ?",
          phpVersion,
          site,
          phpVersion
      );
    }
    if (updateCount > 0) {
      Account.Name account = getAccountForSite(conn, site);
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_SITES,
          account,
          linuxServer,
          false
      );

      boolean useApache = conn.queryBoolean(
          "select (\n"
              + "  select id from \"web.tomcat\".\"JkMount\"\n"
              + "  where (httpd_tomcat_site, path)=(?, '/*')\n"
              + ") is null",
          site
      );
      boolean hasPhp;
      if (phpVersion != -1) {
        // CGI-based PHP
        hasPhp = true;
      } else {
        // Check for mod_php
        hasPhp = conn.queryBoolean(
            "select (\n"
                + "  select\n"
                + "    hs.id\n"
                + "  from\n"
                + "    web.\"VirtualHost\" hsb\n"
                + "    inner join web.\"HttpdBind\" hb on hsb.httpd_bind=hb.net_bind\n"
                + "    inner join web.\"HttpdServer\" hs on hb.httpd_server=hs.id\n"
                + "  where\n"
                + "    hsb.httpd_site=?\n"
                + "    and hs.mod_php_version is not null\n"
                + "  limit 1\n"
                + ") is not null",
            site
        );
      }
      if (!useApache && hasPhp) {
        if (
            conn.queryBoolean(
                "select (\n"
                    + "  select id from \"web.tomcat\".\"JkMount\"\n"
                    + "  where (httpd_tomcat_site, path)=(?, '/*.php')\n"
                    + ") is null",
                site
            )
        ) {
          // Add /*.php to JkUnMounts
          conn.update(
              "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,'/*.php',FALSE)",
              site
          );
          invalidateList.addTable(
              conn,
              Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
              account,
              linuxServer,
              false
          );
        }
      } else {
        // Remove /*.php from JkUnMounts
        if (
            conn.update(
                "delete from \"web.tomcat\".\"JkMount\" where (httpd_tomcat_site, path, mount)=(?,'/*.php',FALSE)",
                site
            ) > 0
        ) {
          invalidateList.addTable(
              conn,
              Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
              account,
              linuxServer,
              false
          );
        }
      }
    }
  }

  public static void setSiteEnableCgi(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableCgi
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableCgi", site);

    if (enableCgi && isStaticSite(conn, site)) {
      // TODO: This would be better modeled by not having enable_cgi on the web.Site table, but rather more specialized types of sites
      throw new SQLException("May not enable CGI on a static site");
    }

    // Update the database
    if (
        conn.update(
            "update web.\"Site\" set enable_cgi=? where id=? and enable_cgi != ?",
            enableCgi,
            site,
            enableCgi
        ) > 0
    ) {
      Account.Name account = getAccountForSite(conn, site);
      int linuxServer = getLinuxServerForSite(conn, site);
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_SITES,
          account,
          linuxServer,
          false
      );
      List<String> paths = conn.queryStringList("select path from \"web.tomcat\".\"Context\" where tomcat_site=?", site);
      if (!paths.isEmpty()) {
        for (String path : paths) {
          if (enableCgi) {
            // Add /cgi-bin to JkUnMounts
            conn.update(
                "insert into \"web.tomcat\".\"JkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
                site,
                checkJkMountPath(path + "/cgi-bin/*")
            );
          } else {
            // Remove /cgi-bin from JkUnMounts
            conn.update(
                "delete from \"web.tomcat\".\"JkMount\" where (httpd_tomcat_site, path, mount)=(?,?,FALSE)",
                site,
                checkJkMountPath(path + "/cgi-bin/*")
            );
          }
        }
      }
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
          account,
          linuxServer,
          false
      );
    }
  }

  public static void setSiteEnableSsi(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableSsi
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableSsi", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set enable_ssi=? where id=?",
        enableSsi,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteEnableHtaccess(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableHtaccess
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableHtaccess", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set enable_htaccess=? where id=?",
        enableHtaccess,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteEnableIndexes(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableIndexes
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableIndexes", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set enable_indexes=? where id=?",
        enableIndexes,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteEnableFollowSymlinks(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableFollowSymlinks
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableFollowSymlinks", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set enable_follow_symlinks=? where id=?",
        enableFollowSymlinks,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteEnableAnonymousFtp(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean enableAnonymousFtp
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteEnableAnonymousFtp", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set enable_anonymous_ftp=? where id=?",
        enableAnonymousFtp,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteBlockTraceTrack(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean blockTraceTrack
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteBlockTraceTrack", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set block_trace_track=? where id=?",
        blockTraceTrack,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteBlockScm(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean blockScm
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteBlockScm", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set block_scm=? where id=?",
        blockScm,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteBlockCoreDumps(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean blockCoreDumps
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteBlockCoreDumps", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set block_core_dumps=? where id=?",
        blockCoreDumps,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setSiteBlockEditorBackups(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int site,
      boolean blockEditorBackups
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setSiteBlockEditorBackups", site);

    // Update the database
    conn.update(
        "update web.\"Site\" set block_editor_backups=? where id=?",
        blockEditorBackups,
        site
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITES,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static int setContextAttributes(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int context,
      String className,
      boolean cookies,
      boolean crossContext,
      PosixPath docBase,
      boolean override,
      String path,
      boolean privileged,
      boolean reloadable,
      boolean useNaming,
      String wrapperClass,
      int debug,
      PosixPath workDir,
      boolean serverXmlConfigured
  ) throws IOException, SQLException {
    int tomcatSite = conn.queryInt("select tomcat_site from \"web.tomcat\".\"Context\" where id=?", context);
    checkAccessSite(conn, source, "setContextAttributes", tomcatSite);
    if (isSiteDisabled(conn, tomcatSite)) {
      throw new SQLException("Unable to set Context attributes, Site disabled: " + tomcatSite);
    }
    checkContext(
        conn,
        source,
        tomcatSite,
        className,
        crossContext,
        docBase,
        override,
        path,
        privileged,
        wrapperClass,
        workDir,
        serverXmlConfigured
    );

    Account.Name account = getAccountForSite(conn, tomcatSite);
    int linuxServer = getLinuxServerForSite(conn, tomcatSite);

    String oldPath = conn.queryString("select path from \"web.tomcat\".\"Context\" where id=?", context);
    if (oldPath.length() == 0 && path.length() > 0) {
      throw new SQLException("Not allowed to change the path of the default context: " + path);
    }

    try (PreparedStatement pstmt = conn.getConnection().prepareStatement(
        "update\n"
            + "  \"web.tomcat\".\"Context\"\n"
            + "set\n"
            + "  class_name=?,\n"
            + "  cookies=?,\n"
            + "  cross_context=?,\n"
            + "  doc_base=?,\n"
            + "  override=?,\n"
            + "  path=?,\n"
            + "  privileged=?,\n"
            + "  reloadable=?,\n"
            + "  use_naming=?,\n"
            + "  wrapper_class=?,\n"
            + "  debug=?,\n"
            + "  work_dir=?,\n"
            + "  server_xml_configured=?\n"
            + "where\n"
            + "  id=?"
    )) {
      try {
        pstmt.setString(1, className);
        pstmt.setBoolean(2, cookies);
        pstmt.setBoolean(3, crossContext);
        pstmt.setString(4, docBase.toString());
        pstmt.setBoolean(5, override);
        pstmt.setString(6, path);
        pstmt.setBoolean(7, privileged);
        pstmt.setBoolean(8, reloadable);
        pstmt.setBoolean(9, useNaming);
        pstmt.setString(10, wrapperClass);
        pstmt.setInt(11, debug);
        pstmt.setString(12, Objects.toString(workDir, null));
        pstmt.setBoolean(13, serverXmlConfigured);
        pstmt.setInt(14, context);

        pstmt.executeUpdate();
      } catch (Error | RuntimeException | SQLException e) {
        ErrorPrinter.addSql(e, pstmt);
        throw e;
      }
    }

    if (
        !path.equals(oldPath)
            && conn.update(
            "update \"web.tomcat\".\"JkMount\"\n"
                + "set path = ? || substring(path from ?)\n"
                + "where httpd_tomcat_site=? and substring(path from 1 for ?)=?",
            path,
            oldPath.length(),
            tomcatSite,
            oldPath.length() + 1,
            oldPath + '/'
        ) > 0
    ) {
      invalidateList.addTable(
          conn,
          Table.TableId.HTTPD_TOMCAT_SITE_JK_MOUNTS,
          account,
          linuxServer,
          false
      );
    }

    invalidateList.addTable(
        conn,
        Table.TableId.HTTPD_TOMCAT_CONTEXTS,
        account,
        linuxServer,
        false
    );

    return context;
  }

  public static void setPrivateTomcatSiteMaxParameterCount(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      int maxParameterCount
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteMaxParameterCount", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set \"maxParameterCount\"=? where tomcat_site=?",
        maxParameterCount == -1 ? DatabaseAccess.Null.INTEGER : maxParameterCount,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteMaxPostSize(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      int maxPostSize
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteMaxPostSize", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set max_post_size=? where tomcat_site=?",
        maxPostSize == -1 ? DatabaseAccess.Null.INTEGER : maxPostSize,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteUnpackWars(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      boolean unpackWars
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteUnpackWars", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set unpack_wars=? where tomcat_site=?",
        unpackWars,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteAutoDeploy(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      boolean autoDeploy
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteAutoDeploy", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set auto_deploy=? where tomcat_site=?",
        autoDeploy,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteUndeployOldVersions(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      boolean undeployOldVersions
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteUndeployOldVersions", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set \"undeployOldVersions\"=? where tomcat_site=?",
        undeployOldVersions,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteTomcatAuthentication(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      boolean tomcatAuthentication
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setPrivateTomcatSiteTomcatAuthentication", privateTomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"PrivateTomcatSite\" set \"tomcatAuthentication\"=? where tomcat_site=?",
        tomcatAuthentication,
        privateTomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a PrivateTomcatSite: #" + privateTomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_STD_SITES,
        getAccountForSite(conn, privateTomcatSite),
        getLinuxServerForSite(conn, privateTomcatSite),
        false
    );
  }

  public static void setPrivateTomcatSiteVersion(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int privateTomcatSite,
      int version
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setHttpdTomcatStdSiteVersion", privateTomcatSite);

    // Make sure the version change is acceptable
    checkUpgradeFrom(
        conn.queryString(
            "select\n"
                + "  tv.version\n"
                + "from\n"
                + "  \"web.tomcat\".\"PrivateTomcatSite\" htss\n"
                + "  inner join \"web.tomcat\".\"Site\" hts on htss.tomcat_site=hts.httpd_site\n"
                + "  inner join distribution.\"SoftwareVersion\" tv on hts.version=tv.id\n"
                + "where htss.tomcat_site=?",
            privateTomcatSite
        )
    );
    checkUpgradeTo(
        conn.queryString(
            "select\n"
                + "  tv.version\n"
                + "from\n"
                + "  \"web.tomcat\".\"Version\" htv\n"
                + "  inner join distribution.\"SoftwareVersion\" tv on htv.version=tv.id\n"
                + "where htv.version=?",
            version
        )
    );

    // Make sure operating system version matches
    int linuxServer = getLinuxServerForSite(conn, privateTomcatSite);
    int fromOsv = NetHostHandler.getOperatingSystemVersionForHost(conn, linuxServer);
    int toOsv = conn.queryInt(
        "select operating_system_version from distribution.\"SoftwareVersion\" where id=?",
        version
    );
    if (fromOsv != toOsv) {
      throw new SQLException("OperatingSystemVersion mismatch: " + fromOsv + " != " + toOsv);
    }
    // TODO: Check osv match on adding new sites (Tomcat versions)

    // TODO: Check if disabled in distribution.SoftwareVersion (do this in set PHP version, too)
    // TODO: Add update of this in end-of-life tasks
    // TODO: Check this "disable_time" in control panels, too.
    // TODO: Check this on add site (both PHP and Tomcat versions)

    // Update the database
    // TODO: Update the context paths to an webapps in /opt/apache-tomcat.../webpaps to the new version
    // TODO: See web.tomcat.Version table (might shared with the same code above)
    conn.update(
        "update \"web.tomcat\".\"Site\" set version=? where httpd_site=?",
        version,
        privateTomcatSite
    );

    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_SITES,
        getAccountForSite(conn, privateTomcatSite),
        linuxServer,
        false
    );
  }

  public static void setPrimaryVirtualHostName(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int virtualHostName
  ) throws IOException, SQLException {
    int virtualHost = conn.queryInt("select httpd_site_bind from web.\"VirtualHostName\" where id=?", virtualHostName);
    int site = getSiteForVirtualHost(conn, virtualHost);
    checkAccessSite(conn, source, "setPrimaryVirtualHostName", site);

    conn.update("update web.\"VirtualHostName\" set is_primary=false where is_primary and httpd_site_bind=?", virtualHost);
    conn.update("update web.\"VirtualHostName\" set is_primary=true where id=? and httpd_site_bind=?", virtualHostName, virtualHost);
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_SITE_URLS,
        getAccountForSite(conn, site),
        getLinuxServerForSite(conn, site),
        false
    );
  }

  public static void setTomcatSiteBlockWebinf(
      DatabaseConnection conn,
      RequestSource source,
      InvalidateList invalidateList,
      int tomcatSite,
      boolean blockWebinf
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "setTomcatSiteBlockWebinf", tomcatSite);

    // Update the database
    int updateCount = conn.update(
        "update \"web.tomcat\".\"Site\" set block_webinf=? where httpd_site=?",
        blockWebinf,
        tomcatSite
    );
    if (updateCount == 0) {
      throw new SQLException("Not a Site: #" + tomcatSite);
    }
    if (updateCount != 1) {
      throw new SQLException("Unexpected updateCount: " + updateCount);
    }
    invalidateList.addTable(conn,
        Table.TableId.HTTPD_TOMCAT_SITES,
        getAccountForSite(conn, tomcatSite),
        getLinuxServerForSite(conn, tomcatSite),
        false
    );
  }

  public static void startApache(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_apache");
    if (!canControl) {
      throw new SQLException("Not allowed to start Apache on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.startApache();
  }

  public static void stopApache(
      DatabaseConnection conn,
      RequestSource source,
      int linuxServer
  ) throws IOException, SQLException {
    boolean canControl = AccountHandler.canAccountHost_column(conn, source, linuxServer, "can_control_apache");
    if (!canControl) {
      throw new SQLException("Not allowed to stop Apache on " + linuxServer);
    }
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.stopApache();
  }

  public static void getAwstatsFile(
      DatabaseConnection conn,
      RequestSource source,
      int site,
      String path,
      String queryString,
      StreamableOutput out
  ) throws IOException, SQLException {
    checkAccessSite(conn, source, "getAwstatsFile", site);

    String siteName = getNameForSite(conn, site);
    AoservDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn,
        getLinuxServerForSite(conn, site)
    );
    conn.close(); // Don't hold database connection while connecting to the daemon
    daemonConnector.getAwstatsFile(siteName, path, queryString, out);
  }
}
