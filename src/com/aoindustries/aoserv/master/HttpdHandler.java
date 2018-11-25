/*
 * Copyright 2001-2013, 2014, 2015, 2016, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.FirewalldZone;
import com.aoindustries.aoserv.client.HttpdJKProtocol;
import com.aoindustries.aoserv.client.HttpdSharedTomcat;
import com.aoindustries.aoserv.client.HttpdSite;
import com.aoindustries.aoserv.client.HttpdSiteAuthenticatedLocation;
import com.aoindustries.aoserv.client.HttpdTomcatContext;
import com.aoindustries.aoserv.client.HttpdTomcatSiteJkMount;
import com.aoindustries.aoserv.client.HttpdTomcatStdSite;
import com.aoindustries.aoserv.client.HttpdTomcatVersion;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.TechnologyName;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseAccess.Null;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.lang.ObjectUtils;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.Email;
import com.aoindustries.net.Port;
import com.aoindustries.security.Identifier;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import com.aoindustries.util.SortedIntArrayList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The <code>HttpdHandler</code> handles all the accesses to the HTTPD tables.
 *
 * @author  AO Industries, Inc.
 */
final public class HttpdHandler {

	/**
	 * Make no instances.
	 */
	private HttpdHandler() {
	}

	/**
	 * The first port number that may be used for automatic port allocations.
	 */
	public static final int MINIMUM_AUTO_PORT_NUMBER = 16384;

	private final static Map<Integer,Boolean> disabledHttpdSharedTomcats = new HashMap<>();
	private final static Map<Integer,Boolean> disabledHttpdSiteBinds = new HashMap<>();
	private final static Map<Integer,Boolean> disabledHttpdSites = new HashMap<>();

	public static void addTomcatWorker(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int netBindPKey,
		int httpdSitePKey
	) throws IOException, SQLException {
		int server=NetBindHandler.getServerForNetBind(conn, netBindPKey);
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
		int aoServer = server;
		if(httpdSitePKey == -1) {
			conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  \"web/tomcat\".\"TomcatWorker\"\n"
				+ "VALUES (\n"
				+ "  ?,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      hjc.code\n"
				+ "    from\n"
				+ "      \"web/tomcat\".\"TomcatWorkerName\" hjc\n"
				+ "    where\n"
				+ "      (\n"
				+ "        select\n"
				+ "          hw.\"name\"\n"
				+ "        from\n"
				+ "          \"web/tomcat\".\"TomcatWorker\" hw,\n"
				+ "          net.\"Bind\" nb\n"
				+ "        where\n"
				+ "          hw.bind=nb.pkey\n"
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
				netBindPKey,
				aoServer
			);
		} else {
			conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  \"web/tomcat\".\"TomcatWorker\"\n"
				+ "VALUES (\n"
				+ "  ?,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      hjc.code\n"
				+ "    from\n"
				+ "      \"web/tomcat\".\"TomcatWorkerName\" hjc\n"
				+ "    where\n"
				+ "      (\n"
				+ "        select\n"
				+ "          hw.\"name\"\n"
				+ "        from\n"
				+ "          \"web/tomcat\".\"TomcatWorker\" hw,\n"
				+ "          net.\"Bind\" nb\n"
				+ "        where\n"
				+ "          hw.bind=nb.pkey\n"
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
				netBindPKey,
				aoServer,
				httpdSitePKey
			);
		}
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_WORKERS,
			NetBindHandler.getBusinessForNetBind(conn, netBindPKey),
			aoServer,
			false
		);
	}

	public static int addHttpdSiteURL(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int hsb_pkey,
		DomainName hostname
	) throws IOException, SQLException {
		int hs=getHttpdSiteForHttpdSiteBind(conn, hsb_pkey);
		checkAccessHttpdSite(conn, source, "addHttpdSiteURL", hs);
		if(isHttpdSiteBindDisabled(conn, hsb_pkey)) throw new SQLException("Unable to add HttpdSiteURL, HttpdSiteBind disabled: "+hsb_pkey);
		MasterServer.checkAccessHostname(conn, source, "addHttpdSiteURL", hostname.toString());

		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  web.\"VirtualHostName\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  (select pkey from web.\"VirtualHostName\" where httpd_site_bind=? and is_primary limit 1) is null\n"
			+ ") RETURNING pkey",
			hsb_pkey,
			hostname,
			hsb_pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_URLS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);

		return pkey;
	}

	public static void checkAccessHttpdSharedTomcat(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
		if(
			!LinuxAccountHandler.canAccessLinuxServerGroup(
				conn,
				source,
				conn.executeIntQuery("select linux_server_group from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey)
			)
		) {
			String message=
				"business_administrator.username="
				+source.getUsername()
				+" is not allowed to access httpd_shared_tomcat: action='"
				+action
				+"', pkey="
				+pkey
			;
			throw new SQLException(message);
		}
	}

	public static boolean canAccessHttpdSite(DatabaseConnection conn, RequestSource source, int httpdSite) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				return ServerHandler.canAccessServer(conn, source, getAOServerForHttpdSite(conn, httpdSite));
			} else {
				return true;
			}
		} else {
			return PackageHandler.canAccessPackage(conn, source, getPackageForHttpdSite(conn, httpdSite));
		}
	}

	public static void checkAccessHttpdServer(DatabaseConnection conn, RequestSource source, String action, int httpdServer) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu != null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length != 0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForHttpdServer(conn, httpdServer));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForHttpdServer(conn, httpdServer));
		}
	}

	public static void checkAccessHttpdSite(DatabaseConnection conn, RequestSource source, String action, int httpdSite) throws IOException, SQLException {
		MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
		if(mu!=null) {
			if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
				ServerHandler.checkAccessServer(conn, source, action, getAOServerForHttpdSite(conn, httpdSite));
			}
		} else {
			PackageHandler.checkAccessPackage(conn, source, action, getPackageForHttpdSite(conn, httpdSite));
		}
	}

	public static int getHttpdServerConcurrency(
		DatabaseConnection conn,
		RequestSource source,
		int httpdServer
	) throws IOException, SQLException {
		checkAccessHttpdServer(conn, source, "getHttpdServerConcurrency", httpdServer);
		return DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForHttpdServer(conn, httpdServer)
		).getHttpdServerConcurrency(httpdServer);
	}

	/**
	 * Gets the pkey of a HttpdSite given its server and name or {@code -1} if not found.
	 */
	public static int getHttpdSite(DatabaseConnection conn, int aoServer, String siteName) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select coalesce(\n"
			+ "  (select pkey from web.\"Site\" where (ao_server, \"name\")=(?,?)),\n"
			+ "  -1\n"
			+ ")",
			aoServer,
			siteName
		);
	}

	/**
	 * Gets the pkey of a HttpdSharedTomcat given its server and name or {@code -1} if not found.
	 */
	public static int getHttpdSharedTomcat(DatabaseConnection conn, int aoServer, String name) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select coalesce(\n"
			+ "  (select pkey from \"web/tomcat\".\"SharedTomcat\" where (ao_server, name)=(?,?)),\n"
			+ "  -1\n"
			+ ")",
			aoServer,
			name
		);
	}

	public static int addHttpdSiteAuthenticatedLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int httpd_site,
		String path,
		boolean isRegularExpression,
		String authName,
		UnixPath authGroupFile,
		UnixPath authUserFile,
		String require,
		String handler
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "addHttpdSiteAuthenticatedLocation", httpd_site);
		if(isHttpdSiteDisabled(conn, httpd_site)) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation, HttpdSite disabled: "+httpd_site);
		String error = HttpdSiteAuthenticatedLocation.validatePath(path);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthName(authName);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthGroupFile(authGroupFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthUserFile(authUserFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateRequire(require);
		if(error!=null) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation: "+error);

		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  web.\"Location\"\n"
			+ "VALUES (\n"
			+ "  default,\n" // pkey
			+ "  ?,\n" // httpd_site
			+ "  ?,\n" // path
			+ "  ?,\n" // is_regular_expression
			+ "  ?,\n" // auth_name
			+ "  ?,\n" // auth_group_file
			+ "  ?,\n" // auth_user_file
			+ "  ?,\n" // require
			+ "  ?\n"  // handler
			+ ") RETURNING pkey",
			httpd_site,
			path,
			isRegularExpression,
			authName,
			authGroupFile==null ? "" : authGroupFile.toString(),
			authUserFile==null ? "" : authUserFile.toString(),
			require,
			handler
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
			getBusinessForHttpdSite(conn, httpd_site),
			getAOServerForHttpdSite(conn, httpd_site),
			false
		);

		return pkey;
	}

	/**
	 * Checks a JkMount/JkUnMount path for validity, throwing SQLException if invalid.
	 *
	 * @see  HttpdTomcatSiteJkMount#isValidPath(java.lang.String)
	 */
	public static String checkJkMountPath(String path) throws SQLException {
		if(!HttpdTomcatSiteJkMount.isValidPath(path)) throw new SQLException("Invalid path: " + path);
		return path;
	}

	@SuppressWarnings("deprecation") // Java 1.7: Do not suppress
	public static int addHttpdTomcatContext(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int tomcat_site,
		String className,
		boolean cookies,
		boolean crossContext,
		UnixPath docBase,
		boolean override,
		String path,
		boolean privileged,
		boolean reloadable,
		boolean useNaming,
		String wrapperClass,
		int debug,
		UnixPath workDir,
		boolean serverXmlConfigured
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "addHttpdTomcatContext", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to add HttpdTomcatContext, HttpdSite disabled: "+tomcat_site);
		checkHttpdTomcatContext(
			conn,
			source,
			tomcat_site,
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

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  \"web/tomcat\".\"TomcatContext\"\n"
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
			+ ") RETURNING pkey",
			tomcat_site,
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
			ObjectUtils.toString(workDir),
			serverXmlConfigured
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			accounting,
			aoServer,
			false
		);

		// Initial HttpdTomcatSiteJkMounts
		boolean useApache = conn.executeBooleanQuery(
			"select (\n"
			+ "  select pkey from \"web/tomcat\".\"TomcatJkMount\"\n"
			+ "  where (httpd_tomcat_site, path)=(?, '/*')\n"
			+ ") is null",
			tomcat_site
		);

		if(useApache) {
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				tomcat_site,
				checkJkMountPath(path + "/j_security_check")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				tomcat_site,
				checkJkMountPath(path + "/servlet/*")
			);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounting, aoServer, false);
		} else {
			boolean enableCgi = conn.executeBooleanQuery("select enable_cgi from web.\"Site\" where pkey=?", tomcat_site);
			if(enableCgi) {
				conn.executeUpdate(
					"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
					tomcat_site,
					checkJkMountPath(path + "/cgi-bin/*")
				);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounting, aoServer, false);
			}
		}

		return pkey;
	}

	public static int addHttpdTomcatDataSource(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int tomcat_context,
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
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "addHttpdTomcatDataSource", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to add HttpdTomcatDataSource, HttpdSite disabled: "+tomcat_site);

		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  \"web/tomcat\".\"TomcatContextDataSource\"\n"
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
			+ ") RETURNING pkey",
			tomcat_context,
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
			SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);

		return pkey;
	}

	public static int addHttpdTomcatParameter(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int tomcat_context,
		String name,
		String value,
		boolean override,
		String description
	) throws IOException, SQLException {
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "addHttpdTomcatParameter", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to add HttpdTomcatParameter, HttpdSite disabled: "+tomcat_site);

		int pkey = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  \"web/tomcat\".\"TomcatContextParameter\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ") RETURNING pkey",
			tomcat_context,
			name,
			value,
			override,
			description
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);

		return pkey;
	}

	public static int addHttpdTomcatSiteJkMount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int tomcat_site,
		String path,
		boolean mount
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "addHttpdTomcatSiteJkMount", tomcat_site);

		int pkey = conn.executeIntUpdate(
			"INSERT INTO \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, \"path\", mount) VALUES (?,?,?) RETURNING pkey",
			tomcat_site,
			checkJkMountPath(path),
			mount
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);

		return pkey;
	}

	public static void removeHttpdTomcatSiteJkMount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int tomcat_site = conn.executeIntQuery("select httpd_tomcat_site from \"web/tomcat\".\"TomcatJkMount\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatSiteJkMount", tomcat_site);

		conn.executeUpdate("delete from \"web/tomcat\".\"TomcatJkMount\" where pkey=?", pkey);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);
	}

	public static void checkHttpdTomcatContext(
		DatabaseConnection conn,
		RequestSource source,
		int tomcat_site,
		String className,
		boolean crossContext,
		UnixPath docBase,
		boolean override,
		String path,
		boolean privileged,
		String wrapperClass,
		UnixPath workDir,
		boolean serverXmlConfigured
	) throws IOException, SQLException {
		if(!HttpdTomcatContext.isValidDocBase(docBase)) throw new SQLException("Invalid docBase: "+docBase);
		int aoServer=getAOServerForHttpdSite(conn, tomcat_site);

		// OperatingSystem settings
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		UnixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
		UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		String docBaseStr = docBase.toString();
		if(docBaseStr.startsWith("/home/")) {
			// Must be able to access one of the linux.UserServer with that home directory
			//
			// This means there must be an accessible account that has a home directory that is a prefix of this docbase.
			// Such as /home/e/example/ being a prefix of /home/e/example/my-webapp
			IntList lsas = conn.executeIntListQuery(
				"select\n"
				+ "  pkey\n"
				+ "from\n"
				+ "  linux.\"UserServer\"\n"
				+ "where\n"
				+ "  ao_server=?\n"
				+ "  and (home || '/')=substring(? from 1 for (length(home) + 1))",
				aoServer,
				docBaseStr
			);
			boolean found=false;
			for(int c=0;c<lsas.size();c++) {
				if(LinuxAccountHandler.canAccessLinuxServerAccount(conn, source, lsas.getInt(c))) {
					found=true;
					break;
				}
			}
			if(!found) throw new SQLException("Home directory not allowed for path: " + docBaseStr);
		} else if(docBaseStr.startsWith(httpdSitesDir + "/")) {
			int slashPos = docBaseStr.indexOf('/', httpdSitesDir.toString().length() + 1);
			if(slashPos == -1) slashPos = docBaseStr.length();
			String siteName = docBaseStr.substring(httpdSitesDir.toString().length() + 1, slashPos);
			int hs = conn.executeIntQuery("select pkey from web.\"Site\" where ao_server=? and \"name\"=?", aoServer, siteName);
			HttpdHandler.checkAccessHttpdSite(conn, source, "addCvsRepository", hs);
		} else if(docBaseStr.startsWith(httpdSharedTomcatsDir + "/")) {
			int slashPos = docBaseStr.indexOf('/', httpdSharedTomcatsDir.toString().length() + 1);
			if(slashPos == -1) slashPos = docBaseStr.length();
			String tomcatName = docBaseStr.substring(httpdSharedTomcatsDir.toString().length() + 1, slashPos);
			int groupLSA = conn.executeIntQuery("select linux_server_account from \"web/tomcat\".\"SharedTomcat\" where name=? and ao_server=?", tomcatName, aoServer);
			LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", groupLSA);
		} else {
			// Allow the example directories
			List<UnixPath> tomcats = conn.executeObjectListQuery(
				ObjectFactories.unixPathFactory,
				"select\n"
				+ "  htv.install_dir || '/webapps/examples'\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"TomcatSite\" hts\n"
				+ "  inner join \"web/tomcat\".\"TomcatVersion\" htv on hts.version=htv.version\n"
				+ "where\n"
				+ "  hts.httpd_site=?\n"
				+ "union select\n"
				+ "  htv.install_dir || '/webapps/manager'\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"TomcatSite\" hts\n"
				+ "  inner join \"web/tomcat\".\"TomcatVersion\" htv on hts.version=htv.version\n"
				+ "where\n"
				+ "  hts.httpd_site=?\n",
				tomcat_site,
				tomcat_site
			);
			boolean found=false;
			for (UnixPath tomcat : tomcats) {
				if (docBase.equals(tomcat)) {
					found = true;
					break;
				}
			}
			// TODO: It would be better to make sure the
			if(!found) throw new SQLException("Invalid docBase: " + docBase);
		}

		if(!HttpdTomcatContext.isValidPath(path)) throw new SQLException("Invalid path: "+path);
		if(!HttpdTomcatContext.isValidWorkDir(workDir)) throw new SQLException("Invalid workDir: "+workDir);
	}

	/**
	 * Creates a new Tomcat site with the standard configuration.
	 */
	public static int addHttpdJBossSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		String siteName,
		AccountingCode packageName,
		UserId username,
		GroupId group,
		Email serverAdmin,
		boolean useApache,
		int ipAddress,
		DomainName primaryHttpHostname,
		DomainName[] altHttpHostnames,
		int jBossVersion,
		int phpVersion,
		boolean enableCgi,
		boolean enableSsi,
		boolean enableHtaccess,
		boolean enableIndexes,
		boolean enableFollowSymlinks
	) throws IOException, SQLException {
		return addHttpdJVMSite(
			"addHttdJBossSite",
			conn,
			source,
			invalidateList,
			aoServer,
			siteName,
			packageName,
			username,
			group,
			serverAdmin,
			useApache,
			ipAddress,
			primaryHttpHostname,
			altHttpHostnames,
			"jboss",
			jBossVersion,
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

	private static int addHttpdJVMSite(
		String methodName,
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		String siteName,
		AccountingCode packageName,
		UserId username,
		GroupId group,
		Email serverAdmin,
		boolean useApache,
		int ipAddress,
		DomainName primaryHttpHostname,
		DomainName[] altHttpHostnames,
		String siteType,
		int jBossVersion,
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
		ServerHandler.checkAccessServer(conn, source, methodName, aoServer);
		if(!HttpdSite.isValidSiteName(siteName)) throw new SQLException("Invalid site name: "+siteName);
		PackageHandler.checkAccessPackage(conn, source, methodName, packageName);
		if(PackageHandler.isPackageDisabled(conn, packageName)) throw new SQLException("Unable to "+methodName+", Package disabled: "+packageName);
		LinuxAccountHandler.checkAccessLinuxAccount(conn, source, methodName, username);
		int lsa=LinuxAccountHandler.getLinuxServerAccount(conn, username, aoServer);
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to "+methodName+", LinuxServerAccount disabled: "+lsa);
		LinuxAccountHandler.checkAccessLinuxGroup(conn, source, methodName, group);
		int lsg=LinuxAccountHandler.getLinuxServerGroup(conn, group, aoServer);
		if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to "+methodName+" for user '"+LinuxAccount.MAIL+'\'');
		LinuxAccountHandler.checkAccessLinuxGroup(conn, source, methodName, group);
		final int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		if(osv == -1) throw new SQLException("Unknown operating system version for server #" + aoServer);
		if(
			group.equals(LinuxGroup.FTPONLY)
			|| group.equals(LinuxGroup.MAIL)
			|| group.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to "+methodName+" for group '"+group+'\'');
		int sharedTomcatPkey = 0;
		if ("jboss".equals(siteType)) {
			if(tomcatVersion!=-1) throw new SQLException("TomcatVersion cannot be supplied for a JBoss site: "+tomcatVersion);
			tomcatVersion=conn.executeIntQuery("select tomcat_version from \"web/jboss\".\"Version\" where version=?", jBossVersion);
		} else if ("tomcat_shared".equals(siteType)) {
			// Get shared Tomcat pkey
			sharedTomcatPkey = conn.executeIntQuery(
				"select pkey from \"web/tomcat\".\"SharedTomcat\" where ao_server=? and name=?",
				aoServer,
				sharedTomcatName
			);

			// Check for ties between jvm and site in linux.GroupUser
			String sharedTomcatUsername = conn.executeStringQuery("select lsa.username from \"web/tomcat\".\"SharedTomcat\" hst, linux.\"UserServer\" lsa where hst.linux_server_account = lsa.pkey and hst.pkey=?", sharedTomcatPkey);
			String sharedTomcatLinuxGroup = conn.executeStringQuery("select lsg.name from \"web/tomcat\".\"SharedTomcat\" hst, linux.\"GroupServer\" lsg where hst.linux_server_group = lsg.pkey and hst.pkey=?", sharedTomcatPkey);
			boolean hasAccess = conn.executeBooleanQuery(
				"select (\n"
				+ "  select\n"
				+ "    pkey\n"
				+ "  from\n"
				+ "    linux.\"GroupUser\"\n"
				+ "  where\n"
				+ "    \"group\"=?\n"
				+ "    and username=?\n"
				+ "    and (\n"
				+ "      operating_system_version is null\n"
				+ "      or operating_system_version=?\n"
				+ "    )\n"
				+ ") is not null",
				sharedTomcatLinuxGroup,
				username,
				osv
			);
			if (!hasAccess) throw new SQLException("linux.User ("+username+") does not have access to linux.Group ("+sharedTomcatLinuxGroup+")");
			hasAccess = conn.executeBooleanQuery(
				"select (\n"
				+ "  select\n"
				+ "    pkey\n"
				+ "  from\n"
				+ "    linux.\"GroupUser\"\n"
				+ "  where\n"
				+ "    \"group\"=?\n"
				+ "    and username=?\n"
				+ "    and (\n"
				+ "      operating_system_version is null\n"
				+ "      or operating_system_version=?\n"
				+ "    )\n"
				+ ") is not null",
				group,
				sharedTomcatUsername,
				osv
			);
			if (!hasAccess) throw new SQLException("linux.User ("+sharedTomcatName+") does not have access to linux.Group ("+group+")");

			if(tomcatVersion!=-1) throw new SQLException("TomcatVersion cannot be supplied for a TomcatShared site: "+tomcatVersion);
			tomcatVersion = conn.executeIntQuery("select version from \"web/tomcat\".\"SharedTomcat\" where pkey=?", sharedTomcatPkey);
		}
		String tomcatVersionStr=conn.executeStringQuery("select version from distribution.\"SoftwareVersion\" where pkey=?", tomcatVersion);
		boolean isTomcat4 =
			!tomcatVersionStr.equals(HttpdTomcatVersion.VERSION_3_1)
			&& !tomcatVersionStr.equals(HttpdTomcatVersion.VERSION_3_2_4)
		;
		if(ipAddress!=-1) {
			IPAddressHandler.checkAccessIPAddress(conn, source, methodName, ipAddress);
			// The IP must be on the provided server
			int ipServer=IPAddressHandler.getServerForIPAddress(conn, ipAddress);
			if(ipServer!=aoServer) throw new SQLException("IP address "+ipAddress+" is not hosted on AOServer #"+aoServer);
		} else {
			ipAddress=IPAddressHandler.getSharedHttpdIP(conn, aoServer);
			if(ipAddress==-1) throw new SQLException("Unable to find shared IP address for AOServer #"+aoServer);
		}
		if(phpVersion != -1) {
			// Version must be correct for this server
			int tvOsv = conn.executeIntQuery(
				"select coalesce(\n"
				+ "  (select operating_system_version from distribution.\"SoftwareVersion\" where pkey=? and name=?),\n"
				+ "  -1\n"
				+ ")",
				phpVersion,
				TechnologyName.PHP
			);
			if(tvOsv == -1) throw new SQLException("Requested PHP version is not a PHP version: #" + phpVersion);
			if(tvOsv != osv) throw new SQLException("Requested PHP version is for the wrong operating system version: #" + phpVersion + ": " + tvOsv + " != " + osv);
		}

		PackageHandler.checkPackageAccessServer(conn, source, methodName, packageName, aoServer);

		AccountingCode accounting = PackageHandler.getBusinessForPackage(conn, packageName);

		Port httpPort;
		try {
			httpPort = Port.valueOf(80, com.aoindustries.net.Protocol.TCP);
		} catch(ValidationException e) {
			throw new SQLException(e);
		}

		List<DomainName> tlds = DNSHandler.getDNSTLDs(conn);
//		DomainName testURL;
//		try {
//			testURL = DomainName.valueOf(siteName + "." + ServerHandler.getHostnameForAOServer(conn, aoServer));
//		} catch(ValidationException e) {
//			throw new SQLException(e);
//		}
//		DNSHandler.addDNSRecord(
//			conn,
//			invalidateList,
//			testURL,
//			IPAddressHandler.getInetAddressForIPAddress(conn, ipAddress),
//			tlds
//		);

		// Finish up the security checks with the Connection
		MasterServer.checkAccessHostname(conn, source, methodName, primaryHttpHostname.toString(), tlds);
		for(DomainName altHttpHostname : altHttpHostnames) {
			MasterServer.checkAccessHostname(conn, source, methodName, altHttpHostname.toString(), tlds);
		}

		// Create and/or get the HttpdBind info
		int httpNetBind = getHttpdBind(conn, invalidateList, packageName, aoServer, ipAddress, httpPort, Protocol.HTTP);

		// Create the HttpdSite
		int httpdSitePKey = conn.executeIntUpdate(
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
			+ ") RETURNING pkey",
			aoServer,
			siteName,
			packageName.toString(),
			username.toString(),
			group.toString(),
			serverAdmin.toString(),
			(phpVersion == -1) ? Null.INTEGER : phpVersion,
			enableCgi,
			enableSsi,
			enableHtaccess,
			enableIndexes,
			enableFollowSymlinks
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITES, accounting, aoServer, false);

		// Create the HttpdTomcatSite
		conn.executeUpdate(
			"INSERT INTO \"web/tomcat\".\"TomcatSite\" (httpd_site, version) VALUES (?,?)",
			httpdSitePKey,
			tomcatVersion
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITES, accounting, aoServer, false);

		// OperatingSystem settings
		UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);
		UnixPath docBase;
		try {
			docBase = UnixPath.valueOf(httpdSitesDir + "/" + siteName + "/webapps/" + HttpdTomcatContext.ROOT_DOC_BASE);
		} catch(ValidationException e) {
			throw new SQLException(e);
		}

		// Add the default httpd_tomcat_context
		conn.executeUpdate(
			"INSERT INTO\n"
			+ "  \"web/tomcat\".\"TomcatContext\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  "+HttpdTomcatContext.DEFAULT_CLASS_NAME+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_COOKIES+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_CROSS_CONTEXT+",\n"
			+ "  ?,\n"
			+ "  "+HttpdTomcatContext.DEFAULT_OVERRIDE+",\n"
			+ "  '"+HttpdTomcatContext.ROOT_PATH+"',\n"
			+ "  "+HttpdTomcatContext.DEFAULT_PRIVILEGED+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_RELOADABLE+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_USE_NAMING+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_WRAPPER_CLASS+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_DEBUG+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_WORK_DIR+",\n"
			+ "  "+HttpdTomcatContext.DEFAULT_SERVER_XML_CONFIGURED+"\n"
			+ ")",
			httpdSitePKey,
			docBase
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS, accounting, aoServer, false);

		if(!isTomcat4) {
			conn.executeUpdate(
				"INSERT INTO\n"
				+ "  \"web/tomcat\".\"TomcatContext\"\n"
				+ "VALUES (\n"
				+ "  default,\n"
				+ "  ?,\n"
				+ "  "+HttpdTomcatContext.DEFAULT_CLASS_NAME+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_COOKIES+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_CROSS_CONTEXT+",\n"
				+ "  ?,\n"
				+ "  "+HttpdTomcatContext.DEFAULT_OVERRIDE+",\n"
				+ "  '/examples',\n"
				+ "  "+HttpdTomcatContext.DEFAULT_PRIVILEGED+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_RELOADABLE+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_USE_NAMING+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_WRAPPER_CLASS+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_DEBUG+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_WORK_DIR+",\n"
				+ "  "+HttpdTomcatContext.DEFAULT_SERVER_XML_CONFIGURED+"\n"
				+ ")",
				httpdSitePKey,
				conn.executeStringQuery("select install_dir from \"web/tomcat\".\"TomcatVersion\" where version=?", tomcatVersion)+"/webapps/examples"
			);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS, accounting, aoServer, false);
		}

		if ("jboss".equals(siteType)) {
			// Create the HttpdJBossSite
			int wildcardIP=IPAddressHandler.getWildcardIPAddress(conn);
			int jnpBind = NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				wildcardIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.JNP,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			int webserverBind = NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				wildcardIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.WEBSERVER,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			int rmiBind = NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				wildcardIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.RMI,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			int hypersonicBind = NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				wildcardIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.HYPERSONIC,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			int jmxBind = NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				wildcardIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.JMX,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into \"web/jboss\".\"Site\" values(?,?,?,?,?,?,?)");
			try {
				pstmt.setInt(1, httpdSitePKey);
				pstmt.setInt(2, jBossVersion);
				pstmt.setInt(3, jnpBind);
				pstmt.setInt(4, webserverBind);
				pstmt.setInt(5, rmiBind);
				pstmt.setInt(6, hypersonicBind);
				pstmt.setInt(7, jmxBind);
				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from query: "+pstmt.toString());
				throw err;
			} finally {
				pstmt.close();
			}
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_JBOSS_SITES, accounting, aoServer, false);
		} else if ("tomcat_shared".equals(siteType)) {
			// Create the HttpdTomcatSharedSite
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"SharedTomcatSite\" values(?,?)",
				httpdSitePKey,
				sharedTomcatPkey
			);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SHARED_SITES, accounting, aoServer, false);
		} else if ("tomcat_standard".equals(siteType)) {
			// Create the HttpdTomcatStdSite
			if(isTomcat4) {
				int shutdownPort=NetBindHandler.allocateNetBind(
					conn,
					invalidateList,
					aoServer,
					IPAddressHandler.getLoopbackIPAddress(conn, aoServer),
					com.aoindustries.net.Protocol.TCP,
					Protocol.TOMCAT4_SHUTDOWN,
					packageName,
					MINIMUM_AUTO_PORT_NUMBER
				);
				conn.executeUpdate(
					"insert into \"web/tomcat\".\"PrivateTomcatSite\" values(?,?,?,?,true,true)",
					httpdSitePKey,
					shutdownPort,
					new Identifier(MasterServer.getRandom()).toString(),
					HttpdTomcatStdSite.DEFAULT_MAX_POST_SIZE
				);
			} else {
				conn.executeUpdate(
					"insert into \"web/tomcat\".\"PrivateTomcatSite\" values(?,null,null,?,true,true)",
					httpdSitePKey,
					HttpdSharedTomcat.DEFAULT_MAX_POST_SIZE
				);
			}
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES, accounting, aoServer, false);
		}

		if(!isTomcat4 || !"tomcat_shared".equals(siteType)) {
			// Allocate a NetBind for the worker
			int netBindPKey=NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				IPAddressHandler.getLoopbackIPAddress(conn, aoServer),
				com.aoindustries.net.Protocol.TCP,
				isTomcat4?HttpdJKProtocol.AJP13:HttpdJKProtocol.AJP12,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);
			// Create the HttpdWorker
			addTomcatWorker(
				conn,
				invalidateList,
				netBindPKey,
				httpdSitePKey
			);
		}

		// Create the HTTP HttpdSiteBind
		String siteLogsDir = OperatingSystemVersion.getHttpdSiteLogsDirectory(osv).toString();
		int httpSiteBindPKey = conn.executeIntUpdate(
			"INSERT INTO web.\"VirtualHost\" (httpd_site, httpd_bind, access_log, error_log) VALUES (?,?,?,?) RETURNING pkey",
			httpdSitePKey,
			httpNetBind,
			siteLogsDir + '/' + siteName + "/http/access_log",
			siteLogsDir + '/' + siteName + "/http/error_log"
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BINDS, accounting, aoServer, false);

		conn.executeUpdate(
			"insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,true)",
			httpSiteBindPKey,
			primaryHttpHostname
		);
		for (DomainName altHttpHostname : altHttpHostnames) {
			conn.executeUpdate(
				"insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,false)",
				httpSiteBindPKey,
				altHttpHostname
			);
		}
//		conn.executeUpdate(
//			"insert into web.\"VirtualHostName\"(httpd_site_bind, hostname, is_primary) values(?,?,false)",
//			httpSiteBindPKey,
//			testURL
//		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_URLS, accounting, aoServer, false);

		// Initial HttpdTomcatSiteJkMounts
		if(useApache) {
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/j_security_check")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/servlet/*")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.do")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.jsp")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.jspa")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.jspx")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.vm")
			);
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*.xml")
			);
		} else {
			conn.executeUpdate(
				"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,TRUE)",
				httpdSitePKey,
				checkJkMountPath("/*")
			);
			if(enableCgi) {
				conn.executeUpdate(
					"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
					httpdSitePKey,
					checkJkMountPath("/cgi-bin/*")
				);
			}
			boolean hasPhp;
			if(phpVersion != -1) {
				// CGI-based PHP
				hasPhp = true;
			} else {
				// Check for mod_php
				hasPhp = conn.executeBooleanQuery(
					"select\n"
					+ "  hs.mod_php_version is not null\n"
					+ "from\n"
					+ "  web.\"HttpdBind\" hb\n"
					+ "  inner join web.\"HttpdServer\" hs on hb.httpd_server=hs.pkey\n"
					+ "where\n"
					+ "  hb.net_bind=?",
					httpNetBind
				);
			}
			if(hasPhp) {
				conn.executeUpdate(
					"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,'/*.php',FALSE)",
					httpdSitePKey
				);
			}
		}
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS, accounting, aoServer, false);

		return httpdSitePKey;
	}

	public static int addHttpdSharedTomcat(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		String name,
		int aoServer,
		int version,
		UserId linuxServerAccount,
		GroupId linuxServerGroup,
		boolean skipSecurityChecks
	) throws IOException, SQLException {
		if(!HttpdSharedTomcat.isValidSharedTomcatName(name)) throw new SQLException("Invalid shared Tomcat name: "+name);
		if(linuxServerAccount.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add HttpdSharedTomcat for user '"+LinuxAccount.MAIL+'\'');
		int lsaPkey = LinuxAccountHandler.getLinuxServerAccount(conn, linuxServerAccount, aoServer);
		int lsgPkey = LinuxAccountHandler.getLinuxServerGroup(conn, linuxServerGroup, aoServer);
		if(!skipSecurityChecks) {
			ServerHandler.checkAccessServer(conn, source, "addHttpdSharedTomcat", aoServer);
			LinuxAccountHandler.checkAccessLinuxAccount(conn, source, "addHttpdSharedTomcat", linuxServerAccount);
			if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsaPkey)) throw new SQLException("Unable to add HttpdSharedTomcat, LinuxServerAccount disabled: "+lsaPkey);
			LinuxAccountHandler.checkAccessLinuxGroup(conn, source, "addHttpdSharedTomcat", linuxServerGroup);
		}
		if(
			linuxServerGroup.equals(LinuxGroup.FTPONLY)
			|| linuxServerGroup.equals(LinuxGroup.MAIL)
			|| linuxServerGroup.equals(LinuxGroup.MAILONLY)
		) throw new SQLException("Not allowed to add HttpdSharedTomcat for group '"+linuxServerGroup+'\'');

		// Tomcat 4 version will start with "4."
		String versionStr=conn.executeStringQuery("select version from distribution.\"SoftwareVersion\" where pkey=?", version);
		boolean isTomcat4 =
			!versionStr.equals(HttpdTomcatVersion.VERSION_3_1)
			&& !versionStr.equals(HttpdTomcatVersion.VERSION_3_2_4)
		;

		int pkey;
		if(isTomcat4) {
			AccountingCode packageName=LinuxAccountHandler.getPackageForLinuxGroup(conn, linuxServerGroup);
			int loopbackIP=IPAddressHandler.getLoopbackIPAddress(conn, aoServer);

			// Allocate a NetBind for the worker
			int hwBindPKey=NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				loopbackIP,
				com.aoindustries.net.Protocol.TCP,
				HttpdJKProtocol.AJP13,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);

			// Create the HttpdWorker
			addTomcatWorker(
				conn,
				invalidateList,
				hwBindPKey,
				-1
			);

			// Allocate the shutdown port
			int shutdownBindPKey=NetBindHandler.allocateNetBind(
				conn,
				invalidateList,
				aoServer,
				loopbackIP,
				com.aoindustries.net.Protocol.TCP,
				Protocol.TOMCAT4_SHUTDOWN,
				packageName,
				MINIMUM_AUTO_PORT_NUMBER
			);

			pkey = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  \"web/tomcat\".\"SharedTomcat\"\n"
				+ "VALUES(\n"
				+ "  default,\n" // pkey
				+ "  ?,\n" // name
				+ "  ?,\n" // ao_server
				+ "  ?,\n" // version
				+ "  ?,\n" // linux_server_account
				+ "  ?,\n" // linux_server_group
				+ "  null,\n" // disable_log
				+ "  ?,\n" // tomcat4_worker
				+ "  ?,\n" // tomcat4_shutdown_port
				+ "  ?,\n" // tomcat4_shutdown_key
				+ "  false,\n" // is_manual
				+ "  ?,\n" // max_post_size
				+ "  true,\n" // unpack_wars
				+ "  true\n" // auto_deploy
				+ ") RETURNING pkey",
				name,
				aoServer,
				version,
				lsaPkey,
				lsgPkey,
				hwBindPKey,
				shutdownBindPKey,
				new Identifier(MasterServer.getRandom()).toString(),
				HttpdSharedTomcat.DEFAULT_MAX_POST_SIZE
			);
		} else {
			pkey = conn.executeIntUpdate(
				"INSERT INTO\n"
				+ "  \"web/tomcat\".\"SharedTomcat\"\n"
				+ "VALUES (\n"
				+ "  default,\n" // pkey
				+ "  ?,\n" // name
				+ "  ?,\n" // ao_server
				+ "  ?,\n" // version
				+ "  ?,\n" // linux_server_account
				+ "  ?,\n" // linux_server_group
				+ "  null,\n" // disable_log
				+ "  null,\n" // tomcat4_worker
				+ "  null,\n" // tomcat4_shutdown_port
				+ "  null,\n" // tomcat4_shutdown_key
				+ "  false,\n" // is_manual
				+ "  ?,\n" // max_post_size
				+ "  true,\n" // unpack_wars
				+ "  true\n" // auto_deploy
				+ ") RETURNING pkey",
				name,
				aoServer,
				version,
				lsaPkey,
				lsgPkey,
				HttpdSharedTomcat.DEFAULT_MAX_POST_SIZE
			);
		}
		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			UsernameHandler.getBusinessForUsername(conn, linuxServerAccount),
			aoServer,
			false
		);
		return pkey;
	}

	/**
	 * Creates a new Tomcat site with the standard configuration.
	 */
	public static int addHttpdTomcatSharedSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		String siteName,
		AccountingCode packageName,
		UserId username,
		GroupId group,
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
		return addHttpdJVMSite(
			"addTomcatSharedSite",
			conn,
			source,
			invalidateList,
			aoServer,
			siteName,
			packageName,
			username,
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
	public static int addHttpdTomcatStdSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int aoServer,
		String siteName,
		AccountingCode packageName,
		UserId username,
		GroupId group,
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
		return addHttpdJVMSite(
			"addTomcatStdSite",
			conn,
			source,
			invalidateList,
			aoServer,
			siteName,
			packageName,
			username,
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

	public static void disableHttpdSharedTomcat(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isHttpdSharedTomcatDisabled(conn, pkey)) throw new SQLException("HttpdSharedTomcat is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableHttpdSharedTomcat", disableLog, false);
		checkAccessHttpdSharedTomcat(conn, source, "disableHttpdSharedTomcat", pkey);

		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	public static void disableHttpdSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isHttpdSiteDisabled(conn, pkey)) throw new SQLException("HttpdSite is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableHttpdSite", disableLog, false);
		checkAccessHttpdSite(conn, source, "disableHttpdSite", pkey);
		IntList httpdSiteBinds=getHttpdSiteBindsForHttpdSite(conn, pkey);
		for(int c=0;c<httpdSiteBinds.size();c++) {
			int hsb=httpdSiteBinds.getInt(c);
			if(!isHttpdSiteBindDisabled(conn, hsb)) {
				throw new SQLException("Cannot disable HttpdSite #"+pkey+": HttpdSiteBind not disabled: "+hsb);
			}
		}

		conn.executeUpdate(
			"update web.\"Site\" set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void disableHttpdSiteBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		int pkey
	) throws IOException, SQLException {
		if(isHttpdSiteBindDisabled(conn, pkey)) throw new SQLException("HttpdSiteBind is already disabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "disableHttpdSiteBind", disableLog, false);
		int httpdSite=conn.executeIntQuery("select httpd_site from web.\"VirtualHost\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "disableHttpdSiteBind", httpdSite);

		conn.executeUpdate(
			"update web.\"VirtualHost\" set disable_log=? where pkey=?",
			disableLog,
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_BINDS,
			getBusinessForHttpdSite(conn, httpdSite),
			getAOServerForHttpdSite(conn, httpdSite),
			false
		);
	}

	public static void enableHttpdSharedTomcat(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForHttpdSharedTomcat(conn, pkey);
		if(disableLog==-1) throw new SQLException("HttpdSharedTomcat is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableHttpdSharedTomcat", disableLog, true);
		checkAccessHttpdSharedTomcat(conn, source, "enableHttpdSharedTomcat", pkey);
		AccountingCode pk=getPackageForHttpdSharedTomcat(conn, pkey);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable HttpdSharedTomcat #"+pkey+", Package not enabled: "+pk);
		int lsa=getLinuxServerAccountForHttpdSharedTomcat(conn, pkey);
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to enable HttpdSharedTomcat #"+pkey+", LinuxServerAccount not enabled: "+lsa);

		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			PackageHandler.getBusinessForPackage(conn, pk),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	public static void enableHttpdSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForHttpdSite(conn, pkey);
		if(disableLog==-1) throw new SQLException("HttpdSite is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableHttpdSite", disableLog, true);
		checkAccessHttpdSite(conn, source, "enableHttpdSite", pkey);
		AccountingCode pk=getPackageForHttpdSite(conn, pkey);
		if(PackageHandler.isPackageDisabled(conn, pk)) throw new SQLException("Unable to enable HttpdSite #"+pkey+", Package not enabled: "+pk);
		int lsa=getLinuxServerAccountForHttpdSite(conn, pkey);
		if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to enable HttpdSite #"+pkey+", LinuxServerAccount not enabled: "+lsa);

		conn.executeUpdate(
			"update web.\"Site\" set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			PackageHandler.getBusinessForPackage(conn, pk),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void enableHttpdSiteBind(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int disableLog=getDisableLogForHttpdSiteBind(conn, pkey);
		if(disableLog==-1) throw new SQLException("HttpdSiteBind is already enabled: "+pkey);
		BusinessHandler.checkAccessDisableLog(conn, source, "enableHttpdSiteBind", disableLog, true);
		int hs=getHttpdSiteForHttpdSiteBind(conn, pkey);
		checkAccessHttpdSite(conn, source, "enableHttpdSiteBind", hs);
		if(isHttpdSiteDisabled(conn, hs)) throw new SQLException("Unable to enable HttpdSiteBind #"+pkey+", HttpdSite not enabled: "+hs);

		conn.executeUpdate(
			"update web.\"VirtualHost\" set disable_log=null where pkey=?",
			pkey
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_BINDS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static String generateSharedTomcatName(DatabaseConnection conn, String template) throws SQLException, IOException {
		// Load the entire list of site names
		List<String> names=conn.executeStringListQuery("select name from \"web/tomcat\".\"SharedTomcat\" group by name");
		int size=names.size();

		// Sort them
		List<String> sorted=new SortedArrayList<>(size);
		sorted.addAll(names);

		// OperatingSystem settings
		UnixPath httpdSharedTomcatsDirCentOS5 = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(OperatingSystemVersion.CENTOS_5_I686_AND_X86_64);
		UnixPath httpdSharedTomcatsDirCentOS7 = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(OperatingSystemVersion.CENTOS_7_X86_64);

		// Find one that is not used
		String goodOne=null;
		for(int c=1;c<Integer.MAX_VALUE;c++) {
			String name=template+c;
			if(!HttpdSharedTomcat.isValidSharedTomcatName(name)) throw new SQLException("Invalid shared Tomcat name: "+name);
			if(!sorted.contains(name)) {
				UnixPath wwwgroupDirCentOS5;
				UnixPath wwwgroupDirCentOS7;
				try {
					wwwgroupDirCentOS5 = UnixPath.valueOf(httpdSharedTomcatsDirCentOS5 + "/" + name);
					wwwgroupDirCentOS7 = UnixPath.valueOf(httpdSharedTomcatsDirCentOS7 + "/" + name);
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
				if(
					// Must also not be found in linux.UserServer.home
					conn.executeIntQuery(
						"select\n"
						+ "  count(*)\n"
						+ "from\n"
						+ "  linux.\"UserServer\"\n"
						+ "where\n"
						+ "  home=?\n"
						+ "  or substring(home from 1 for " + (wwwgroupDirCentOS5.toString().length() + 1) + ")=?\n"
						+ "  or home=?\n"
						+ "  or substring(home from 1 for " + (wwwgroupDirCentOS7.toString().length() + 1) + ")=?",
						wwwgroupDirCentOS5,
						wwwgroupDirCentOS5 + "/",
						wwwgroupDirCentOS7,
						wwwgroupDirCentOS7 + "/"
					) == 0
					// Must also not be found in account.Username.username
					&& conn.executeIntQuery("select count(*) from account.\"Username\" where username=?", name) == 0
					// Must also not be found in linux.Group.name
					&& conn.executeIntQuery("select count(*) from linux.\"Group\" where name=?", name) == 0
				) {
					goodOne = name;
					break;
				}
			}
		}

		// If could not find one, report and error
		if(goodOne==null) throw new SQLException("Unable to find available shared Tomcat name for template: "+template);
		else return goodOne;
	}

	public static String generateSiteName(
		DatabaseConnection conn,
		String template
	) throws IOException, SQLException {
		// Load the entire list of site names
		List<String> names=conn.executeStringListQuery("select \"name\" from web.\"Site\" group by \"name\"");
		int size=names.size();

		// Sort them
		List<String> sorted=new SortedArrayList<>(size);
		sorted.addAll(names);

		// OperatingSystem settings
		UnixPath httpdSitesDirCentOS5 = OperatingSystemVersion.getHttpdSitesDirectory(OperatingSystemVersion.CENTOS_5_I686_AND_X86_64);
		UnixPath httpdSitesDirCentOS7 = OperatingSystemVersion.getHttpdSitesDirectory(OperatingSystemVersion.CENTOS_7_X86_64);

		// Find one that is not used
		String goodOne=null;
		for(int c=1;c<Integer.MAX_VALUE;c++) {
			String name=template+c;
			if(!HttpdSite.isValidSiteName(name)) throw new SQLException("Invalid site name: "+name);
			if(!sorted.contains(name)) {
				// Must also not be found in linux.UserServer.home on CentOS 5 or CentOS 7
				UnixPath wwwDirCentOS5;
				UnixPath wwwDirCentOS7;
				try {
					wwwDirCentOS5 = UnixPath.valueOf(httpdSitesDirCentOS5 + "/" + name);
					wwwDirCentOS7 = UnixPath.valueOf(httpdSitesDirCentOS7 + "/" + name);
				} catch(ValidationException e) {
					throw new SQLException(e);
				}
				int count=conn.executeIntQuery(
					"select\n"
					+ "  count(*)\n"
					+ "from\n"
					+ "  linux.\"UserServer\"\n"
					+ "where\n"
					+ "  home=?\n"
					+ "  or substring(home from 1 for " + (wwwDirCentOS5.toString().length() + 1) + ")=?\n"
					+ "  or home=?\n"
					+ "  or substring(home from 1 for " + (wwwDirCentOS7.toString().length() + 1) + ")=?",
					wwwDirCentOS5,
					wwwDirCentOS5 + "/",
					wwwDirCentOS7,
					wwwDirCentOS7 + "/"
				);
				if(count==0) {
					goodOne=name;
					break;
				}
			}
		}

		// If could not find one, report and error
		if(goodOne==null) throw new SQLException("Unable to find available site name for template: "+template);
		return goodOne;
	}

	public static int getDisableLogForHttpdSharedTomcat(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);
	}

	public static int getDisableLogForHttpdSite(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from web.\"Site\" where pkey=?", pkey);
	}

	public static int getDisableLogForHttpdSiteBind(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from web.\"VirtualHost\" where pkey=?", pkey);
	}

	public static IntList getHttpdSiteBindsForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from web.\"VirtualHost\" where httpd_site=?", pkey);
	}

	public static int getHttpdSiteForHttpdSiteURL(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  hsb.httpd_site\n"
			+ "from\n"
			+ "  web.\"VirtualHostName\" hsu,\n"
			+ "  web.\"VirtualHost\" hsb\n"
			+ "where\n"
			+ "  hsu.pkey=?\n"
			+ "  and hsu.httpd_site_bind=hsb.pkey",
			pkey
		);
	}

	public static IntList getHttpdSharedTomcatsForLinuxServerAccount(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from \"web/tomcat\".\"SharedTomcat\" where linux_server_account=?", pkey);
	}

	public static IntList getHttpdSharedTomcatsForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  hst.pkey\n"
			+ "from\n"
			+ "  linux.\"Group\" lg,\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  \"web/tomcat\".\"SharedTomcat\" hst\n"
			+ "where\n"
			+ "  lg.package=?\n"
			+ "  and lg.name=lsg.name\n"
			+ "  and lsg.pkey=hst.linux_server_group",
			name
		);
	}

	public static IntList getHttpdSitesForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from web.\"Site\" where package=?", name);
	}

	public static IntList getHttpdSitesForLinuxServerAccount(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  hs.pkey\n"
			+ "from\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  web.\"Site\" hs\n"
			+ "where\n"
			+ "  lsa.pkey=?\n"
			+ "  and lsa.username=hs.linux_account\n"
			+ "  and lsa.ao_server=hs.ao_server",
			pkey
		);
	}

	public static AccountingCode getBusinessForHttpdSharedTomcat(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  \"web/tomcat\".\"SharedTomcat\" hst,\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  hst.pkey=?\n"
			+ "  and hst.linux_server_group=lsg.pkey\n"
			+ "  and lsg.name=lg.name\n"
			+ "  and lg.package=pk.name",
			pkey
		);
	}

	public static AccountingCode getBusinessForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  web.\"Site\" hs,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  hs.pkey=?\n"
			+ "  and hs.package=pk.name",
			pkey
		);
	}

	public static AccountingCode getBusinessForHttpdServer(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.accounting\n"
			+ "from\n"
			+ "  web.\"HttpdServer\" hs,\n"
			+ "  billing.\"Package\" pk\n"
			+ "where\n"
			+ "  hs.pkey=?\n"
			+ "  and hs.package=pk.pkey",
			pkey
		);
	}

	public static int getHttpdSiteForHttpdSiteBind(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery("select httpd_site from web.\"VirtualHost\" where pkey=?", pkey);
	}

	public static int getLinuxServerAccountForHttpdSharedTomcat(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_account from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);
	}

	public static int getLinuxServerAccountForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  lsa.pkey\n"
			+ "from\n"
			+ "  web.\"Site\" hs,\n"
			+ "  linux.\"UserServer\" lsa\n"
			+ "where\n"
			+ "  hs.pkey=?\n"
			+ "  and hs.linux_account=lsa.username\n"
			+ "  and hs.ao_server=lsa.ao_server",
			pkey
		);
	}

	public static AccountingCode getPackageForHttpdServer(
		DatabaseConnection conn,
		int httpdServer
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  pk.name\n"
			+ "from\n"
			+ "  web.\"HttpdServer\" hs\n"
			+ "  inner join billing.\"Package\" pk on hs.package=pk.pkey\n"
			+ "where\n"
			+ "  hs.pkey=?",
			httpdServer
		);
	}

	public static AccountingCode getPackageForHttpdSharedTomcat(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select\n"
			+ "  lg.package\n"
			+ "from\n"
			+ "  \"web/tomcat\".\"SharedTomcat\" hst,\n"
			+ "  linux.\"GroupServer\" lsg,\n"
			+ "  linux.\"Group\" lg\n"
			+ "where\n"
			+ "  hst.pkey=?\n"
			+ "  and hst.linux_server_group=lsg.pkey\n"
			+ "  and lsg.name=lg.name",
			pkey
		);
	}

	public static AccountingCode getPackageForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountingCodeFactory,
			"select package from web.\"Site\" where pkey=?",
			pkey
		);
	}

	public static int getAOServerForHttpdSharedTomcat(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);
	}

	public static int getAOServerForHttpdSite(DatabaseConnection conn, int httpdSite) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from web.\"Site\" where pkey=?", httpdSite);
	}

	public static int getAOServerForHttpdServer(DatabaseConnection conn, int httpdServer) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from web.\"HttpdServer\" where pkey=?", httpdServer);
	}

	public static String getSiteNameForHttpdSite(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeStringQuery("select \"name\" from web.\"Site\" where pkey=?", pkey);
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
		if(isHttpdSiteDisabled(conn, sitePKey)) throw new SQLException("Unable to initialize HttpdSite passwd file, HttpdSite disabled: "+sitePKey);

		DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForHttpdSite(conn, sitePKey)
		).initializeHttpdSitePasswdFile(sitePKey, username, encPassword);
	}*/

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.HTTPD_SHARED_TOMCATS) {
			synchronized(HttpdHandler.class) {
				disabledHttpdSharedTomcats.clear();
			}
		} else if(tableID==SchemaTable.TableID.HTTPD_SITE_BINDS) {
			synchronized(HttpdHandler.class) {
				disabledHttpdSiteBinds.clear();
			}
		} else if(tableID==SchemaTable.TableID.HTTPD_SITES) {
			synchronized(HttpdHandler.class) {
				disabledHttpdSites.clear();
			}
		}
	}

	public static boolean isHttpdSharedTomcatDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		Integer pkeyObj = pkey;
		synchronized(HttpdHandler.class) {
			Boolean isDisabledObj = disabledHttpdSharedTomcats.get(pkeyObj);
			if(isDisabledObj != null) return isDisabledObj;
			boolean isDisabled = getDisableLogForHttpdSharedTomcat(conn, pkey) != -1;
			disabledHttpdSharedTomcats.put(pkeyObj, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isHttpdSiteBindDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		Integer pkeyObj = pkey;
		synchronized(HttpdHandler.class) {
			Boolean isDisabledObj = disabledHttpdSiteBinds.get(pkeyObj);
			if(isDisabledObj != null) return isDisabledObj;
			boolean isDisabled = getDisableLogForHttpdSiteBind(conn, pkey) != -1;
			disabledHttpdSiteBinds.put(pkeyObj, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isHttpdSiteDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		Integer pkeyObj = pkey;
		synchronized(HttpdHandler.class) {
			Boolean isDisabledObj = disabledHttpdSites.get(pkeyObj);
			if(isDisabledObj != null) return isDisabledObj;
			boolean isDisabled = getDisableLogForHttpdSite(conn, pkey) != -1;
			disabledHttpdSites.put(pkeyObj, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isSharedTomcatNameAvailable(DatabaseConnection conn, String name) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select pkey from \"web/tomcat\".\"SharedTomcat\" where name=? limit 1) is null", name);
	}

	public static boolean isSiteNameAvailable(DatabaseConnection conn, String siteName) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select pkey from web.\"Site\" where \"name\"=? limit 1) is null", siteName);
	}

	/**
	 * Starts up a Java VM
	 */
	public static String startJVM(
		DatabaseConnection conn,
		RequestSource source,
		int tomcat_site
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "startJVM", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to start JVM, HttpdSite disabled: "+tomcat_site);

		// Get the server and siteName for the site
		int aoServer=getAOServerForHttpdSite(conn, tomcat_site);

		// Contact the daemon and start the JVM
		return DaemonHandler.getDaemonConnector(conn, aoServer).startJVM(tomcat_site);
	}

	/**
	 * Stops up a Java VM
	 */
	public static String stopJVM(
		DatabaseConnection conn,
		RequestSource source, 
		int tomcat_site
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "stopJVM", tomcat_site);
		// Can only stop the daemon if can access the shared linux account
		if(conn.executeBooleanQuery("select (select tomcat_site from \"web/tomcat\".\"SharedTomcatSite\" where tomcat_site=?) is not null", tomcat_site)) {
			int lsa=conn.executeIntQuery(
				"select\n"
				+ "  hst.linux_server_account\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"SharedTomcatSite\" htss,\n"
				+ "  \"web/tomcat\".\"SharedTomcat\" hst\n"
				+ "where\n"
				+ "  htss.tomcat_site=?\n"
				+ "  and htss.httpd_shared_tomcat=hst.pkey",
				tomcat_site
			);
			LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "stopJVM", lsa);
		}

		// Get the server and siteName for the site
		int aoServer=getAOServerForHttpdSite(conn, tomcat_site);

		// Contact the daemon and start the JVM
		return DaemonHandler.getDaemonConnector(conn, aoServer).stopJVM(tomcat_site);
	}

	/**
	 * Waits for pending or processing updates to complete.
	 */
	public static void waitForHttpdSiteRebuild(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		ServerHandler.checkAccessServer(conn, source, "waitForHttpdSiteRebuild", aoServer);
		ServerHandler.waitForInvalidates(aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).waitForHttpdSiteRebuild();
	}

	public static int getHttpdBind(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		AccountingCode packageName,
		int aoServer,
		int ipAddress,
		Port httpPort,
		String protocol
	) throws IOException, SQLException {
		// First, find the net_bind
		int netBind;
		PreparedStatement pstmt = conn.getConnection(
			Connection.TRANSACTION_READ_COMMITTED,
			true
		).prepareStatement("select pkey, app_protocol from net.\"Bind\" where server=? and \"ipAddress\"=? and port=? and net_protocol=?");
		try {
			pstmt.setInt(1, aoServer);
			pstmt.setInt(2, ipAddress);
			pstmt.setInt(3, httpPort.getPort());
			pstmt.setString(4, httpPort.getProtocol().name().toLowerCase(Locale.ROOT));
			try (ResultSet results = pstmt.executeQuery()) {
				if(results.next()) {
					netBind=results.getInt(1);
					String bindProtocol=results.getString(2);
					if(!protocol.equals(bindProtocol)) throw new SQLException(
						"Protocol mismatch on net.\"Bind\"(pkey="
						+netBind
						+" ao_server="
						+aoServer
						+" ipAddress="
						+ipAddress
						+" port="
						+httpPort
						+" net_protocol=tcp), app_protocol is "
						+bindProtocol
						+", requested protocol is "
						+protocol
					);
				} else netBind=-1;
			}
		} catch(SQLException err) {
			System.err.println("Error from query: "+pstmt.toString());
			throw err;
		} finally {
			pstmt.close();
		}

		// Allocate the net_bind, if needed
		if(netBind == -1) {
			netBind = conn.executeIntUpdate(
				"INSERT INTO net.\"Bind\" VALUES (default,?,?,?,?,?,?,true) RETURNING pkey",
				packageName.toString(),
				aoServer,
				ipAddress,
				httpPort.getPort(),
				httpPort.getProtocol().name().toLowerCase(Locale.ROOT),
				protocol
			);
			AccountingCode business = PackageHandler.getBusinessForPackage(conn, packageName);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.NET_BINDS,
				business,
				aoServer,
				false
			);
			// Default to open in public firewalld zone
			conn.executeUpdate(
				"insert into net.\"BindFirewallZone\" (net_bind, firewalld_zone) values (\n"
				+ "  ?,\n"
				+ "  (select pkey from net.\"FirewallZone\" where server=? and \"name\"=?)\n"
				+ ")",
				netBind,
				aoServer,
				FirewalldZone.PUBLIC
			);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES,
				business,
				aoServer,
				false
			);
		}

		// Allocate the httpd_bind if needed
		if(
			conn.executeBooleanQuery(
				"select\n"
				+ "  (\n"
				+ "    select\n"
				+ "      net_bind\n"
				+ "    from\n"
				+ "      web.\"HttpdBind\"\n"
				+ "    where\n"
				+ "      net_bind=?\n"
				+ "    limit 1\n"
				+ "  ) is null",
				netBind
			)
		) {
			// Get the list of web.HttpdServer and how many web.VirtualHost there are
			int lowestPKey=-1;
			int lowestCount=Integer.MAX_VALUE;
			pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
				"select\n"
				+ "  hs.pkey,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      count(*)\n"
				+ "    from\n"
				+ "      web.\"HttpdBind\" hb,\n"
				+ "      web.\"VirtualHost\" hsb\n"
				+ "    where\n"
				+ "      hs.pkey=hb.httpd_server\n"
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
				+ "        (select pk1.accounting from billing.\"Package\" pk1 where hs.package=pk1.pkey),\n"
				+ "        (select accounting from billing.\"Package\" where name=?)\n"
				+ "      )\n"
				+ "    )\n"
				+ "  )"
			);
			try {
				pstmt.setInt(1, aoServer);
				pstmt.setString(2, packageName.toString());
				ResultSet results=pstmt.executeQuery();
				try {
					while(results.next()) {
						int pkey=results.getInt(1);
						int useCount=results.getInt(2);
						if(useCount<lowestCount) {
							lowestPKey=pkey;
							lowestCount=useCount;
						}
					}
				} finally {
					results.close();
				}
			} finally {
				pstmt.close();
			}
			if(lowestPKey==-1) throw new SQLException("Unable to determine which httpd_server to add the new httpd_bind to");
			// Insert into the DB
			conn.executeUpdate(
				"insert into web.\"HttpdBind\" values(?,?)",
				netBind,
				lowestPKey
			);
		}
		// Always invalidate these tables because adding site may grant permissions to these rows
		AccountingCode business = PackageHandler.getBusinessForPackage(conn, packageName);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_BINDS,
			business,
			aoServer,
			false
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.NET_BINDS,
			business,
			aoServer,
			false
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES,
			business,
			aoServer,
			false
		);

		return netBind;
	}

	public static void removeHttpdSharedTomcat(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "removeHttpdSharedTomcat", pkey);

		removeHttpdSharedTomcat(conn, invalidateList, pkey);
	}

	public static void removeHttpdSharedTomcat(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForHttpdSharedTomcat(conn, pkey);
		int aoServer=getAOServerForHttpdSharedTomcat(conn, pkey);

		int tomcat4Worker=conn.executeIntQuery("select coalesce(tomcat4_worker, -1) from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);
		int tomcat4ShutdownPort=conn.executeIntQuery("select coalesce(tomcat4_shutdown_port, -1) from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);

		conn.executeUpdate("delete from \"web/tomcat\".\"SharedTomcat\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			accounting,
			aoServer,
			false
		);

		if(tomcat4Worker!=-1) {
			int bind = conn.executeIntQuery("select bind from \"web/tomcat\".\"TomcatWorker\" where pkey=?", tomcat4Worker);
			conn.executeUpdate("delete from \"web/tomcat\".\"TomcatWorker\" where bind=?", tomcat4Worker);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_WORKERS, accounting, aoServer, false);

			conn.executeUpdate("delete from net.\"Bind\" where pkey=?", bind);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BINDS, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, accounting, aoServer, false);
		}

		if(tomcat4ShutdownPort!=-1) {
			conn.executeUpdate("delete from net.\"Bind\" where pkey=?", tomcat4ShutdownPort);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BINDS, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, accounting, aoServer, false);
		}
	}

	public static void removeHttpdSite(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int httpdSitePKey
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "removeHttpdSite", httpdSitePKey);

		removeHttpdSite(conn, invalidateList, httpdSitePKey);
	}

	public static boolean isHttpdStaticSite(DatabaseConnection conn, int httpd_site) throws SQLException {
		return conn.executeBooleanQuery("select (select httpd_site from web.\"StaticSite\" where httpd_site=?) is not null", httpd_site);
	}

	/**
	 * web.Site
	 *           + web.VirtualHost
	 *           |                + web.Header
	 *           |                + web.Redirect
	 *           |                + web.VirtualHostName
	 *           |                |               + dns.Record
	 *           |                + web.HttpdBind
	 *           |                            + net.Bind
	 *           + web/tomcat.TomcatSite
	 *           |                  + web/tomcat.TomcatContext
	 *           |                                        + web/tomcat.TomcatContextDataSource
	 *           |                                        + web/tomcat.TomcatContextParameter
	 *           |                  + web/tomcat.TomcatWorker
	 *           |                  |             + net.Bind
	 *           |                  + web/tomcat.SharedTomcatSite
	 *           |                  |             + linux.GroupUser
	 *           |                  + web/tomcat.PrivateTomcatSite
	 *           |                                         + net.Bind
	 *           |                  + web/jboss.Site
	 *           |                                   + net.Bind
	 *           + web.StaticSite
	 */
	public static void removeHttpdSite(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int httpdSitePKey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForHttpdSite(conn, httpdSitePKey);
		int aoServer=getAOServerForHttpdSite(conn, httpdSitePKey);
		String siteName=conn.executeStringQuery("select \"name\" from web.\"Site\" where pkey=?", httpdSitePKey);

		// OperatingSystem settings
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		// Must not contain a CVS repository
		UnixPath siteDir;
		try {
			siteDir = UnixPath.valueOf(httpdSitesDir + "/" + siteName);
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		int count = conn.executeIntQuery(
			"select\n"
			+ "  count(*)\n"
			+ "from\n"
			+ "  cvs_repositories cr,\n"
			+ "  linux.\"UserServer\" lsa\n"
			+ "where\n"
			+ "  cr.linux_server_account=lsa.pkey\n"
			+ "  and lsa.ao_server=?\n"
			+ "  and (\n"
			+ "    cr.path=?\n"
			+ "    or substring(cr.path from 1 for " + (siteDir.toString().length() + 1) + ")=?\n"
			+ "  )",
			aoServer,
			siteDir,
			siteDir + "/"
		);
		if(count > 0) {
			throw new SQLException(
				"Site directory on AOServer #" + aoServer + " contains "
				+ count + " CVS " + (count == 1 ? "repository" : "repositories")
				+ ": " + siteDir
			);
		}

		// web.Location
		if(conn.executeUpdate("delete from web.\"Location\" where httpd_site=?", httpdSitePKey) > 0) {
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_AUTHENTICATED_LOCATIONS, accounting, aoServer, false);
		}

		// web.VirtualHost
		IntList httpdSiteBinds=conn.executeIntListQuery("select pkey from web.\"VirtualHost\" where httpd_site=?", httpdSitePKey);
		if(httpdSiteBinds.size() > 0) {
			List<DomainName> tlds=DNSHandler.getDNSTLDs(conn);
			SortedIntArrayList httpdBinds=new SortedIntArrayList();
			for(int c=0;c<httpdSiteBinds.size();c++) {
				int httpdSiteBind=httpdSiteBinds.getInt(c);

				// web.VirtualHostName
				IntList httpdSiteURLs=conn.executeIntListQuery("select pkey from web.\"VirtualHostName\" where httpd_site_bind=?", httpdSiteBind);
				for(int d=0;d<httpdSiteURLs.size();d++) {
					int httpdSiteURL=httpdSiteURLs.getInt(d);

					// dns.Record
					DomainName hostname = conn.executeObjectQuery(
						ObjectFactories.domainNameFactory,
						"select hostname from web.\"VirtualHostName\" where pkey=?",
						httpdSiteURL
					);
					conn.executeUpdate("delete from web.\"VirtualHostName\" where pkey=?", httpdSiteURL);
					invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_URLS, accounting, aoServer, false);
					DNSHandler.removeUnusedDNSRecord(conn, invalidateList, hostname, tlds);
				}

				int hb=conn.executeIntQuery("select httpd_bind from web.\"VirtualHost\" where pkey=?", httpdSiteBind);
				if(!httpdBinds.contains(hb)) httpdBinds.add(hb);
			}
			conn.executeUpdate("delete from web.\"VirtualHost\" where httpd_site=?", httpdSitePKey);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BINDS, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BIND_HEADERS, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BIND_REDIRECTS, accounting, aoServer, false);

			for(int c=0;c<httpdBinds.size();c++) {
				int httpdBind=httpdBinds.getInt(c);
				if(
					conn.executeBooleanQuery(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      pkey\n"
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
					if(
						conn.executeBooleanQuery(
							"select\n"
							+ "  not ia.\"isOverflow\"\n"
							+ "from\n"
							+ "  net.\"Bind\" nb,\n"
							+ "  net.\"IpAddress\" ia\n"
							+ "where\n"
							+ "  nb.pkey=?\n"
							+ "  and nb.\"ipAddress\"=ia.id",
							httpdBind
						)
					) {
						conn.executeUpdate("delete from web.\"HttpdBind\" where net_bind=?", httpdBind);
						conn.executeUpdate("delete from net.\"Bind\" where pkey=?", httpdBind);
					}
				}
			}
		}

		// web/tomcat.TomcatSite
		if(conn.executeBooleanQuery("select (select httpd_site from \"web/tomcat\".\"TomcatSite\" where httpd_site=? limit 1) is not null", httpdSitePKey)) {
			// web/tomcat.TomcatContextDataSource
			IntList htdss=conn.executeIntListQuery("select htds.pkey from \"web/tomcat\".\"TomcatContext\" htc, \"web/tomcat\".\"TomcatContextDataSource\" htds where htc.tomcat_site=? and htc.pkey=htds.tomcat_context", httpdSitePKey);
			if(htdss.size() > 0) {
				for(int c=0;c<htdss.size();c++) {
					conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextDataSource\" where pkey=?", htdss.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES, accounting, aoServer, false);
			}

			// web/tomcat.TomcatContextParameter
			IntList htps=conn.executeIntListQuery("select htp.pkey from \"web/tomcat\".\"TomcatContext\" htc, \"web/tomcat\".\"TomcatContextParameter\" htp where htc.tomcat_site=? and htc.pkey=htp.tomcat_context", httpdSitePKey);
			if(htps.size() > 0) {
				for(int c=0;c<htps.size();c++) {
					conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextParameter\" where pkey=?", htps.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS, accounting, aoServer, false);
			}

			// web/tomcat.TomcatContext
			IntList htcs=conn.executeIntListQuery("select pkey from \"web/tomcat\".\"TomcatContext\" where tomcat_site=?", httpdSitePKey);
			if(htcs.size() > 0) {
				for(int c=0;c<htcs.size();c++) {
					conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContext\" where pkey=?", htcs.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS, accounting, aoServer, false);
			}

			// web/tomcat.TomcatWorker
			IntList httpdWorkers = conn.executeIntListQuery("select bind from \"web/tomcat\".\"TomcatWorker\" where \"tomcatSite\"=?", httpdSitePKey);
			if(httpdWorkers.size() > 0) {
				for(int c=0;c<httpdWorkers.size();c++) {
					int bind = httpdWorkers.getInt(c);
					conn.executeUpdate("delete from \"web/tomcat\".\"TomcatWorker\" where bind=?", bind);
					NetBindHandler.removeNetBind(conn, invalidateList, bind);
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_WORKERS, accounting, aoServer, false);
			}

			// web/tomcat.SharedTomcatSite
			if(conn.executeUpdate("delete from \"web/tomcat\".\"SharedTomcatSite\" where tomcat_site=?", httpdSitePKey) > 0) {
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SHARED_SITES, accounting, aoServer, false);
			}

			// web/tomcat.PrivateTomcatSite
			if(conn.executeBooleanQuery("select (select tomcat_site from \"web/tomcat\".\"PrivateTomcatSite\" where tomcat_site=? limit 1) is not null", httpdSitePKey)) {
				int tomcat4ShutdownPort=conn.executeIntQuery("select coalesce(tomcat4_shutdown_port, -1) from \"web/tomcat\".\"PrivateTomcatSite\" where tomcat_site=?", httpdSitePKey);

				conn.executeUpdate("delete from \"web/tomcat\".\"PrivateTomcatSite\" where tomcat_site=?", httpdSitePKey);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES, accounting, aoServer, false);

				if(tomcat4ShutdownPort!=-1) {
					conn.executeUpdate("delete from net.\"Bind\" where pkey=?", tomcat4ShutdownPort);
					invalidateList.addTable(conn, SchemaTable.TableID.NET_BINDS, accounting, aoServer, false);
					invalidateList.addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, accounting, aoServer, false);
				}
			}

			// web/jboss.Site
			if(conn.executeBooleanQuery("select (select tomcat_site from \"web/jboss\".\"Site\" where tomcat_site=? limit 1) is not null", httpdSitePKey)) {
				// net.Bind
				int jnp_bind=conn.executeIntQuery("select jnp_bind from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);
				int webserver_bind=conn.executeIntQuery("select webserver_bind from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);
				int rmi_bind=conn.executeIntQuery("select rmi_bind from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);
				int hypersonic_bind=conn.executeIntQuery("select hypersonic_bind from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);
				int jmx_bind=conn.executeIntQuery("select jmx_bind from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);

				conn.executeUpdate("delete from \"web/jboss\".\"Site\" where tomcat_site=?", httpdSitePKey);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_JBOSS_SITES, accounting, aoServer, false);
				NetBindHandler.removeNetBind(conn, invalidateList, jnp_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, webserver_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, rmi_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, hypersonic_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, jmx_bind);
			}

			conn.executeUpdate("delete from \"web/tomcat\".\"TomcatSite\" where httpd_site=?", httpdSitePKey);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITES, accounting, aoServer, false);
		}

		// web.StaticSite
		if(conn.executeUpdate("delete from web.\"StaticSite\" where httpd_site=?", httpdSitePKey) != 0) {
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_STATIC_SITES, accounting, aoServer, false);
		}

		// web.Site
		conn.executeUpdate("delete from web.\"Site\" where pkey=?", httpdSitePKey);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITES, accounting, aoServer, false);
	}

	public static void removeHttpdServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForHttpdServer(conn, pkey);
		int aoServer = getAOServerForHttpdServer(conn, pkey);

		// web.Site
		conn.executeUpdate("delete from web.\"HttpdServer\" where pkey=?", pkey);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SERVERS, accounting, aoServer, false);
	}

	public static void removeHttpdSiteAuthenticatedLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int httpd_site=conn.executeIntQuery("select httpd_site from web.\"Location\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdSiteAuthenticatedLocation", httpd_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, httpd_site);
		int aoServer=getAOServerForHttpdSite(conn, httpd_site);

		conn.executeUpdate("delete from web.\"Location\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
			accounting,
			aoServer,
			false
		);
	}

	public static void removeHttpdSiteURL(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int hs=getHttpdSiteForHttpdSiteURL(conn, pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdSiteURL", hs);
		if(conn.executeBooleanQuery("select is_primary from web.\"VirtualHostName\" where pkey=?", pkey)) throw new SQLException("Not allowed to remove the primary hostname: "+pkey);
		if(
			conn.executeBooleanQuery(
				"select\n"
				+ "  (\n"
				+ "    select hostname from web.\"VirtualHostName\" where pkey=?\n"
				+ "  )=(\n"
				+ "    select hs.\"name\"||'.'||ao.hostname from web.\"Site\" hs, linux.\"Server\" ao where hs.pkey=? and hs.ao_server=ao.server\n"
				+ "  )",
				pkey,
				hs
			)
		) throw new SQLException("Not allowed to remove a test URL: "+pkey);

		conn.executeUpdate("delete from web.\"VirtualHostName\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_URLS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void removeHttpdTomcatContext(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int tomcat_site = conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatContext", tomcat_site);
		String path = conn.executeStringQuery("select path from \"web/tomcat\".\"TomcatContext\" where pkey=?", pkey);
		if(path.isEmpty()) throw new SQLException("Not allowed to remove the default context: " + pkey);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		if(conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextDataSource\" where tomcat_context=?", pkey) > 0) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES,
				accounting,
				aoServer,
				false
			);
		}

		if(conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextParameter\" where tomcat_context=?", pkey) > 0) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
				accounting,
				aoServer,
				false
			);
		}

		conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContext\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			accounting,
			aoServer,
			false
		);

		if(
			conn.executeUpdate(
				"delete from \"web/tomcat\".\"TomcatJkMount\" where httpd_tomcat_site=? and substring(path from 1 for ?)=?",
				tomcat_site,
				path.length() + 1,
				path + '/'
			) > 0
		) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
				accounting,
				aoServer,
				false
			);
		}
	}

	public static void removeHttpdTomcatDataSource(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int tomcat_context=conn.executeIntQuery("select tomcat_context from \"web/tomcat\".\"TomcatContextDataSource\" where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatDataSource", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextDataSource\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES,
			accounting,
			aoServer,
			false
		);
	}

	public static void removeHttpdTomcatParameter(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int tomcat_context=conn.executeIntQuery("select tomcat_context from \"web/tomcat\".\"TomcatContextParameter\" where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatParameter", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate("delete from \"web/tomcat\".\"TomcatContextParameter\" where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
			accounting,
			aoServer,
			false
		);
	}

	public static void updateHttpdTomcatDataSource(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
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
		int tomcat_context=conn.executeIntQuery("select tomcat_context from \"web/tomcat\".\"TomcatContextDataSource\" where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "updateHttpdTomcatDataSource", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate(
			"update \"web/tomcat\".\"TomcatContextDataSource\" set name=?, driver_class_name=?, url=?, username=?, password=?, max_active=?, max_idle=?, max_wait=?, validation_query=? where pkey=?",
			name,
			driverClassName,
			url,
			username,
			password,
			maxActive,
			maxIdle,
			maxWait,
			validationQuery,
			pkey
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES,
			accounting,
			aoServer,
			false
		);
	}

	public static void updateHttpdTomcatParameter(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		String name,
		String value,
		boolean override,
		String description
	) throws IOException, SQLException {
		int tomcat_context=conn.executeIntQuery("select tomcat_context from \"web/tomcat\".\"TomcatContextParameter\" where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "updateHttpdTomcatParameter", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate(
			"update \"web/tomcat\".\"TomcatContextParameter\" set name=?, value=?, override=?, description=? where pkey=?",
			name,
			value,
			override,
			description,
			pkey
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
			accounting,
			aoServer,
			false
		);
	}

	public static void restartApache(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_apache");
		if(!canControl) throw new SQLException("Not allowed to restart Apache on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).restartApache();
	}

	public static void setHttpdSharedTomcatIsManual(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean isManual
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "setHttpdSharedTomcatIsManual", pkey);
		if(isHttpdSharedTomcatDisabled(conn, pkey)) throw new SQLException("Unable to set is_manual flag: HttpdSharedTomcat disabled: "+pkey);

		// Update the database
		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set is_manual=? where pkey=?",
			isManual,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	public static void setHttpdSharedTomcatMaxPostSize(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int maxPostSize
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "setHttpdSharedTomcatMaxPostSize", pkey);

		// Update the database
		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set max_post_size=? where pkey=?",
			maxPostSize==-1 ? DatabaseAccess.Null.INTEGER : maxPostSize,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	public static void setHttpdSharedTomcatUnpackWARs(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean unpackWARs
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "setHttpdSharedTomcatUnpackWARs", pkey);

		// Update the database
		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set unpack_wars=? where pkey=?",
			unpackWARs,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	public static void setHttpdSharedTomcatAutoDeploy(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean autoDeploy
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "setHttpdSharedTomcatAutoDeploy", pkey);

		// Update the database
		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set auto_deploy=? where pkey=?",
			autoDeploy,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			getAOServerForHttpdSharedTomcat(conn, pkey),
			false
		);
	}

	private static void checkUpgradeFrom(String fromVersion) throws SQLException {
		if(!HttpdTomcatVersion.canUpgradeFrom(fromVersion)) {
			throw new SQLException("In-place Tomcat upgrades and downgrades are only supported from Tomcat 4.1 and newer, not supported from version \"" + fromVersion + "\".");
		}
	}

	private static void checkUpgradeTo(String toVersion) throws SQLException {
		if(!HttpdTomcatVersion.canUpgradeTo(toVersion)) {
			throw new SQLException("In-place Tomcat upgrades and downgrades are only supported to Tomcat 8.5 and newer, not supported to version \"" + toVersion + "\".");
		}
	}

	public static void setHttpdSharedTomcatVersion(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int version
	) throws IOException, SQLException {
		checkAccessHttpdSharedTomcat(conn, source, "setHttpdSharedTomcatVersion", pkey);

		// Make sure the version change is acceptable
		checkUpgradeFrom(
			conn.executeStringQuery(
				"select\n"
				+ "  tv.version\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"SharedTomcat\" hst\n"
				+ "  inner join distribution.\"SoftwareVersion\" tv on hst.version=tv.pkey\n"
				+ "where hst.pkey=?",
				pkey
			)
		);
		checkUpgradeTo(
			conn.executeStringQuery(
				"select\n"
				+ "  tv.version\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"TomcatVersion\" htv\n"
				+ "  inner join distribution.\"SoftwareVersion\" tv on htv.version=tv.pkey\n"
				+ "where htv.version=?",
				version
			)
		);

		// Make sure operating system version matches
		int aoServer = getAOServerForHttpdSharedTomcat(conn, pkey);
		int fromOsv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		int toOsv = conn.executeIntQuery(
			"select operating_system_version from distribution.\"SoftwareVersion\" where pkey=?",
			version
		);
		if(fromOsv != toOsv) throw new SQLException("OperatingSystemVersion mismatch: " + fromOsv + " != " + toOsv);
		// TODO: Check osv match on adding new sites (Tomcat versions)

		// TODO: Check if disabled in distribution.SoftwareVersion (do this in set PHP version, too)
		// TODO: Add update of this in end-of-life tasks
		// TODO: Check this "disable_time" in control panels, too.
		// TODO: Check this on add site (both PHP and Tomcat versions)

		// Update the database
		conn.executeUpdate(
			"update \"web/tomcat\".\"SharedTomcat\" set version=? where pkey=?",
			version,
			pkey
		);
		// TODO: Update the context paths to an webapps in /opt/apache-tomcat.../webpaps to the new version
		// TODO: See web/tomcat.TomcatVersion table
		conn.executeUpdate(
			"update \"web/tomcat\".\"TomcatSite\" set version=? where httpd_site in (\n"
			+ "  select tomcat_site from \"web/tomcat\".\"SharedTomcatSite\" where httpd_shared_tomcat=?\n"
			+ ")",
			version,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			getBusinessForHttpdSharedTomcat(conn, pkey),
			aoServer,
			false
		);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_SITES,
			InvalidateList.allBusinesses, // TODO: Could be more selective here
			aoServer,
			false
		);
	}

	public static void setHttpdSiteAuthenticatedLocationAttributes(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		String path,
		boolean isRegularExpression,
		String authName,
		UnixPath authGroupFile,
		UnixPath authUserFile,
		String require,
		String handler
	) throws IOException, SQLException {
		int httpd_site=conn.executeIntQuery("select httpd_site from web.\"Location\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "setHttpdSiteAuthenticatedLocationAttributes", httpd_site);
		if(isHttpdSiteDisabled(conn, httpd_site)) throw new SQLException("Unable to set HttpdSiteAuthenticatedLocation attributes, HttpdSite disabled: "+httpd_site);
		String error = HttpdSiteAuthenticatedLocation.validatePath(path);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthName(authName);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthGroupFile(authGroupFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthUserFile(authUserFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateRequire(require);
		if(error!=null) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation: "+error);
		if(HttpdSiteAuthenticatedLocation.Handler.CURRENT.equals(handler)) {
			conn.executeUpdate(
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
				+ "  pkey=?",
				path,
				isRegularExpression,
				authName,
				authGroupFile==null ? "" : authGroupFile.toString(),
				authUserFile==null ? "" : authUserFile.toString(),
				require,
				pkey
			);
		} else {
			conn.executeUpdate(
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
				+ "  pkey=?",
				path,
				isRegularExpression,
				authName,
				authGroupFile==null ? "" : authGroupFile.toString(),
				authUserFile==null ? "" : authUserFile.toString(),
				require,
				handler,
				pkey
			);
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_AUTHENTICATED_LOCATIONS,
			getBusinessForHttpdSite(conn, httpd_site),
			getAOServerForHttpdSite(conn, httpd_site),
			false
		);
	}

	public static void setHttpdSiteBindIsManual(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean isManual
	) throws IOException, SQLException {
		int hs=getHttpdSiteForHttpdSiteBind(conn, pkey);
		checkAccessHttpdSite(conn, source, "setHttpdSiteBindIsManual", hs);
		if(isHttpdSiteBindDisabled(conn, pkey)) throw new SQLException("Unable to set is_manual flag: HttpdSiteBind disabled: "+pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"VirtualHost\" set is_manual=? where pkey=?",
			isManual,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_BINDS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void setHttpdSiteBindRedirectToPrimaryHostname(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean redirect_to_primary_hostname
	) throws IOException, SQLException {
		int hs=getHttpdSiteForHttpdSiteBind(conn, pkey);
		checkAccessHttpdSite(conn, source, "setHttpdSiteBindRedirectToPrimaryHostname", hs);
		if(isHttpdSiteBindDisabled(conn, pkey)) throw new SQLException("Unable to set redirect_to_primary_hostname flag: HttpdSiteBind disabled: "+pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"VirtualHost\" set redirect_to_primary_hostname=? where pkey=?",
			redirect_to_primary_hostname,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_BINDS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void setHttpdSiteBindPredisableConfig(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int hsb,
		String config
	) throws IOException, SQLException {
		int hs=getHttpdSiteForHttpdSiteBind(conn, hsb);
		checkAccessHttpdSite(conn, source, "setHttpdSiteBindPredisableConfig", hs);
		if(config==null) {
			if(isHttpdSiteBindDisabled(conn, hsb)) throw new SQLException("Unable to clear HttpdSiteBind predisable config, bind disabled: "+hsb);
		} else {
			if(!isHttpdSiteBindDisabled(conn, hsb)) throw new SQLException("Unable to set HttpdSiteBind predisable config, bind not disabled: "+hsb);
		}

		// Update the database
		conn.executeUpdate(
			"update web.\"VirtualHost\" set predisable_config=? where pkey=?",
			config,
			hsb
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_BINDS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void setHttpdSiteIsManual(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean isManual
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteIsManual", pkey);
		if(isHttpdSiteDisabled(conn, pkey)) throw new SQLException("Unable to set is_manual flag: HttpdSite disabled: "+pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set is_manual=? where pkey=?",
			isManual,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteServerAdmin(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		Email emailAddress
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteServerAdmin", pkey);
		if(isHttpdSiteDisabled(conn, pkey)) throw new SQLException("Unable to set server administrator: HttpdSite disabled: "+pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set server_admin=? where pkey=?",
			emailAddress,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSitePhpVersion(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int phpVersion
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSitePhpVersion", pkey);
		int aoServer = getAOServerForHttpdSite(conn, pkey);
		if(phpVersion != -1) {
			if(isHttpdStaticSite(conn, pkey)) {
				// TODO: This would be better modeled by not having php_version on the web.Site table, but rather more specialized types of sites
				// TODO: How to enable PHP on a per-site basis, so static site under mod_php apache doesn't get php?
				throw new SQLException("May not enable PHP on a static site");
			}
			int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
			// Version must be correct for this server
			int tvOsv = conn.executeIntQuery(
				"select coalesce(\n"
				+ "  (select operating_system_version from distribution.\"SoftwareVersion\" where pkey=? and name=?),\n"
				+ "  -1\n"
				+ ")",
				phpVersion,
				TechnologyName.PHP
			);
			if(tvOsv == -1) throw new SQLException("Requested PHP version is not a PHP version: #" + phpVersion);
			if(tvOsv != osv) throw new SQLException("Requested PHP version is for the wrong operating system version: #" + phpVersion + ": " + tvOsv + " != " + osv);
		}
		// Update the database
		int updateCount;
		if(phpVersion == -1) {
			updateCount = conn.executeUpdate(
				"update web.\"Site\" set php_version=null where pkey=? and php_version is not null",
				pkey
			);
		} else {
			updateCount = conn.executeUpdate(
				"update web.\"Site\" set php_version=? where pkey=? and php_version!=?",
				phpVersion,
				pkey,
				phpVersion
			);
		}
		if(updateCount > 0) {
			AccountingCode accounting = getBusinessForHttpdSite(conn, pkey);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_SITES,
				accounting,
				aoServer,
				false
			);

			boolean useApache = conn.executeBooleanQuery(
				"select (\n"
				+ "  select pkey from \"web/tomcat\".\"TomcatJkMount\"\n"
				+ "  where (httpd_tomcat_site, path)=(?, '/*')\n"
				+ ") is null",
				pkey
			);
			boolean hasPhp;
			if(phpVersion != -1) {
				// CGI-based PHP
				hasPhp = true;
			} else {
				// Check for mod_php
				hasPhp = conn.executeBooleanQuery(
					"select (\n"
					+ "  select\n"
					+ "    hs.pkey\n"
					+ "  from\n"
					+ "    web.\"VirtualHost\" hsb\n"
					+ "    inner join web.\"HttpdBind\" hb on hsb.httpd_bind=hb.net_bind\n"
					+ "    inner join web.\"HttpdServer\" hs on hb.httpd_server=hs.pkey\n"
					+ "  where\n"
					+ "    hsb.httpd_site=?\n"
					+ "    and hs.mod_php_version is not null\n"
					+ "  limit 1\n"
					+ ") is not null",
					pkey
				);
			}
			if(!useApache && hasPhp) {
				if(
					conn.executeBooleanQuery(
						"select (\n"
						+ "  select pkey from \"web/tomcat\".\"TomcatJkMount\"\n"
						+ "  where (httpd_tomcat_site, path)=(?, '/*.php')\n"
						+ ") is null",
						pkey
					)
				) {
					// Add /*.php to JkUnMounts
					conn.executeUpdate(
						"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,'/*.php',FALSE)",
						pkey
					);
					invalidateList.addTable(
						conn,
						SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
						accounting,
						aoServer,
						false
					);
				}
			} else {
				// Remove /*.php from JkUnMounts
				if(
					conn.executeUpdate(
						"delete from \"web/tomcat\".\"TomcatJkMount\" where (httpd_tomcat_site, path, mount)=(?,'/*.php',FALSE)",
						pkey
					) > 0
				) {
					invalidateList.addTable(
						conn,
						SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
						accounting,
						aoServer,
						false
					);
				}
			}
		}
	}

	public static void setHttpdSiteEnableCgi(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableCgi
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableCgi", pkey);

		if(enableCgi && isHttpdStaticSite(conn, pkey)) {
			// TODO: This would be better modeled by not having enable_cgi on the web.Site table, but rather more specialized types of sites
			throw new SQLException("May not enable CGI on a static site");
		}

		// Update the database
		if(
			conn.executeUpdate(
				"update web.\"Site\" set enable_cgi=? where pkey=? and enable_cgi != ?",
				enableCgi,
				pkey,
				enableCgi
			) > 0
		) {
			AccountingCode accounting = getBusinessForHttpdSite(conn, pkey);
			int aoServer = getAOServerForHttpdSite(conn, pkey);
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_SITES,
				accounting,
				aoServer,
				false
			);
			List<String> paths = conn.executeStringListQuery("select path from \"web/tomcat\".\"TomcatContext\" where tomcat_site=?", pkey);
			if(!paths.isEmpty()) {
				for(String path : paths) {
					if(enableCgi) {
						// Add /cgi-bin to JkUnMounts
						conn.executeUpdate(
							"insert into \"web/tomcat\".\"TomcatJkMount\" (httpd_tomcat_site, path, mount) values (?,?,FALSE)",
							pkey,
							checkJkMountPath(path + "/cgi-bin/*")
						);
					} else {
						// Remove /cgi-bin from JkUnMounts
						conn.executeUpdate(
							"delete from \"web/tomcat\".\"TomcatJkMount\" where (httpd_tomcat_site, path, mount)=(?,?,FALSE)",
							pkey,
							checkJkMountPath(path + "/cgi-bin/*")
						);
					}
				}
			}
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
				accounting,
				aoServer,
				false
			);
		}
	}

	public static void setHttpdSiteEnableSsi(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableSsi
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableSsi", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set enable_ssi=? where pkey=?",
			enableSsi,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteEnableHtaccess(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableHtaccess
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableHtaccess", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set enable_htaccess=? where pkey=?",
			enableHtaccess,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteEnableIndexes(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableIndexes
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableIndexes", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set enable_indexes=? where pkey=?",
			enableIndexes,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteEnableFollowSymlinks(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableFollowSymlinks
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableFollowSymlinks", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set enable_follow_symlinks=? where pkey=?",
			enableFollowSymlinks,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteEnableAnonymousFtp(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableAnonymousFtp
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableAnonymousFtp", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set enable_anonymous_ftp=? where pkey=?",
			enableAnonymousFtp,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteBlockTraceTrack(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean blockTraceTrack
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteBlockTraceTrack", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set block_trace_track=? where pkey=?",
			blockTraceTrack,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteBlockScm(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean blockScm
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteBlockScm", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set block_scm=? where pkey=?",
			blockScm,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteBlockCoreDumps(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean blockCoreDumps
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteBlockCoreDumps", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set block_core_dumps=? where pkey=?",
			blockCoreDumps,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdSiteBlockEditorBackups(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean blockEditorBackups
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteBlockEditorBackups", pkey);

		// Update the database
		conn.executeUpdate(
			"update web.\"Site\" set block_editor_backups=? where pkey=?",
			blockEditorBackups,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	@SuppressWarnings("deprecation") // Java 1.7: Do not suppress
	public static int setHttpdTomcatContextAttributes(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		String className,
		boolean cookies,
		boolean crossContext,
		UnixPath docBase,
		boolean override,
		String path,
		boolean privileged,
		boolean reloadable,
		boolean useNaming,
		String wrapperClass,
		int debug,
		UnixPath workDir,
		boolean serverXmlConfigured
	) throws IOException, SQLException {
		int tomcat_site=conn.executeIntQuery("select tomcat_site from \"web/tomcat\".\"TomcatContext\" where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "setHttpdTomcatContextAttributes", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to set HttpdTomcatContext attributes, HttpdSite disabled: "+tomcat_site);
		checkHttpdTomcatContext(
			conn,
			source,
			tomcat_site,
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

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		String oldPath=conn.executeStringQuery("select path from \"web/tomcat\".\"TomcatContext\" where pkey=?", pkey);
		if(oldPath.length()==0 && path.length() > 0) throw new SQLException("Not allowed to change the path of the default context: "+path);

		try (PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"update\n"
			+ "  \"web/tomcat\".\"TomcatContext\"\n"
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
			+ "  pkey=?"
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
				pstmt.setString(12, ObjectUtils.toString(workDir));
				pstmt.setBoolean(13, serverXmlConfigured);
				pstmt.setInt(14, pkey);

				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

		if(
			!path.equals(oldPath)
			&& conn.executeUpdate(
				"update \"web/tomcat\".\"TomcatJkMount\"\n"
				+ "set path = ? || substring(path from ?)\n"
				+ "where httpd_tomcat_site=? and substring(path from 1 for ?)=?",
				path,
				oldPath.length(),
				tomcat_site,
				oldPath.length() + 1,
				oldPath + '/'
			) > 0
		) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS,
				accounting,
				aoServer,
				false
			);
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			accounting,
			aoServer,
			false
		);

		return pkey;
	}

	public static void setHttpdTomcatStdSiteMaxPostSize(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int maxPostSize
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatStdSiteMaxPostSize", pkey);

		// Update the database
		int updateCount = conn.executeUpdate(
			"update \"web/tomcat\".\"PrivateTomcatSite\" set max_post_size=? where httpd_site=?",
			maxPostSize==-1 ? DatabaseAccess.Null.INTEGER : maxPostSize,
			pkey
		);
		if(updateCount == 0) throw new SQLException("Not a HttpdTomcatStdSite: #" + pkey);
		if(updateCount != 1) throw new SQLException("Unexpected updateCount: " + updateCount);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdTomcatStdSiteUnpackWARs(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean unpackWARs
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatStdSiteUnpackWARs", pkey);

		// Update the database
		int updateCount = conn.executeUpdate(
			"update \"web/tomcat\".\"PrivateTomcatSite\" set unpack_wars=? where httpd_site=?",
			unpackWARs,
			pkey
		);
		if(updateCount == 0) throw new SQLException("Not a HttpdTomcatStdSite: #" + pkey);
		if(updateCount != 1) throw new SQLException("Unexpected updateCount: " + updateCount);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdTomcatStdSiteAutoDeploy(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean autoDeploy
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatStdSiteAutoDeploy", pkey);

		// Update the database
		int updateCount = conn.executeUpdate(
			"update \"web/tomcat\".\"PrivateTomcatSite\" set auto_deploy=? where httpd_site=?",
			autoDeploy,
			pkey
		);
		if(updateCount == 0) throw new SQLException("Not a HttpdTomcatStdSite: #" + pkey);
		if(updateCount != 1) throw new SQLException("Unexpected updateCount: " + updateCount);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void setHttpdTomcatStdSiteVersion(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		int version
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatStdSiteVersion", pkey);

		// Make sure the version change is acceptable
		checkUpgradeFrom(
			conn.executeStringQuery(
				"select\n"
				+ "  tv.version\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"PrivateTomcatSite\" htss\n"
				+ "  inner join \"web/tomcat\".\"TomcatSite\" hts on htss.tomcat_site=hts.httpd_site\n"
				+ "  inner join distribution.\"SoftwareVersion\" tv on hts.version=tv.pkey\n"
				+ "where htss.tomcat_site=?",
				pkey
			)
		);
		checkUpgradeTo(
			conn.executeStringQuery(
				"select\n"
				+ "  tv.version\n"
				+ "from\n"
				+ "  \"web/tomcat\".\"TomcatVersion\" htv\n"
				+ "  inner join distribution.\"SoftwareVersion\" tv on htv.version=tv.pkey\n"
				+ "where htv.version=?",
				version
			)
		);

		// Make sure operating system version matches
		int aoServer = getAOServerForHttpdSite(conn, pkey);
		int fromOsv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		int toOsv = conn.executeIntQuery(
			"select operating_system_version from distribution.\"SoftwareVersion\" where pkey=?",
			version
		);
		if(fromOsv != toOsv) throw new SQLException("OperatingSystemVersion mismatch: " + fromOsv + " != " + toOsv);
		// TODO: Check osv match on adding new sites (Tomcat versions)

		// TODO: Check if disabled in distribution.SoftwareVersion (do this in set PHP version, too)
		// TODO: Add update of this in end-of-life tasks
		// TODO: Check this "disable_time" in control panels, too.
		// TODO: Check this on add site (both PHP and Tomcat versions)

		// Update the database
		// TODO: Update the context paths to an webapps in /opt/apache-tomcat.../webpaps to the new version
		// TODO: See web/tomcat.TomcatVersion table (might shared with the same code above)
		conn.executeUpdate(
			"update \"web/tomcat\".\"TomcatSite\" set version=? where httpd_site=?",
			version,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_SITES,
			getBusinessForHttpdSite(conn, pkey),
			aoServer,
			false
		);
	}

	public static void setPrimaryHttpdSiteURL(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int hsb=conn.executeIntQuery("select httpd_site_bind from web.\"VirtualHostName\" where pkey=?", pkey);
		int hs=getHttpdSiteForHttpdSiteBind(conn, hsb);
		checkAccessHttpdSite(conn, source, "setPrimaryHttpdSiteURL", hs);

		conn.executeUpdate("update web.\"VirtualHostName\" set is_primary=(pkey=?) where httpd_site_bind=?", pkey, hsb);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_URLS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void setHttpdTomcatSiteBlockWebinf(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean blockWebinf
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatSiteBlockWebinf", pkey);

		// Update the database
		int updateCount = conn.executeUpdate(
			"update \"web/tomcat\".\"TomcatSite\" set block_webinf=? where httpd_site=?",
			blockWebinf,
			pkey
		);
		if(updateCount == 0) throw new SQLException("Not a HttpdTomcatSite: #" + pkey);
		if(updateCount != 1) throw new SQLException("Unexpected updateCount: " + updateCount);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_SITES,
			getBusinessForHttpdSite(conn, pkey),
			getAOServerForHttpdSite(conn, pkey),
			false
		);
	}

	public static void startApache(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_apache");
		if(!canControl) throw new SQLException("Not allowed to start Apache on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).startApache();
	}

	public static void stopApache(
		DatabaseConnection conn,
		RequestSource source,
		int aoServer
	) throws IOException, SQLException {
		boolean canControl=BusinessHandler.canBusinessServer(conn, source, aoServer, "can_control_apache");
		if(!canControl) throw new SQLException("Not allowed to stop Apache on "+aoServer);
		DaemonHandler.getDaemonConnector(conn, aoServer).stopApache();
	}

	public static void getAWStatsFile(
		DatabaseConnection conn,
		RequestSource source,
		int pkey,
		String path,
		String queryString,
		CompressedDataOutputStream out
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "getAWStatsFile", pkey);

		DaemonHandler.getDaemonConnector(
			conn,
			getAOServerForHttpdSite(conn, pkey)
		).getAWStatsFile(
			getSiteNameForHttpdSite(conn, pkey),
			path,
			queryString,
			out
		);
	}
}
