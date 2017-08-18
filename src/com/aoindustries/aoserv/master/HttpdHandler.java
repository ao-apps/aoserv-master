/*
 * Copyright 2001-2013, 2014, 2015, 2016, 2017 by AO Industries, Inc.,
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
import java.sql.Types;
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

	public static int addHttpdWorker(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int netBindPKey,
		int httpdSitePKey
	) throws IOException, SQLException {
		int server=NetBindHandler.getServerForNetBind(conn, netBindPKey);
		if(!ServerHandler.isAOServer(conn, server)) throw new SQLException("Server is not an AOServer: "+server);
		int aoServer = server;
		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_workers_pkey_seq')");
		if(httpdSitePKey==-1) {
			conn.executeUpdate(
				"insert into\n"
				+ "  httpd_workers\n"
				+ "values(\n"
				+ "  ?,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      hjc.code\n"
				+ "    from\n"
				+ "      httpd_jk_codes hjc\n"
				+ "    where\n"
				+ "      (\n"
				+ "        select\n"
				+ "          hw.code\n"
				+ "        from\n"
				+ "          httpd_workers hw,\n"
				+ "          net_binds nb\n"
				+ "        where\n"
				+ "          hw.net_bind=nb.pkey\n"
				+ "          and nb.server=?\n"
				+ "          and hjc.code=hw.code\n"
				+ "        limit 1\n"
				+ "      ) is null\n"
				+ "    order by\n"
				+ "      code\n"
				+ "    limit 1\n"
				+ "  ),\n"
				+ "  ?,\n"
				+ "  null\n"
				+ ")",
				pkey,
				aoServer,
				netBindPKey
			);
		} else {
			conn.executeUpdate(
				"insert into\n"
				+ "  httpd_workers\n"
				+ "values(\n"
				+ "  ?,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      hjc.code\n"
				+ "    from\n"
				+ "      httpd_jk_codes hjc\n"
				+ "    where\n"
				+ "      (\n"
				+ "        select\n"
				+ "          hw.code\n"
				+ "        from\n"
				+ "          httpd_workers hw,\n"
				+ "          net_binds nb\n"
				+ "        where\n"
				+ "          hw.net_bind=nb.pkey\n"
				+ "          and nb.server=?\n"
				+ "          and hjc.code=hw.code\n"
				+ "        limit 1\n"
				+ "      ) is null\n"
				+ "    order by\n"
				+ "      code\n"
				+ "    limit 1\n"
				+ "  ),\n"
				+ "  ?,\n"
				+ "  ?\n"
				+ ")",
				pkey,
				aoServer,
				netBindPKey,
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
		return pkey;
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

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_site_urls_pkey_seq')");
		conn.executeUpdate(
			"insert into\n"
			+ "  httpd_site_urls\n"
			+ "values (\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  (select pkey from httpd_site_urls where httpd_site_bind=? and is_primary limit 1) is null\n"
			+ ")",
			pkey,
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
				conn.executeIntQuery("select linux_server_group from httpd_shared_tomcats where pkey=?", pkey)
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

	/**
	 * Gets the pkey of a HttpdSite given its server and name or {@code -1} if not found.
	 */
	public static int getHttpdSite(DatabaseConnection conn, int aoServer, String siteName) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select coalesce(\n"
			+ "  (select pkey from httpd_sites where (ao_server, site_name)=(?,?)),\n"
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
			+ "  (select pkey from httpd_shared_tomcats where (ao_server, name)=(?,?)),\n"
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
		String require
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "addHttpdSiteAuthenticatedLocation", httpd_site);
		if(isHttpdSiteDisabled(conn, httpd_site)) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation, HttpdSite disabled: "+httpd_site);
		String error = HttpdSiteAuthenticatedLocation.validatePath(path);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthName(authName);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthGroupFile(authGroupFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthUserFile(authUserFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateRequire(require);
		if(error!=null) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation: "+error);

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_site_authenticated_locations_pkey_seq')");
		conn.executeUpdate(
			"insert into\n"
			+ "  httpd_site_authenticated_locations\n"
			+ "values (\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ")",
			pkey,
			httpd_site,
			path,
			isRegularExpression,
			authName,
			authGroupFile==null ? "" : authGroupFile.toString(),
			authUserFile==null ? "" : authUserFile.toString(),
			require
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
		UnixPath workDir
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
			workDir
		);

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_tomcat_contexts_pkey_seq')");
		try (PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"insert into\n"
			+ "  httpd_tomcat_contexts\n"
			+ "values (\n"
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
			+ ")"
		)) {
			try {
				pstmt.setInt(1, pkey);
				pstmt.setInt(2, tomcat_site);
				pstmt.setString(3, className);
				pstmt.setBoolean(4, cookies);
				pstmt.setBoolean(5, crossContext);
				pstmt.setString(6, docBase.toString());
				pstmt.setBoolean(7, override);
				pstmt.setString(8, path);
				pstmt.setBoolean(9, privileged);
				pstmt.setBoolean(10, reloadable);
				pstmt.setBoolean(11, useNaming);
				pstmt.setString(12, wrapperClass);
				pstmt.setInt(13, debug);
				pstmt.setString(14, ObjectUtils.toString(workDir));

				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);

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
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "addHttpdTomcatDataSource", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to add HttpdTomcatDataSource, HttpdSite disabled: "+tomcat_site);

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_tomcat_data_sources_pkey_seq')");
		try (PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"insert into\n"
			+ "  httpd_tomcat_data_sources\n"
			+ "values (\n"
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
			+ ")"
		)) {
			try {
				pstmt.setInt(1, pkey);
				pstmt.setInt(2, tomcat_context);
				pstmt.setString(3, name);
				pstmt.setString(4, driverClassName);
				pstmt.setString(5, url);
				pstmt.setString(6, username);
				pstmt.setString(7, password);
				pstmt.setInt(8, maxActive);
				pstmt.setInt(9, maxIdle);
				pstmt.setInt(10, maxWait);
				pstmt.setString(11, validationQuery);

				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

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
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "addHttpdTomcatParameter", tomcat_site);
		if(isHttpdSiteDisabled(conn, tomcat_site)) throw new SQLException("Unable to add HttpdTomcatParameter, HttpdSite disabled: "+tomcat_site);

		int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_tomcat_parameters_pkey_seq')");
		PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"insert into\n"
			+ "  httpd_tomcat_parameters\n"
			+ "values (\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ")"
		);
		try {
			pstmt.setInt(1, pkey);
			pstmt.setInt(2, tomcat_context);
			pstmt.setString(3, name);
			pstmt.setString(4, value);
			pstmt.setBoolean(5, override);
			pstmt.setString(6, description);

			pstmt.executeUpdate();
		} catch(SQLException err) {
			System.err.println("Error from update: "+pstmt.toString());
			throw err;
		} finally {
			pstmt.close();
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
			false
		);

		return pkey;
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
		UnixPath workDir
	) throws IOException, SQLException {
		boolean isSecure=conn.executeBooleanQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        hst.is_secure\n"
			+ "      from\n"
			+ "        httpd_tomcat_shared_sites htss,\n"
			+ "        httpd_shared_tomcats hst\n"
			+ "      where\n"
			+ "        htss.tomcat_site=?\n"
			+ "        and htss.httpd_shared_tomcat=hst.pkey\n"
			+ "    ), false\n"
			+ "  )",
			tomcat_site
		);
		if(!HttpdTomcatContext.isValidDocBase(docBase)) throw new SQLException("Invalid docBase: "+docBase);
		int aoServer=getAOServerForHttpdSite(conn, tomcat_site);

		// OperatingSystem settings
		int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
		UnixPath httpdSharedTomcatsDir = OperatingSystemVersion.getHttpdSharedTomcatsDirectory(osv);
		UnixPath httpdSitesDir = OperatingSystemVersion.getHttpdSitesDirectory(osv);

		String docBaseStr = docBase.toString();
		if(docBaseStr.startsWith("/home/")) {
			// Must be able to access one of the linux_server_accounts with that home directory
			//
			// This means there must be an accessible account that has a home directory that is a prefix of this docbase.
			// Such as /home/e/example/ being a prefix of /home/e/example/my-webapp
			IntList lsas = conn.executeIntListQuery(
				"select\n"
				+ "  pkey\n"
				+ "from\n"
				+ "  linux_server_accounts\n"
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
			int hs = conn.executeIntQuery("select pkey from httpd_sites where ao_server=? and site_name=?", aoServer, siteName);
			HttpdHandler.checkAccessHttpdSite(conn, source, "addCvsRepository", hs);
		} else if(docBaseStr.startsWith(httpdSharedTomcatsDir + "/")) {
			int slashPos = docBaseStr.indexOf('/', httpdSharedTomcatsDir.toString().length() + 1);
			if(slashPos == -1) slashPos = docBaseStr.length();
			String tomcatName = docBaseStr.substring(httpdSharedTomcatsDir.toString().length() + 1, slashPos);
			int groupLSA = conn.executeIntQuery("select linux_server_account from httpd_shared_tomcats where name=? and ao_server=?", tomcatName, aoServer);
			LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", groupLSA);
		} else {
			// Allow the example directories
			List<UnixPath> tomcats = conn.executeObjectListQuery(
				ObjectFactories.unixPathFactory,
				"select install_dir||'/webapps/examples' from httpd_tomcat_versions"
			);
			boolean found=false;
			for (UnixPath tomcat : tomcats) {
				if (docBase.equals(tomcat)) {
					found = true;
					break;
				}
			}
			if(!found) throw new SQLException("Invalid docBase: " + docBase);
		}

		if(!HttpdTomcatContext.isValidPath(path)) throw new SQLException("Invalid path: "+path);
		if(!HttpdTomcatContext.isValidWorkDir(workDir)) throw new SQLException("Invalid workDir: "+workDir);
		if(isSecure) {
			if(!ObjectUtils.equals(className, HttpdTomcatContext.DEFAULT_CLASS_NAME)) throw new SQLException("className not allowed for secure JVM: "+className);
			if(crossContext!=HttpdTomcatContext.DEFAULT_CROSS_CONTEXT) throw new SQLException("crossContext not allowed for secure JVM: "+crossContext);
			String siteName=getSiteNameForHttpdSite(conn, tomcat_site);
			if(!docBaseStr.startsWith(httpdSitesDir + "/" + siteName + "/")) throw new SQLException("docBase not allowed for secure JVM: " + docBase);
			if(override!=HttpdTomcatContext.DEFAULT_OVERRIDE) throw new SQLException("override not allowed for secure JVM: "+override);
			if(privileged!=HttpdTomcatContext.DEFAULT_PRIVILEGED) throw new SQLException("privileged not allowed for secure JVM: "+privileged);
			if(!ObjectUtils.equals(wrapperClass, HttpdTomcatContext.DEFAULT_WRAPPER_CLASS)) throw new SQLException("wrapperClass not allowed for secure JVM: "+wrapperClass);
			if(!ObjectUtils.equals(workDir, HttpdTomcatContext.DEFAULT_WORK_DIR)) throw new SQLException("workDir not allowed for secure JVM: "+workDir);
		}
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
		UnixPath contentSrc,
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
			contentSrc,
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
		UnixPath contentSrc,
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
			tomcatVersion=conn.executeIntQuery("select tomcat_version from httpd_jboss_versions where version=?", jBossVersion);
		} else if ("tomcat_shared".equals(siteType)) {
			// Get shared Tomcat pkey
			sharedTomcatPkey = conn.executeIntQuery(
				"select pkey from httpd_shared_tomcats where ao_server=? and name=?",
				aoServer,
				sharedTomcatName
			);

			// Check for ties between jvm and site in linux_group_accounts
			String sharedTomcatUsername = conn.executeStringQuery("select lsa.username from httpd_shared_tomcats hst, linux_server_accounts lsa where hst.linux_server_account = lsa.pkey and hst.pkey=?", sharedTomcatPkey);
			String sharedTomcatLinuxGroup = conn.executeStringQuery("select lsg.name from httpd_shared_tomcats hst, linux_server_groups lsg where hst.linux_server_group = lsg.pkey and hst.pkey=?", sharedTomcatPkey);
			boolean hasAccess = conn.executeBooleanQuery(
				"select (\n"
				+ "  select\n"
				+ "    pkey\n"
				+ "  from\n"
				+ "    linux_group_accounts\n"
				+ "  where\n"
				+ "    group_name=?\n"
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
			if (!hasAccess) throw new SQLException("Linux_account ("+username+") does not have access to linux_group ("+sharedTomcatLinuxGroup+")");
			hasAccess = conn.executeBooleanQuery(
				"select (\n"
				+ "  select\n"
				+ "    pkey\n"
				+ "  from\n"
				+ "    linux_group_accounts\n"
				+ "  where\n"
				+ "    group_name=?\n"
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
			if (!hasAccess) throw new SQLException("Linux_account ("+sharedTomcatName+") does not have access to linux_group ("+group+")");

			if(tomcatVersion!=-1) throw new SQLException("TomcatVersion cannot be supplied for a TomcatShared site: "+tomcatVersion);
			tomcatVersion = conn.executeIntQuery("select version from httpd_shared_tomcats where pkey=?", sharedTomcatPkey);
		}
		String tomcatVersionStr=conn.executeStringQuery("select version from technology_versions where pkey=?", tomcatVersion);
		boolean isTomcat4 =
			tomcatVersionStr.startsWith(HttpdTomcatVersion.VERSION_4_1_PREFIX)
			|| tomcatVersionStr.startsWith(HttpdTomcatVersion.VERSION_5_5_PREFIX)
			|| tomcatVersionStr.startsWith(HttpdTomcatVersion.VERSION_6_0_PREFIX)
			|| tomcatVersionStr.startsWith(HttpdTomcatVersion.VERSION_7_0_PREFIX)
			|| tomcatVersionStr.startsWith(HttpdTomcatVersion.VERSION_8_0_PREFIX)
		;
		if(ipAddress!=-1) {
			IPAddressHandler.checkAccessIPAddress(conn, source, methodName, ipAddress);
			// The IP must be on the provided server
			int ipServer=IPAddressHandler.getServerForIPAddress(conn, ipAddress);
			if(ipServer!=aoServer) throw new SQLException("IP address "+ipAddress+" is not hosted on AOServer #"+aoServer);
			if(isTomcat4) {
				Port[] ports;
				try {
					ports = new Port[] {
						Port.valueOf(80, com.aoindustries.net.Protocol.TCP),
						Port.valueOf(443, com.aoindustries.net.Protocol.TCP)
					};
				} catch(ValidationException e) {
					throw new AssertionError("Hard-coded ports should never fail", e);
				}
				for (Port port : ports) {
					// The IP must either be not attached to a httpd_server, or the server must support jk
					int nb = NetBindHandler.getNetBind(conn, aoServer, ipAddress, port);
					if(nb != -1) {
						int hs=conn.executeIntQuery(
							"select\n"
							+ "  coalesce(\n"
							+ "    (\n"
							+ "      select\n"
							+ "        httpd_server\n"
							+ "      from\n"
							+ "        httpd_binds\n"
							+ "      where\n"
							+ "        net_bind=?\n"
							+ "    ), -1\n"
							+ "  )",
							nb
						);
						if(
							hs!=-1
							&& conn.executeBooleanQuery(
								"select not is_mod_jk from httpd_servers where pkey=?",
								hs
							)
						) throw new SQLException("httpd_server does not support mod_jk: "+hs);
					}
				}
			}
		} else {
			ipAddress=IPAddressHandler.getSharedHttpdIP(conn, aoServer, isTomcat4);
			if(ipAddress==-1) throw new SQLException("Unable to find shared IP address for AOServer #"+aoServer);
		}
		if(phpVersion != -1) {
			// Version must be correct for this server
			int tvOsv = conn.executeIntQuery(
				"select coalesce(\n"
				+ "  (select operating_system_version from technology_versions where pkey=? and name=?),\n"
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
		int httpdSitePKey;

		List<DomainName> tlds = DNSHandler.getDNSTLDs(conn);
		DomainName testURL;
		try {
			testURL = DomainName.valueOf(siteName + "." + ServerHandler.getHostnameForAOServer(conn, aoServer));
		} catch(ValidationException e) {
			throw new SQLException(e);
		}
		DNSHandler.addDNSRecord(
			conn,
			invalidateList,
			testURL,
			IPAddressHandler.getInetAddressForIPAddress(conn, ipAddress),
			tlds
		);

		// Finish up the security checks with the Connection
		MasterServer.checkAccessHostname(conn, source, methodName, primaryHttpHostname.toString(), tlds);
		for(DomainName altHttpHostname : altHttpHostnames) {
			MasterServer.checkAccessHostname(conn, source, methodName, altHttpHostname.toString(), tlds);
		}

		// Create and/or get the HttpdBind info
		int httpNetBind=getHttpdBind(conn, invalidateList, packageName, aoServer, ipAddress, httpPort, Protocol.HTTP, isTomcat4);

		// Create the HttpdSite
		httpdSitePKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_sites_pkey_seq')");
		PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"insert into\n"
			+ "  httpd_sites\n"
			+ "values(\n"
			+ "  ?,\n" // pkey
			+ "  ?,\n" // ao_server
			+ "  ?,\n" // site_name
			+ "  false,\n" // list_first
			+ "  ?,\n" // package
			+ "  ?,\n" // linux_account
			+ "  ?,\n" // linux_group
			+ "  ?,\n" // server_admin
			+ "  ?,\n" // content_src
			+ "  null,\n" // disable_log
			+ "  false,\n" // is_manual
			+ "  null,\n" // awstats_skip_files
			+ "  ?,\n" // php_version
			+ "  ?,\n" // enable_cgi
			+ "  ?,\n" // enable_ssh
			+ "  ?,\n" // enable_htaccess
			+ "  ?,\n" // enable_indexes
			+ "  ?,\n" // enable_follow_symlinks
			+ "  false\n" // enable_anonymous_ftp
			+ ")");
		try {
			pstmt.setInt(1, httpdSitePKey);
			pstmt.setInt(2, aoServer);
			pstmt.setString(3, siteName);
			pstmt.setString(4, packageName.toString());
			pstmt.setString(5, username.toString());
			pstmt.setString(6, group.toString());
			pstmt.setString(7, serverAdmin.toString());
			pstmt.setString(8, ObjectUtils.toString(contentSrc));
			if(phpVersion == -1) pstmt.setNull(9, Types.INTEGER);
			else pstmt.setInt(9, phpVersion);
			pstmt.setBoolean(10, enableCgi);
			pstmt.setBoolean(11, enableSsi);
			pstmt.setBoolean(12, enableHtaccess);
			pstmt.setBoolean(13, enableIndexes);
			pstmt.setBoolean(14, enableFollowSymlinks);
			pstmt.executeUpdate();
		} catch(SQLException err) {
			System.err.println("Error from query: "+pstmt.toString());
			throw err;
		} finally {
			pstmt.close();
		}
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITES, accounting, aoServer, false);

		// Create the HttpdTomcatSite
		conn.executeUpdate(
			"insert into httpd_tomcat_sites values(?,?,?)",
			httpdSitePKey,
			tomcatVersion,
			useApache
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
		int htcPKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_tomcat_contexts_pkey_seq')");
		conn.executeUpdate(
			"insert into\n"
			+ "  httpd_tomcat_contexts\n"
			+ "values (\n"
			+ "  ?,\n"
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
			+ "  "+HttpdTomcatContext.DEFAULT_WORK_DIR+"\n"
			+ ")",
			htcPKey,
			httpdSitePKey,
			docBase
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS, accounting, aoServer, false);

		if(!isTomcat4) {
			htcPKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_tomcat_contexts_pkey_seq')");
			conn.executeUpdate(
				"insert into\n"
				+ "  httpd_tomcat_contexts\n"
				+ "values (\n"
				+ "  ?,\n"
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
				+ "  "+HttpdTomcatContext.DEFAULT_WORK_DIR+"\n"
				+ ")",
				htcPKey,
				httpdSitePKey,
				conn.executeStringQuery("select install_dir from httpd_tomcat_versions where version=?", tomcatVersion)+"/webapps/examples"
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
			pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into httpd_jboss_sites values(?,?,?,?,?,?,?)");
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
				"insert into httpd_tomcat_shared_sites values(?,?)",
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
					"insert into httpd_tomcat_std_sites values(?,?,?,?,true,true)",
					httpdSitePKey,
					shutdownPort,
					new Identifier(MasterServer.getRandom()).toString(),
					HttpdTomcatStdSite.DEFAULT_MAX_POST_SIZE
				);
			} else {
				conn.executeUpdate(
					"insert into httpd_tomcat_std_sites values(?,null,null,?,true,true)",
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
			int httpdWorkerPKey=addHttpdWorker(
				conn,
				invalidateList,
				netBindPKey,
				httpdSitePKey
			);
		}

		// Create the HTTP HttpdSiteBind
		int httpSiteBindPKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_site_binds_pkey_seq')");
		String siteLogsDir = OperatingSystemVersion.getHttpdSiteLogsDirectory(osv).toString();
		conn.executeUpdate(
			"insert into httpd_site_binds values(?,?,?,?,?,null,null,null,null,false,true)",
			httpSiteBindPKey,
			httpdSitePKey,
			httpNetBind,
			siteLogsDir + '/' + siteName + "/http/access_log",
			siteLogsDir + '/' + siteName + "/http/error_log"
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BINDS, accounting, aoServer, false);

		conn.executeUpdate(
			"insert into httpd_site_urls(httpd_site_bind, hostname, is_primary) values(?,?,true)",
			httpSiteBindPKey,
			primaryHttpHostname
		);
		for (DomainName altHttpHostname : altHttpHostnames) {
			conn.executeUpdate(
				"insert into httpd_site_urls(httpd_site_bind, hostname, is_primary) values(?,?,false)",
				httpSiteBindPKey,
				altHttpHostname
			);
		}
		conn.executeUpdate(
			"insert into httpd_site_urls(httpd_site_bind, hostname, is_primary) values(?,?,false)",
			httpSiteBindPKey,
			testURL
		);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_URLS, accounting, aoServer, false);

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
		boolean isSecure,
		boolean isOverflow,
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
		String versionStr=conn.executeStringQuery("select version from technology_versions where pkey=?", version);
		boolean isTomcat4 =
			versionStr.startsWith(HttpdTomcatVersion.VERSION_4_1_PREFIX)
			|| versionStr.startsWith(HttpdTomcatVersion.VERSION_5_5_PREFIX)
			|| versionStr.startsWith(HttpdTomcatVersion.VERSION_6_0_PREFIX)
			|| versionStr.startsWith(HttpdTomcatVersion.VERSION_7_0_PREFIX)
			|| versionStr.startsWith(HttpdTomcatVersion.VERSION_8_0_PREFIX)
		;

		int pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('httpd_shared_tomcats_pkey_seq')");
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
			int httpdWorkerPKey=addHttpdWorker(
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

			conn.executeUpdate(
				"insert into\n"
				+ "  httpd_shared_tomcats\n"
				+ "values(\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  null,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  false,\n" // is_manual
				+ "  ?,\n" // max_post_size
				+ "  true,\n" // unpack_wars
				+ "  true\n" // auto_deploy
				+ ")",
				pkey,
				name,
				aoServer,
				version,
				lsaPkey,
				lsgPkey,
				isSecure,
				isOverflow,
				httpdWorkerPKey,
				shutdownBindPKey,
				new Identifier(MasterServer.getRandom()).toString(),
				HttpdSharedTomcat.DEFAULT_MAX_POST_SIZE
			);
		} else {
			conn.executeUpdate(
				"insert into\n"
				+ "  httpd_shared_tomcats\n"
				+ "values(\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  null,\n"
				+ "  null,\n"
				+ "  null,\n"
				+ "  null,\n"
				+ "  false,\n" // is_manual
				+ "  ?,\n" // max_post_size
				+ "  true,\n" // unpack_wars
				+ "  true\n" // auto_deploy
				+ ")",
				pkey,
				name,
				aoServer,
				version,
				lsaPkey,
				lsgPkey,
				isSecure,
				isOverflow,
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
		int version,
		UnixPath contentSrc,
		int phpVersion,
		boolean enableCgi,
		boolean enableSsi,
		boolean enableHtaccess,
		boolean enableIndexes,
		boolean enableFollowSymlinks
	) throws IOException, SQLException {
		if(sharedTomcatName==null) {
			throw new SQLException("Fatal: No shared Tomcat specified.");
			/*
			if(version==-1) throw new SQLException("One of tomcat_name or tomcat_version must be requested.");
			boolean found=false;
			String[] okVersions=HttpdSharedTomcat.getOverflowVersions();
			for(int c=0;c<okVersions.length;c++) {
				if(
					conn.executeIntQuery(
						Connection.TRANSACTION_READ_COMMITTED,
						true,
						"select\n"
						+ "  pkey\n"
						+ "from\n"
						+ "  technology_versions\n"
						+ "where\n"
						+ "  name='"+HttpdTomcatVersion.TECHNOLOGY_NAME+"'\n"
						+ "  and version=?\n"
			+ "  and operating_system_version=(select operating_system_version from servers where pkey=?)",
						okVersions[c],
			aoServer
					)==version
				) {
					found=true;
					break;
				}
			}
			if(!found) throw new SQLException("Secured Shared JVM now allowed for tomcat_version: "+version+", aoServer="+aoServer+", okVersions={"+StringUtility.buildList(okVersions)+"}");

			// Find or allocate a shared Tomcat for use with this site

			// Find a JVM that does not already have the maximum number of sites
			int sharedTomcatPKey = conn.executeIntQuery(
				Connection.TRANSACTION_READ_COMMITTED,
				true,
				"select\n"
				+ "  coalesce(\n"
				+ "    (\n"
				+ "      select\n"
				+ "        hst.pkey\n"
				+ "      from\n"
				+ "        httpd_shared_tomcats hst\n"
				+ "      where\n"
				+ "        hst.is_overflow\n"
				+ "        and hst.ao_server=?\n"
				+ "        and hst.version=?\n"
				+ "        and (\n"
				+ "          select\n"
				+ "            count(*)\n"
				+ "          from\n"
				+ "            httpd_tomcat_shared_sites htss\n"
				+ "          where\n"
				+ "            hst.pkey=htss.httpd_shared_tomcat\n"
				+ "        )<"+HttpdSharedTomcat.MAX_SITES+"\n"
				+ "      order by\n"
				+ "        hst.name\n"
				+ "      limit 1\n"
				+ "    ),\n"
				+ "    -1\n"
				+ "  )",
				aoServer,
				version
			);

			String sharedTomcatUsername;
			String sharedTomcatGroup;

			if (sharedTomcatPKey!=-1) {
				// Get the JVM name
				sharedTomcatName=conn.executeStringQuery(
					Connection.TRANSACTION_READ_COMMITTED,
					true,
					"select name from httpd_shared_tomcats where pkey=?",
					sharedTomcatPKey
				);

				// Available JVM found, use it
				sharedTomcatUsername=conn.executeStringQuery(
					Connection.TRANSACTION_READ_COMMITTED,
					true,
					  "select\n"
					+ "  lsa.username\n"
					+ "from\n"
					+ "  httpd_shared_tomcats hst,\n"
					+ "  linux_server_accounts lsa\n"
					+ "where\n"
					+ "  hst.pkey=?\n"
					+ "  and hst.linux_server_account=lsa.pkey",
					sharedTomcatPKey
				);
				sharedTomcatGroup=conn.executeStringQuery(
					Connection.TRANSACTION_READ_COMMITTED,
					true,
					  "select\n"
					+ "  lsg.name\n"
					+ "from\n"
					+ "  httpd_shared_tomcats hst,\n"
					+ "  linux_server_groups lsg\n"
					+ "where\n"
					+ "  hst.pkey=?\n"
					+ "  and hst.linux_server_group=lsg.pkey",
					sharedTomcatPKey
				);
			} else {
				// create a new overflow JVM
				sharedTomcatName = generateSharedTomcatName(conn, HttpdSharedTomcat.OVERFLOW_TEMPLATE);

				// Add the username
				UsernameHandler.addUsername(
					conn,
					source,
					invalidateList,
					BusinessHandler.getRootBusiness(),
					sharedTomcatName,
					true
				);

				// Add the Linux group
				LinuxAccountHandler.addLinuxGroup(
					conn,
					source,
					invalidateList,
					sharedTomcatName,
					BusinessHandler.getRootBusiness(),
					LinuxGroupType.APPLICATION,
					true
				);

				// Add the Linux account
				LinuxAccountHandler.addLinuxAccount(
					conn,
					source,
					invalidateList,
					sharedTomcatName,
					sharedTomcatName,
					sharedTomcatName,
					null,
					null,
					null,
					LinuxAccountType.APPLICATION,
					Shell.BASH,
					true
				);

				// Add the Linux server group
				int sharedTomcatGroupPKey=LinuxAccountHandler.addLinuxServerGroup(
					conn,
					source,
					invalidateList,
					sharedTomcatName,
					aoServer,
					true
				);

				// Add the Linux server account
				int sharedTomcatAccountPKey=LinuxAccountHandler.addLinuxServerAccount(
					conn,
					source,
					invalidateList,
					sharedTomcatName,
					aoServer,
					HttpdSharedTomcat.WWW_GROUP_DIR+'/'+sharedTomcatName,
					true
				);

				// Add the new overflow JVM
				addHttpdSharedTomcat(
					conn,
					source,
					invalidateList,
					sharedTomcatName,
					aoServer,
					version,
					sharedTomcatName,
					sharedTomcatName,
					true,
					true,
					true
				);
			}

			// Grant group permissions if they do not already exist
			if(
				conn.executeBooleanQuery(
					Connection.TRANSACTION_READ_COMMITTED,
					true,
					"select (select pkey from linux_group_accounts where group_name=? and username=?) is null",
					operating_system_version, too, if this code is resurrected
					group,
					sharedTomcatName
				)
			) LinuxAccountHandler.addLinuxGroupAccount(
				conn,
				source,
				invalidateList,
				group,
				sharedTomcatName,
				false,
				true
			);

			if(
				conn.executeBooleanQuery(
					Connection.TRANSACTION_READ_COMMITTED,
					true,
					"select (select pkey from linux_group_accounts where group_name=? and username=?) is null",
					operating_system_version, too, if this code is resurrected
					sharedTomcatName,
					username
				)
			) LinuxAccountHandler.addLinuxGroupAccount(
				conn,
				source,
				invalidateList,
				sharedTomcatName,
				username,
				false,
				true
			);
			*/
		} else {
			if(version!=-1) throw new SQLException("Only one of tomcat_name or tomcat_version may be requested.");
			version=conn.executeIntQuery("select version from httpd_shared_tomcats where name=? and ao_server=?", sharedTomcatName, aoServer);
		}
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
			contentSrc,
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
		UnixPath contentSrc,
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
			contentSrc,
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
			"update httpd_shared_tomcats set disable_log=? where pkey=?",
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
			"update httpd_sites set disable_log=? where pkey=?",
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
		int httpdSite=conn.executeIntQuery("select httpd_site from httpd_site_binds where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "disableHttpdSiteBind", httpdSite);

		conn.executeUpdate(
			"update httpd_site_binds set disable_log=? where pkey=?",
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
			"update httpd_shared_tomcats set disable_log=null where pkey=?",
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
			"update httpd_sites set disable_log=null where pkey=?",
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
			"update httpd_site_binds set disable_log=null where pkey=?",
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
		List<String> names=conn.executeStringListQuery("select name from httpd_shared_tomcats group by name");
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
					// Must also not be found in linux_server_accounts.home
					conn.executeIntQuery(
						"select\n"
						+ "  count(*)\n"
						+ "from\n"
						+ "  linux_server_accounts\n"
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
					// Must also not be found in usernames.username
					&& conn.executeIntQuery("select count(*) from usernames where username=?", name) == 0
					// Must also not be found in linux_groups.name
					&& conn.executeIntQuery("select count(*) from linux_groups where name=?", name) == 0
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
		List<String> names=conn.executeStringListQuery("select site_name from httpd_sites group by site_name");
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
				// Must also not be found in linux_server_accounts.home on CentOS 5 or CentOS 7
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
					+ "  linux_server_accounts\n"
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
		return conn.executeIntQuery("select coalesce(disable_log, -1) from httpd_shared_tomcats where pkey=?", pkey);
	}

	public static int getDisableLogForHttpdSite(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from httpd_sites where pkey=?", pkey);
	}

	public static int getDisableLogForHttpdSiteBind(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from httpd_site_binds where pkey=?", pkey);
	}

	public static IntList getHttpdSiteBindsForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery("select pkey from httpd_site_binds where httpd_site=?", pkey);
	}

	public static int getHttpdSiteForHttpdSiteURL(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  hsb.httpd_site\n"
			+ "from\n"
			+ "  httpd_site_urls hsu,\n"
			+ "  httpd_site_binds hsb\n"
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
		return conn.executeIntListQuery("select pkey from httpd_shared_tomcats where linux_server_account=?", pkey);
	}

	public static IntList getHttpdSharedTomcatsForPackage(
		DatabaseConnection conn,
		AccountingCode name
	) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  hst.pkey\n"
			+ "from\n"
			+ "  linux_groups lg,\n"
			+ "  linux_server_groups lsg,\n"
			+ "  httpd_shared_tomcats hst\n"
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
		return conn.executeIntListQuery("select pkey from httpd_sites where package=?", name);
	}

	public static IntList getHttpdSitesForLinuxServerAccount(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntListQuery(
			"select\n"
			+ "  hs.pkey\n"
			+ "from\n"
			+ "  linux_server_accounts lsa,\n"
			+ "  httpd_sites hs\n"
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
			+ "  httpd_shared_tomcats hst,\n"
			+ "  linux_server_groups lsg,\n"
			+ "  linux_groups lg,\n"
			+ "  packages pk\n"
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
			+ "  httpd_sites hs,\n"
			+ "  packages pk\n"
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
			+ "  httpd_servers hs,\n"
			+ "  packages pk\n"
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
		return conn.executeIntQuery("select httpd_site from httpd_site_binds where pkey=?", pkey);
	}

	public static int getLinuxServerAccountForHttpdSharedTomcat(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery("select linux_server_account from httpd_shared_tomcats where pkey=?", pkey);
	}

	public static int getLinuxServerAccountForHttpdSite(
		DatabaseConnection conn,
		int pkey
	) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  lsa.pkey\n"
			+ "from\n"
			+ "  httpd_sites hs,\n"
			+ "  linux_server_accounts lsa\n"
			+ "where\n"
			+ "  hs.pkey=?\n"
			+ "  and hs.linux_account=lsa.username\n"
			+ "  and hs.ao_server=lsa.ao_server",
			pkey
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
			+ "  httpd_shared_tomcats hst,\n"
			+ "  linux_server_groups lsg,\n"
			+ "  linux_groups lg\n"
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
			"select package from httpd_sites where pkey=?",
			pkey
		);
	}

	public static int getAOServerForHttpdSharedTomcat(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from httpd_shared_tomcats where pkey=?", pkey);
	}

	public static int getAOServerForHttpdSite(DatabaseConnection conn, int httpdSite) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from httpd_sites where pkey=?", httpdSite);
	}

	public static int getAOServerForHttpdServer(DatabaseConnection conn, int httpdServer) throws IOException, SQLException {
		return conn.executeIntQuery("select ao_server from httpd_servers where pkey=?", httpdServer);
	}

	public static String getSiteNameForHttpdSite(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeStringQuery("select site_name from httpd_sites where pkey=?", pkey);
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
		return conn.executeBooleanQuery("select (select pkey from httpd_shared_tomcats where name=? limit 1) is null", name);
	}

	public static boolean isSiteNameAvailable(DatabaseConnection conn, String siteName) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select pkey from httpd_sites where site_name=? limit 1) is null", siteName);
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
		if(conn.executeBooleanQuery("select (select tomcat_site from httpd_tomcat_shared_sites where tomcat_site=?) is not null", tomcat_site)) {
			int lsa=conn.executeIntQuery(
				"select\n"
				+ "  hst.linux_server_account\n"
				+ "from\n"
				+ "  httpd_tomcat_shared_sites htss,\n"
				+ "  httpd_shared_tomcats hst\n"
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
		String protocol,
		boolean isTomcat4
	) throws IOException, SQLException {
		// First, find the net_bind
		int netBind;
		PreparedStatement pstmt = conn.getConnection(
			Connection.TRANSACTION_READ_COMMITTED,
			true
		).prepareStatement("select pkey, app_protocol from net_binds where server=? and ip_address=? and port=? and net_protocol=?");
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
						"Protocol mismatch on net_binds(pkey="
						+netBind
						+" ao_server="
						+aoServer
						+" ip_address="
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
			netBind = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('net_binds_pkey_seq')");
			pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into net_binds values(?,?,?,?,?,?,?,true)");
			try {
				pstmt.setInt(1, netBind);
				pstmt.setString(2, packageName.toString());
				pstmt.setInt(3, aoServer);
				pstmt.setInt(4, ipAddress);
				pstmt.setInt(5, httpPort.getPort());
				pstmt.setString(6, httpPort.getProtocol().name().toLowerCase(Locale.ROOT));
				pstmt.setString(7, protocol);
				pstmt.executeUpdate();
			} finally {
				pstmt.close();
			}
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
				"insert into net_bind_firewalld_zones (net_bind, firewalld_zone) values (\n"
				+ "  ?,\n"
				+ "  (select pkey from firewalld_zones where server=? and \"name\"=?)\n"
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
				+ "      httpd_binds\n"
				+ "    where\n"
				+ "      net_bind=?\n"
				+ "    limit 1\n"
				+ "  ) is null",
				netBind
			)
		) {
			// Get the list of httpd_servers and how many httpd_site_binds there are
			int lowestPKey=-1;
			int lowestCount=Integer.MAX_VALUE;
			pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
				"select\n"
				+ "  hs.pkey,\n"
				+ "  (\n"
				+ "    select\n"
				+ "      count(*)\n"
				+ "    from\n"
				+ "      httpd_binds hb,\n"
				+ "      httpd_site_binds hsb\n"
				+ "    where\n"
				+ "      hs.pkey=hb.httpd_server\n"
				+ "      and hb.net_bind=hsb.httpd_bind\n"
				+ "  )\n"
				+ "from\n"
				+ "  httpd_servers hs\n"
				+ "where\n"
				+ "  hs.can_add_sites\n"
				+ "  and (\n"
				+ "    hs.is_mod_jk\n"
				+ "    or hs.is_mod_jk=?\n"
				+ "  ) and hs.ao_server=?\n"
				+ "  and (\n"
				+ "    hs.is_shared\n"
				+ "    or (\n"
				+ "      is_business_or_parent(\n"
				+ "        (select pk1.accounting from packages pk1 where hs.package=pk1.pkey),\n"
				+ "        (select accounting from packages where name=?)\n"
				+ "      )\n"
				+ "    )\n"
				+ "  )\n"
				+ "  and (\n"
				+ "    select\n"
				+ "      count(*)\n"
				+ "    from\n"
				+ "      httpd_binds hb,\n"
				+ "      httpd_site_binds hsb\n"
				+ "    where\n"
				+ "      hs.pkey=hb.httpd_server\n"
				+ "      and hb.net_bind=hsb.httpd_bind\n"
				+ "  )<hs.max_binds"
			);
			try {
				pstmt.setBoolean(1, isTomcat4);
				pstmt.setInt(2, aoServer);
				pstmt.setString(3, packageName.toString());
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
				"insert into httpd_binds values(?,?)",
				netBind,
				lowestPKey
			);
		} else {
			if(
				conn.executeBooleanQuery(
					"select\n"
					+ "  (\n"
					+ "    select\n"
					+ "      count(*)\n"
					+ "    from\n"
					+ "      httpd_binds hb1,\n"
					+ "      httpd_binds hb2,\n"
					+ "      httpd_site_binds hsb\n"
					+ "    where\n"
					+ "      hb1.net_bind=?\n"
					+ "      and hb1.httpd_server=hb2.httpd_server\n"
					+ "      and hb2.net_bind=hsb.httpd_bind\n"
					+ "  )>=(\n"
					+ "    select\n"
					+ "      hs.max_binds\n"
					+ "    from\n"
					+ "      httpd_binds hb,\n"
					+ "      httpd_servers hs\n"
					+ "    where\n"
					+ "      hb.net_bind=?\n"
					+ "      and hb.httpd_server=hs.pkey\n"
					+ "  )",
					netBind,
					netBind
				)
			) throw new SQLException("HttpdServer has reached its maximum number of HttpdSiteBinds: "+ipAddress+":"+httpPort+" on "+aoServer);
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

		int tomcat4Worker=conn.executeIntQuery("select coalesce(tomcat4_worker, -1) from httpd_shared_tomcats where pkey=?", pkey);
		int tomcat4ShutdownPort=conn.executeIntQuery("select coalesce(tomcat4_shutdown_port, -1) from httpd_shared_tomcats where pkey=?", pkey);

		conn.executeUpdate("delete from httpd_shared_tomcats where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SHARED_TOMCATS,
			accounting,
			aoServer,
			false
		);

		if(tomcat4Worker!=-1) {
			int nb=conn.executeIntQuery("select net_bind from httpd_workers where pkey=?", tomcat4Worker);
			conn.executeUpdate("delete from httpd_workers where pkey=?", tomcat4Worker);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_WORKERS, accounting, aoServer, false);

			conn.executeUpdate("delete from net_binds where pkey=?", nb);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BINDS, accounting, aoServer, false);
			invalidateList.addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, accounting, aoServer, false);
		}

		if(tomcat4ShutdownPort!=-1) {
			conn.executeUpdate("delete from net_binds where pkey=?", tomcat4ShutdownPort);
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

		/**
		 * httpd_sites
		 *           + httpd_site_binds
		 *           |                + httpd_site_urls
		 *           |                |               + dns_records
		 *           |                + httpd_binds
		 *           |                            + net_binds
		 *           + httpd_tomcat_sites
		 *           |                  + httpd_tomcat_contexts
		 *           |                                        + httpd_tomcat_data_sources
		 *           |                                        + httpd_tomcat_parameters
		 *           |                  + httpd_workers
		 *           |                  |             + net_binds
		 *           |                  + httpd_tomcat_shared_sites
		 *           |                  |             + linux_group_accounts
		 *           |                  + httpd_tomcat_std_sites
		 *           |                                         + net_binds
		 *           |                  + httpd_jboss_sites
		 *           |                                   + net_binds
		 *           + httpd_static_sites
		 */
	public static void removeHttpdSite(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int httpdSitePKey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForHttpdSite(conn, httpdSitePKey);
		int aoServer=getAOServerForHttpdSite(conn, httpdSitePKey);
		String siteName=conn.executeStringQuery("select site_name from httpd_sites where pkey=?", httpdSitePKey);

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
			+ "  linux_server_accounts lsa\n"
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

		// httpd_site_authenticated_locations
		if(conn.executeUpdate("delete from httpd_site_authenticated_locations where httpd_site=?", httpdSitePKey)>0) {
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_AUTHENTICATED_LOCATIONS, accounting, aoServer, false);
		}

		// httpd_site_binds
		IntList httpdSiteBinds=conn.executeIntListQuery("select pkey from httpd_site_binds where httpd_site=?", httpdSitePKey);
		if(httpdSiteBinds.size()>0) {
			List<DomainName> tlds=DNSHandler.getDNSTLDs(conn);
			SortedIntArrayList httpdBinds=new SortedIntArrayList();
			for(int c=0;c<httpdSiteBinds.size();c++) {
				int httpdSiteBind=httpdSiteBinds.getInt(c);

				// httpd_site_urls
				IntList httpdSiteURLs=conn.executeIntListQuery("select pkey from httpd_site_urls where httpd_site_bind=?", httpdSiteBind);
				for(int d=0;d<httpdSiteURLs.size();d++) {
					int httpdSiteURL=httpdSiteURLs.getInt(d);

					// dns_records
					DomainName hostname = conn.executeObjectQuery(
						ObjectFactories.domainNameFactory,
						"select hostname from httpd_site_urls where pkey=?",
						httpdSiteURL
					);
					conn.executeUpdate("delete from httpd_site_urls where pkey=?", httpdSiteURL);
					invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_URLS, accounting, aoServer, false);
					DNSHandler.removeUnusedDNSRecord(conn, invalidateList, hostname, tlds);
				}

				int hb=conn.executeIntQuery("select httpd_bind from httpd_site_binds where pkey=?", httpdSiteBind);
				if(!httpdBinds.contains(hb)) httpdBinds.add(hb);
			}
			conn.executeUpdate("delete from httpd_site_binds where httpd_site=?", httpdSitePKey);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITE_BINDS, accounting, aoServer, false);

			for(int c=0;c<httpdBinds.size();c++) {
				int httpdBind=httpdBinds.getInt(c);
				if(
					conn.executeBooleanQuery(
						"select\n"
						+ "  (\n"
						+ "    select\n"
						+ "      pkey\n"
						+ "    from\n"
						+ "      httpd_site_binds\n"
						+ "    where\n"
						+ "      httpd_bind=?\n"
						+ "    limit 1\n"
						+ "  ) is null",
						httpdBind
					)
				) {
					// Do not clear up the httpd_binds for overflow IPs
					if(
						conn.executeBooleanQuery(
							"select\n"
							+ "  not ia.is_overflow\n"
							+ "from\n"
							+ "  net_binds nb,\n"
							+ "  ip_addresses ia\n"
							+ "where\n"
							+ "  nb.pkey=?\n"
							+ "  and nb.ip_address=ia.pkey",
							httpdBind
						)
					) {
						conn.executeUpdate("delete from httpd_binds where net_bind=?", httpdBind);
						conn.executeUpdate("delete from net_binds where pkey=?", httpdBind);
					}
				}
			}
		}

		// httpd_tomcat_sites
		if(conn.executeBooleanQuery("select (select httpd_site from httpd_tomcat_sites where httpd_site=? limit 1) is not null", httpdSitePKey)) {
			// httpd_tomcat_data_sources
			IntList htdss=conn.executeIntListQuery("select htds.pkey from httpd_tomcat_contexts htc, httpd_tomcat_data_sources htds where htc.tomcat_site=? and htc.pkey=htds.tomcat_context", httpdSitePKey);
			if(htdss.size()>0) {
				for(int c=0;c<htdss.size();c++) {
					conn.executeUpdate("delete from httpd_tomcat_data_sources where pkey=?", htdss.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES, accounting, aoServer, false);
			}

			// httpd_tomcat_parameters
			IntList htps=conn.executeIntListQuery("select htp.pkey from httpd_tomcat_contexts htc, httpd_tomcat_parameters htp where htc.tomcat_site=? and htc.pkey=htp.tomcat_context", httpdSitePKey);
			if(htps.size()>0) {
				for(int c=0;c<htps.size();c++) {
					conn.executeUpdate("delete from httpd_tomcat_parameters where pkey=?", htps.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS, accounting, aoServer, false);
			}

			// httpd_tomcat_contexts
			IntList htcs=conn.executeIntListQuery("select pkey from httpd_tomcat_contexts where tomcat_site=?", httpdSitePKey);
			if(htcs.size()>0) {
				for(int c=0;c<htcs.size();c++) {
					conn.executeUpdate("delete from httpd_tomcat_contexts where pkey=?", htcs.getInt(c));
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS, accounting, aoServer, false);
			}

			// httpd_workers
			IntList httpdWorkers=conn.executeIntListQuery("select pkey from httpd_workers where tomcat_site=?", httpdSitePKey);
			if(httpdWorkers.size()>0) {
				for(int c=0;c<httpdWorkers.size();c++) {
					int httpdWorker=httpdWorkers.getInt(c);
					int netBind=conn.executeIntQuery("select net_bind from httpd_workers where pkey=?", httpdWorker);
					conn.executeUpdate("delete from httpd_workers where pkey=?", httpdWorker);
					NetBindHandler.removeNetBind(conn, invalidateList, netBind);
				}
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_WORKERS, accounting, aoServer, false);
			}

			// httpd_tomcat_shared_sites
			if(conn.executeBooleanQuery("select (select tomcat_site from httpd_tomcat_shared_sites where tomcat_site=? limit 1) is not null", httpdSitePKey)) {
				// linux_group_accounts
				int httpdSharedTomcat=conn.executeIntQuery("select httpd_shared_tomcat from httpd_tomcat_shared_sites where tomcat_site=?", httpdSitePKey);
				if(conn.executeBooleanQuery("select is_overflow from httpd_shared_tomcats where pkey=?", httpdSharedTomcat)) {
					// Only remove group ties if the shared tomcat is an overflow type
					UserId hsUsername = conn.executeObjectQuery(
						ObjectFactories.userIdFactory,
						"select linux_account from httpd_sites where pkey=?",
						httpdSitePKey
					);
					GroupId hsGroup = conn.executeObjectQuery(
						ObjectFactories.groupIdFactory,
						"select linux_group from httpd_sites where pkey=?",
						httpdSitePKey
					);
					UserId hstUsername = conn.executeObjectQuery(
						ObjectFactories.userIdFactory,
						"select lsa.username from httpd_shared_tomcats hst, linux_server_accounts lsa where hst.pkey=? and hst.linux_server_account=lsa.pkey",
						httpdSharedTomcat
					);
					GroupId hstGroup = conn.executeObjectQuery(
						ObjectFactories.groupIdFactory,
						"select lsg.name from httpd_shared_tomcats hst, linux_server_groups lsg where hst.pkey=? and hst.linux_server_group=lsg.pkey",
						httpdSharedTomcat
					);

					conn.executeUpdate("delete from httpd_tomcat_shared_sites where tomcat_site=?", httpdSitePKey);

					LinuxAccountHandler.removeUnusedAlternateLinuxGroupAccount(conn, invalidateList, hsGroup, hstUsername);
					LinuxAccountHandler.removeUnusedAlternateLinuxGroupAccount(conn, invalidateList, hstGroup, hsUsername);
				}

				conn.executeUpdate("delete from httpd_tomcat_shared_sites where tomcat_site=?", httpdSitePKey);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SHARED_SITES, accounting, aoServer, false);
			}

			// httpd_tomcat_std_sites
			if(conn.executeBooleanQuery("select (select tomcat_site from httpd_tomcat_std_sites where tomcat_site=? limit 1) is not null", httpdSitePKey)) {
				int tomcat4ShutdownPort=conn.executeIntQuery("select coalesce(tomcat4_shutdown_port, -1) from httpd_tomcat_std_sites where tomcat_site=?", httpdSitePKey);

				conn.executeUpdate("delete from httpd_tomcat_std_sites where tomcat_site=?", httpdSitePKey);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_STD_SITES, accounting, aoServer, false);

				if(tomcat4ShutdownPort!=-1) {
					conn.executeUpdate("delete from net_binds where pkey=?", tomcat4ShutdownPort);
					invalidateList.addTable(conn, SchemaTable.TableID.NET_BINDS, accounting, aoServer, false);
					invalidateList.addTable(conn, SchemaTable.TableID.NET_BIND_FIREWALLD_ZONES, accounting, aoServer, false);
				}
			}

			// httpd_jboss_sites
			if(conn.executeBooleanQuery("select (select tomcat_site from httpd_jboss_sites where tomcat_site=? limit 1) is not null", httpdSitePKey)) {
				// net_binds
				int jnp_bind=conn.executeIntQuery("select jnp_bind from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);
				int webserver_bind=conn.executeIntQuery("select webserver_bind from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);
				int rmi_bind=conn.executeIntQuery("select rmi_bind from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);
				int hypersonic_bind=conn.executeIntQuery("select hypersonic_bind from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);
				int jmx_bind=conn.executeIntQuery("select jmx_bind from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);

				conn.executeUpdate("delete from httpd_jboss_sites where tomcat_site=?", httpdSitePKey);
				invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_JBOSS_SITES, accounting, aoServer, false);
				NetBindHandler.removeNetBind(conn, invalidateList, jnp_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, webserver_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, rmi_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, hypersonic_bind);
				NetBindHandler.removeNetBind(conn, invalidateList, jmx_bind);
			}

			conn.executeUpdate("delete from httpd_tomcat_sites where httpd_site=?", httpdSitePKey);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_TOMCAT_SITES, accounting, aoServer, false);
		}

		// httpd_static_sites
		if(conn.executeBooleanQuery("select (select httpd_site from httpd_static_sites where httpd_site=? limit 1) is not null", httpdSitePKey)) {
			conn.executeUpdate("delete from httpd_static_sites where httpd_site=?", httpdSitePKey);
			invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_STATIC_SITES, accounting, aoServer, false);
		}

		// httpd_sites
		conn.executeUpdate("delete from httpd_sites where pkey=?", httpdSitePKey);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SITES, accounting, aoServer, false);
	}

	public static void removeHttpdServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		AccountingCode accounting = getBusinessForHttpdServer(conn, pkey);
		int aoServer = getAOServerForHttpdServer(conn, pkey);

		// httpd_sites
		conn.executeUpdate("delete from httpd_servers where pkey=?", pkey);
		invalidateList.addTable(conn, SchemaTable.TableID.HTTPD_SERVERS, accounting, aoServer, false);
	}

	public static void removeHttpdSiteAuthenticatedLocation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int httpd_site=conn.executeIntQuery("select httpd_site from httpd_site_authenticated_locations where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdSiteAuthenticatedLocation", httpd_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, httpd_site);
		int aoServer=getAOServerForHttpdSite(conn, httpd_site);

		conn.executeUpdate("delete from httpd_site_authenticated_locations where pkey=?", pkey);
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
		if(conn.executeBooleanQuery("select is_primary from httpd_site_urls where pkey=?", pkey)) throw new SQLException("Not allowed to remove the primary hostname: "+pkey);
		if(
			conn.executeBooleanQuery(
				"select\n"
				+ "  (\n"
				+ "    select hostname from httpd_site_urls where pkey=?\n"
				+ "  )=(\n"
				+ "    select hs.site_name||'.'||ao.hostname from httpd_sites hs, ao_servers ao where hs.pkey=? and hs.ao_server=ao.server\n"
				+ "  )",
				pkey,
				hs
			)
		) throw new SQLException("Not allowed to remove a test URL: "+pkey);

		conn.executeUpdate("delete from httpd_site_urls where pkey=?", pkey);
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
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatContext", tomcat_site);
		String path=conn.executeStringQuery("select path from httpd_tomcat_contexts where pkey=?", pkey);
		if(path.length()==0) throw new SQLException("Not allowed to remove the default context: "+pkey);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		if(conn.executeUpdate("delete from httpd_tomcat_data_sources where tomcat_context=?", pkey)>0) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_DATA_SOURCES,
				accounting,
				aoServer,
				false
			);
		}

		if(conn.executeUpdate("delete from httpd_tomcat_parameters where tomcat_context=?", pkey)>0) {
			invalidateList.addTable(
				conn,
				SchemaTable.TableID.HTTPD_TOMCAT_PARAMETERS,
				accounting,
				aoServer,
				false
			);
		}

		conn.executeUpdate("delete from httpd_tomcat_contexts where pkey=?", pkey);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			accounting,
			aoServer,
			false
		);
	}

	public static void removeHttpdTomcatDataSource(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int tomcat_context=conn.executeIntQuery("select tomcat_context from httpd_tomcat_data_sources where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatDataSource", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate("delete from httpd_tomcat_data_sources where pkey=?", pkey);
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
		int tomcat_context=conn.executeIntQuery("select tomcat_context from httpd_tomcat_parameters where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "removeHttpdTomcatParameter", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate("delete from httpd_tomcat_parameters where pkey=?", pkey);
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
		int tomcat_context=conn.executeIntQuery("select tomcat_context from httpd_tomcat_data_sources where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "updateHttpdTomcatDataSource", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate(
			"update httpd_tomcat_data_sources set name=?, driver_class_name=?, url=?, username=?, password=?, max_active=?, max_idle=?, max_wait=?, validation_query=? where pkey=?",
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
		int tomcat_context=conn.executeIntQuery("select tomcat_context from httpd_tomcat_parameters where pkey=?", pkey);
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", tomcat_context);
		checkAccessHttpdSite(conn, source, "updateHttpdTomcatParameter", tomcat_site);

		AccountingCode accounting = getBusinessForHttpdSite(conn, tomcat_site);
		int aoServer = getAOServerForHttpdSite(conn, tomcat_site);

		conn.executeUpdate(
			"update httpd_tomcat_parameters set name=?, value=?, override=?, description=? where pkey=?",
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
			"update httpd_shared_tomcats set is_manual=? where pkey=?",
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
			"update httpd_shared_tomcats set max_post_size=? where pkey=?",
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
			"update httpd_shared_tomcats set unpack_wars=? where pkey=?",
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
			"update httpd_shared_tomcats set auto_deploy=? where pkey=?",
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
		String require
	) throws IOException, SQLException {
		int httpd_site=conn.executeIntQuery("select httpd_site from httpd_site_authenticated_locations where pkey=?", pkey);
		checkAccessHttpdSite(conn, source, "setHttpdSiteAuthenticatedLocationAttributes", httpd_site);
		if(isHttpdSiteDisabled(conn, httpd_site)) throw new SQLException("Unable to set HttpdSiteAuthenticatedLocation attributes, HttpdSite disabled: "+httpd_site);
		String error = HttpdSiteAuthenticatedLocation.validatePath(path);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthName(authName);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthGroupFile(authGroupFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateAuthUserFile(authUserFile);
		if(error==null) error = HttpdSiteAuthenticatedLocation.validateRequire(require);
		if(error!=null) throw new SQLException("Unable to add HttpdSiteAuthenticatedLocation: "+error);
		conn.executeUpdate(
			"update\n"
			+ "  httpd_site_authenticated_locations\n"
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
			"update httpd_site_binds set is_manual=? where pkey=?",
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
			"update httpd_site_binds set redirect_to_primary_hostname=? where pkey=?",
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
			"update httpd_site_binds set predisable_config=? where pkey=?",
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
			"update httpd_sites set is_manual=? where pkey=?",
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
			"update httpd_sites set server_admin=? where pkey=?",
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
			int osv = ServerHandler.getOperatingSystemVersionForServer(conn, aoServer);
			// Version must be correct for this server
			int tvOsv = conn.executeIntQuery(
				"select coalesce(\n"
				+ "  (select operating_system_version from technology_versions where pkey=? and name=?),\n"
				+ "  -1\n"
				+ ")",
				phpVersion,
				TechnologyName.PHP
			);
			if(tvOsv == -1) throw new SQLException("Requested PHP version is not a PHP version: #" + phpVersion);
			if(tvOsv != osv) throw new SQLException("Requested PHP version is for the wrong operating system version: #" + phpVersion + ": " + tvOsv + " != " + osv);
		}
		// Update the database
		conn.executeUpdate(
			"update httpd_sites set php_version=? where pkey=?",
			phpVersion == -1 ? DatabaseAccess.Null.INTEGER : phpVersion,
			pkey
		);

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITES,
			getBusinessForHttpdSite(conn, pkey),
			aoServer,
			false
		);
	}

	public static void setHttpdSiteEnableCgi(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean enableCgi
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdSiteEnableCgi", pkey);

		// Update the database
		conn.executeUpdate(
			"update httpd_sites set enable_cgi=? where pkey=?",
			enableCgi,
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
			"update httpd_sites set enable_ssi=? where pkey=?",
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
			"update httpd_sites set enable_htaccess=? where pkey=?",
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
			"update httpd_sites set enable_indexes=? where pkey=?",
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
			"update httpd_sites set enable_follow_symlinks=? where pkey=?",
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
			"update httpd_sites set enable_anonymous_ftp=? where pkey=?",
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
		UnixPath workDir
	) throws IOException, SQLException {
		int tomcat_site=conn.executeIntQuery("select tomcat_site from httpd_tomcat_contexts where pkey=?", pkey);
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
			workDir
		);
		String oldPath=conn.executeStringQuery("select path from httpd_tomcat_contexts where pkey=?", pkey);
		if(oldPath.length()==0 && path.length()>0) throw new SQLException("Not allowed to change the path of the default context: "+path);

		try (PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"update\n"
			+ "  httpd_tomcat_contexts\n"
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
			+ "  work_dir=?\n"
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
				pstmt.setInt(13, pkey);

				pstmt.executeUpdate();
			} catch(SQLException err) {
				System.err.println("Error from update: "+pstmt.toString());
				throw err;
			}
		}

		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_TOMCAT_CONTEXTS,
			getBusinessForHttpdSite(conn, tomcat_site),
			getAOServerForHttpdSite(conn, tomcat_site),
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
			"update httpd_tomcat_std_sites set max_post_size=? where httpd_site=?",
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
			"update httpd_tomcat_std_sites set unpack_wars=? where httpd_site=?",
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
			"update httpd_tomcat_std_sites set auto_deploy=? where httpd_site=?",
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

	public static void setPrimaryHttpdSiteURL(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey
	) throws IOException, SQLException {
		int hsb=conn.executeIntQuery("select httpd_site_bind from httpd_site_urls where pkey=?", pkey);
		int hs=getHttpdSiteForHttpdSiteBind(conn, hsb);
		checkAccessHttpdSite(conn, source, "setPrimaryHttpdSiteURL", hs);

		conn.executeUpdate("update httpd_site_urls set is_primary=(pkey=?) where httpd_site_bind=?", pkey, hsb);
		invalidateList.addTable(
			conn,
			SchemaTable.TableID.HTTPD_SITE_URLS,
			getBusinessForHttpdSite(conn, hs),
			getAOServerForHttpdSite(conn, hs),
			false
		);
	}

	public static void setHttpdTomcatSiteUseApache(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int pkey,
		boolean useApache
	) throws IOException, SQLException {
		checkAccessHttpdSite(conn, source, "setHttpdTomcatSiteUseApache", pkey);

		// Update the database
		int updateCount = conn.executeUpdate(
			"update httpd_tomcat_sites set use_apache=? where httpd_site=?",
			useApache,
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
