/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.billing.Package;
import com.aoindustries.aoserv.client.billing.Resource;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseAccess.Null;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.IntList;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>PackageHandler</code> handles all the accesses to the <code>billing.Package</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class PackageHandler {

	private PackageHandler() {
	}

	private final static Map<Account.Name,Boolean> disabledPackages=new HashMap<>();

	public static boolean canPackageAccessHost(DatabaseConnection conn, RequestSource source, Account.Name packageName, int host) throws IOException, SQLException {
		return conn.executeBooleanQuery(
			"select\n"
			+ "  (\n"
			+ "    select\n"
			+ "      pk.id\n"
			+ "    from\n"
			+ "      billing.\"Package\" pk,\n"
			+ "      account.\"AccountHost\" bs\n"
			+ "    where\n"
			+ "      pk.name=?\n"
			+ "      and pk.accounting=bs.accounting\n"
			+ "      and bs.server=?\n"
			+ "    limit 1\n"
			+ "  )\n"
			+ "  is not null\n",
			packageName,
			host
		);
	}

	public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, Account.Name packageName) throws IOException, SQLException {
		return AccountHandler.canAccessAccount(conn, source, PackageHandler.getAccountForPackage(conn, packageName));
	}

	public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, int packageId) throws IOException, SQLException {
		return AccountHandler.canAccessAccount(conn, source, getAccountForPackage(conn, packageId));
	}

	public static boolean canAccessPackageDefinition(DatabaseConnection conn, RequestSource source, int packageDefinition) throws IOException, SQLException {
		return AccountHandler.canAccessAccount(conn, source, getAccountForPackageDefinition(conn, packageDefinition));
	}

	public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, Account.Name packageName) throws IOException, SQLException {
		if(!canAccessPackage(conn, source, packageName)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access package: action='"
				+action
				+", name="
				+packageName
			;
			throw new SQLException(message);
		}
	}

	public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, int packageId) throws IOException, SQLException {
		if(!canAccessPackage(conn, source, packageId)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access package: action='"
				+action
				+", id="
				+packageId
			;
			throw new SQLException(message);
		}
	}

	public static void checkAccessPackageDefinition(DatabaseConnection conn, RequestSource source, String action, int packageDefinition) throws IOException, SQLException {
		if(!canAccessPackageDefinition(conn, source, packageDefinition)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access package: action='"
				+action
				+", id="
				+packageDefinition
			;
			throw new SQLException(message);
		}
	}

	/**
	 * Creates a new <code>Package</code>.
	 */
	public static int addPackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name packageName,
		Account.Name account,
		int packageDefinition
	) throws IOException, SQLException {
		AccountHandler.checkAccessAccount(conn, source, "addPackage", account);
		if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to add Package '"+packageName+"', Account disabled: "+account);

		// Check the PackageDefinition rules
		checkAccessPackageDefinition(conn, source, "addPackage", packageDefinition);
		// Businesses parent must be the package definition owner
		Account.Name parent=AccountHandler.getParentAccount(conn, account);
		Account.Name packageDefinitionBusiness = getAccountForPackageDefinition(conn, packageDefinition);
		if(!packageDefinitionBusiness.equals(parent)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition #"+packageDefinition+" not owned by parent Account");
		if(!isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not approved: "+packageDefinition);
		//if(!isPackageDefinitionActive(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not active: "+packageDefinition);

		int packageId = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  billing.\"Package\"\n"
			+ "VALUES (\n"
			+ "  default,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  now(),\n"
			+ "  ?,\n"
			+ "  null,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?,\n"
			+ "  ?\n"
			+ ") RETURNING id",
			packageName.toString(),
			account.toString(),
			packageDefinition,
			source.getCurrentAdministrator().toString(),
			Package.DEFAULT_EMAIL_IN_BURST,
			Package.DEFAULT_EMAIL_IN_RATE,
			Package.DEFAULT_EMAIL_OUT_BURST,
			Package.DEFAULT_EMAIL_OUT_RATE,
			Package.DEFAULT_EMAIL_RELAY_BURST,
			Package.DEFAULT_EMAIL_RELAY_RATE
		);

		// Notify all clients of the update
		invalidateList.addTable(conn, Table.TableID.PACKAGES, account, InvalidateList.allHosts, false);

		return packageId;
	}

	/**
	 * Creates a new <code>PackageDefinition</code>.
	 */
	public static int addPackageDefinition(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name account,
		String category,
		String name,
		String version,
		String display,
		String description,
		int setupFee,
		String setupFeeTransactionType,
		int monthlyRate,
		String monthlyRateTransactionType
	) throws IOException, SQLException {
		AccountHandler.checkAccessAccount(conn, source, "addPackageDefinition", account);
		if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to add PackageDefinition, Account disabled: "+account);

		int packageDefinition = conn.executeIntUpdate(
			"INSERT INTO\n"
			+ "  billing.\"PackageDefinition\"\n"
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
			+ "  false,\n"
			+ "  false\n"
			+ ") RETURNING id",
			account.toString(),
			category,
			name,
			version,
			display,
			description,
			setupFee <= 0 ? Null.NUMERIC : new BigDecimal(SQLUtility.getDecimal(setupFee)),
			setupFeeTransactionType,
			new BigDecimal(SQLUtility.getDecimal(monthlyRate)),
			monthlyRateTransactionType
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);

		return packageDefinition;
	}

	/**
	 * Copies a <code>PackageDefinition</code>.
	 */
	public static int copyPackageDefinition(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int packageDefinition
	) throws IOException, SQLException {
		checkAccessPackageDefinition(conn, source, "copyPackageDefinition", packageDefinition);
		Account.Name account = getAccountForPackageDefinition(conn, packageDefinition);
		if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to copy PackageDefinition, Account disabled: "+account);
		String category=conn.executeStringQuery("select category from billing.\"PackageDefinition\" where id=?", packageDefinition);
		String name=conn.executeStringQuery("select name from billing.\"PackageDefinition\" where id=?", packageDefinition);
		String version=conn.executeStringQuery("select version from billing.\"PackageDefinition\" where id=?", packageDefinition);
		String newVersion=null;
		for(int c=1;c<Integer.MAX_VALUE;c++) {
			String temp=version+"."+c;
			if(
				conn.executeBooleanQuery(
					"select (select id from billing.\"PackageDefinition\" where accounting=? and category=? and name=? and version=? limit 1) is null",
					account,
					category,
					name,
					temp
				)
			) {
				newVersion=temp;
				break;
			}
		}
		if(newVersion==null) throw new SQLException("Unable to generate new version for copy PackageDefinition: "+packageDefinition);

		int newPKey = conn.executeIntUpdate(
			"INSERT INTO billing.\"PackageDefinition\" (\n"
			+ "  accounting,\n"
			+ "  category,\n"
			+ "  name,\n"
			+ "  version,\n"
			+ "  display,\n"
			+ "  description,\n"
			+ "  setup_fee,\n"
			+ "  setup_fee_transaction_type,\n"
			+ "  monthly_rate,\n"
			+ "  monthly_rate_transaction_type\n"
			+ ") SELECT\n"
			+ "  accounting,\n"
			+ "  category,\n"
			+ "  name,\n"
			+ "  ?,\n"
			+ "  display,\n"
			+ "  description,\n"
			+ "  setup_fee,\n"
			+ "  setup_fee_transaction_type,\n"
			+ "  monthly_rate,\n"
			+ "  monthly_rate_transaction_type\n"
			+ "FROM\n"
			+ "  billing.\"PackageDefinition\"\n"
			+ "WHERE\n"
			+ "  id=?\n"
			+ "RETURNING id",
			newVersion,
			packageDefinition
		);
		conn.executeUpdate(
			"insert into\n"
			+ "  billing.\"PackageDefinitionLimit\"\n"
			+ "(\n"
			+ "  package_definition,\n"
			+ "  resource,\n"
			+ "  soft_limit,\n"
			+ "  hard_limit,\n"
			+ "  additional_rate,\n"
			+ "  additional_transaction_type\n"
			+ ") select\n"
			+ "  ?,\n"
			+ "  resource,\n"
			+ "  soft_limit,\n"
			+ "  hard_limit,\n"
			+ "  additional_rate,\n"
			+ "  additional_transaction_type\n"
			+ "from\n"
			+ "  billing.\"PackageDefinitionLimit\"\n"
			+ "where\n"
			+ "  package_definition=?",
			newPKey,
			packageDefinition
		);

		// Notify all clients of the update
		IntList servers = AccountHandler.getHostsForAccount(conn, account);
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			account,
			servers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGE_DEFINITION_LIMITS,
			account,
			servers,
			false
		);

		return newPKey;
	}

	public static void updatePackageDefinition(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int packageDefinition,
		Account.Name account,
		String category,
		String name,
		String version,
		String display,
		String description,
		int setupFee,
		String setupFeeTransactionType,
		int monthlyRate,
		String monthlyRateTransactionType
	) throws IOException, SQLException {
		// Security checks
		checkAccessPackageDefinition(conn, source, "updatePackageDefinition", packageDefinition);
		AccountHandler.checkAccessAccount(conn, source, "updatePackageDefinition", account);
		if(isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("Not allowed to update an approved PackageDefinition: "+packageDefinition);

		PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
			"update\n"
			+ "  billing.\"PackageDefinition\"\n"
			+ "set\n"
			+ "  accounting=?,\n"
			+ "  category=?,\n"
			+ "  name=?,\n"
			+ "  version=?,\n"
			+ "  display=?,\n"
			+ "  description=?,\n"
			+ "  setup_fee=?,\n"
			+ "  setup_fee_transaction_type=?,\n"
			+ "  monthly_rate=?,\n"
			+ "  monthly_rate_transaction_type=?\n"
			+ "where\n"
			+ "  id=?"
		);
		try {
			pstmt.setString(1, account.toString());
			pstmt.setString(2, category);
			pstmt.setString(3, name);
			pstmt.setString(4, version);
			pstmt.setString(5, display);
			pstmt.setString(6, description);
			pstmt.setBigDecimal(7, setupFee<=0 ? null : new BigDecimal(SQLUtility.getDecimal(setupFee)));
			pstmt.setString(8, setupFeeTransactionType);
			pstmt.setBigDecimal(9, new BigDecimal(SQLUtility.getDecimal(monthlyRate)));
			pstmt.setString(10, monthlyRateTransactionType);
			pstmt.setInt(11, packageDefinition);
			pstmt.executeUpdate();
		} catch(SQLException err) {
			throw new WrappedSQLException(err, pstmt);
		} finally {
			pstmt.close();
		}

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void disablePackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int disableLog,
		Account.Name packageName
	) throws IOException, SQLException {
		AccountHandler.checkAccessDisableLog(conn, source, "disablePackage", disableLog, false);
		checkAccessPackage(conn, source, "disablePackage", packageName);
		if(isPackageDisabled(conn, packageName)) throw new SQLException("Package is already disabled: "+packageName);
		IntList hsts=WebHandler.getHttpdSharedTomcatsForPackage(conn, packageName);
		for(int c=0;c<hsts.size();c++) {
			int hst=hsts.getInt(c);
			if(!WebHandler.isSharedTomcatDisabled(conn, hst)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': SharedTomcat not disabled: "+hst);
			}
		}
		IntList eps=EmailHandler.getPipesForPackage(conn, packageName);
		for(int c=0;c<eps.size();c++) {
			int ep=eps.getInt(c);
			if(!EmailHandler.isPipeDisabled(conn, ep)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': Pipe not disabled: "+ep);
			}
		}
		List<com.aoindustries.aoserv.client.account.User.Name> users = AccountUserHandler.getUsersForPackage(conn, packageName);
		for (com.aoindustries.aoserv.client.account.User.Name user : users) {
			if(!AccountUserHandler.isUserDisabled(conn, user)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': Username not disabled: "+user);
			}
		}
		IntList hss=WebHandler.getHttpdSitesForPackage(conn, packageName);
		for(int c=0;c<hss.size();c++) {
			int hs=hss.getInt(c);
			if(!WebHandler.isSiteDisabled(conn, hs)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': Site not disabled: "+hs);
			}
		}
		IntList els=EmailHandler.getListsForPackage(conn, packageName);
		for(int c=0;c<els.size();c++) {
			int el=els.getInt(c);
			if(!EmailHandler.isListDisabled(conn, el)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': List not disabled: "+el);
			}
		}
		IntList ssrs=EmailHandler.getSmtpRelaysForPackage(conn, packageName);
		for(int c=0;c<ssrs.size();c++) {
			int ssr=ssrs.getInt(c);
			if(!EmailHandler.isSmtpRelayDisabled(conn, ssr)) {
				throw new SQLException("Cannot disable Package '"+packageName+"': SmtpRelay not disabled: "+ssr);
			}
		}

		conn.executeUpdate(
			"update billing.\"Package\" set disable_log=? where name=?",
			disableLog,
			packageName
		);

		// Notify all clients of the update
		Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGES,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static void enablePackage(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name packageName
	) throws IOException, SQLException {
		checkAccessPackage(conn, source, "enablePackage", packageName);
		int disableLog=getDisableLogForPackage(conn, packageName);
		if(disableLog==-1) throw new SQLException("Package is already enabled: "+packageName);
		AccountHandler.checkAccessDisableLog(conn, source, "enablePackage", disableLog, true);
		Account.Name account = PackageHandler.getAccountForPackage(conn, packageName);
		if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to enable Package '"+packageName+"', Account not enabled: "+account);

		conn.executeUpdate(
			"update billing.\"Package\" set disable_log=null where name=?",
			packageName
		);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGES,
			account,
			AccountHandler.getHostsForAccount(conn, account),
			false
		);
	}

	public static Account.Name generatePackageName(
		DatabaseConnection conn,
		Account.Name template
	) throws IOException, SQLException {
		// Load the entire list of package names
		Set<Account.Name> names = conn.executeObjectCollectionQuery(new HashSet<>(),
			ObjectFactories.accountNameFactory,
			"select name from billing.\"Package\""
		);
		// Find one that is not used
		for(int c=0;c<Integer.MAX_VALUE;c++) {
			Account.Name name;
			try {
				name = Account.Name.valueOf(template.toString()+c);
			} catch(ValidationException e) {
				throw new SQLException(e);
			}
			if(!names.contains(name)) return name;
		}
		// If could not find one, report and error
		throw new SQLException("Unable to find available package name for template: "+template);
	}

	public static int getDisableLogForPackage(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
		return conn.executeIntQuery("select coalesce(disable_log, -1) from billing.\"Package\" where name=?", packageName);
	}

	// TODO: Unused? 2019-07-10
	public static List<Account.Name> getPackages(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		User masterUser=MasterServer.getUser(conn, currentAdministrator);
		UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, currentAdministrator);
		if(masterUser!=null) {
			if(masterServers.length==0) {
				return conn.executeObjectListQuery(
					ObjectFactories.accountNameFactory,
					"select name from billing.\"Package\""
				);
			} else {
				return conn.executeObjectListQuery(
					ObjectFactories.accountNameFactory,
					"select\n"
					+ "  pk.name\n"
					+ "from\n"
					+ "  master.\"UserHost\" ms,\n"
					+ "  account.\"AccountHost\" bs,\n"
					+ "  billing.\"Package\" pk\n"
					+ "where\n"
					+ "  ms.username=?\n"
					+ "  and ms.server=bs.server\n"
					+ "  and bs.accounting=pk.accounting\n"
					+ "group by\n"
					+ "  pk.name",
					currentAdministrator
				);
			}
		} else {
			return conn.executeObjectListQuery(
				ObjectFactories.accountNameFactory,
				"select\n"
				+ "  pk2.name\n"
				+ "from\n"
				+ "  account.\"User\" un,\n"
				+ "  billing.\"Package\" pk1,\n"
				+ TableHandler.BU1_PARENTS_JOIN
				+ "  billing.\"Package\" pk2\n"
				+ "where\n"
				+ "  un.username=?\n"
				+ "  and un.package=pk1.name\n"
				+ "  and (\n"
				+ TableHandler.PK1_BU1_PARENTS_WHERE
				+ "  )\n"
				+ "  and bu1.accounting=pk2.accounting",
				currentAdministrator
			);
		}
	}

	// TODO: Unused? 2019-07-10
	public static IntList getIntPackages(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		User masterUser=MasterServer.getUser(conn, currentAdministrator);
		UserHost[] masterServers=masterUser==null?null:MasterServer.getUserHosts(conn, currentAdministrator);
		if(masterUser!=null) {
			if(masterServers.length==0) return conn.executeIntListQuery("select id from billing.\"Package\"");
			else return conn.executeIntListQuery(
				"select\n"
				+ "  pk.id\n"
				+ "from\n"
				+ "  master.\"UserHost\" ms,\n"
				+ "  account.\"AccountHost\" bs,\n"
				+ "  billing.\"Package\" pk\n"
				+ "where\n"
				+ "  ms.username=?\n"
				+ "  and ms.server=bs.server\n"
				+ "  and bs.accounting=pk.accounting\n"
				+ "group by\n"
				+ "  pk.id",
				currentAdministrator
			);
		} else return conn.executeIntListQuery(
			"select\n"
			+ "  pk2.id\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting",
			currentAdministrator
		);
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.PACKAGES) {
			synchronized(PackageHandler.class) {
				disabledPackages.clear();
			}
			synchronized(packageAccounts) {
				packageAccounts.clear();
			}
			synchronized(packageNames) {
				packageNames.clear();
			}
			synchronized(packageIds) {
				packageIds.clear();
			}
		}
	}

	public static boolean isPackageDisabled(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
		synchronized(PackageHandler.class) {
			Boolean O=disabledPackages.get(packageName);
			if(O!=null) return O;
			boolean isDisabled=getDisableLogForPackage(conn, packageName)!=-1;
			disabledPackages.put(packageName, isDisabled);
			return isDisabled;
		}
	}

	public static boolean isPackageNameAvailable(DatabaseConnection conn, Account.Name packageName) throws IOException, SQLException {
		return conn.executeBooleanQuery("select (select id from billing.\"Package\" where name=? limit 1) is null", packageName);
	}

	public static int findActivePackageDefinition(DatabaseConnection conn, Account.Name account, int rate, int userLimit, int popLimit) throws IOException, SQLException {
		return conn.executeIntQuery(
			"select\n"
			+ "  coalesce(\n"
			+ "    (\n"
			+ "      select\n"
			+ "        pd.id\n"
			+ "      from\n"
			+ "        billing.\"PackageDefinition\" pd,\n"
			+ "        package_definitions_limits user_pdl,\n"
			+ "        package_definitions_limits pop_pdl\n"
			+ "      where\n"
			+ "        pd.accounting=?\n"
			+ "        and pd.monthly_rate=?\n"
			+ "        and pd.id=user_pdl.package_definition\n"
			+ "        and user_pdl.resource=?\n"
			+ "        and pd.id=pop_pdl.package_definition\n"
			+ "        and pop_pdl.resource=?\n"
			+ "      limit 1\n"
			+ "    ), -1\n"
			+ "  )",
			account,
			SQLUtility.getDecimal(rate),
			Resource.USER,
			Resource.EMAIL
		);
	}

	public static boolean isPackageDefinitionApproved(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
		return conn.executeBooleanQuery("select approved from billing.\"PackageDefinition\" where id=?", packageDefinition);
	}

	public static boolean isPackageDefinitionActive(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
		return conn.executeBooleanQuery("select active from billing.\"PackageDefinition\" where id=?", packageDefinition);
	}

	public static void checkPackageAccessHost(DatabaseConnection conn, RequestSource source, String action, Account.Name packageName, int host) throws IOException, SQLException {
		if(!canPackageAccessHost(conn, source, packageName, host)) {
			String message=
				"package.name="
				+packageName
				+" is not allowed to access server.id="
				+host
				+": action='"
				+action
				+"'"
			;
			throw new SQLException(message);
		}
	}

	public static Account.Name getAccountForPackage(DatabaseAccess database, Account.Name packageName) throws IOException, SQLException {
		return getAccountForPackage(database, getIdForPackage(database, packageName));
	}

	private static final Map<Integer,Account.Name> packageAccounts = new HashMap<>();
	public static Account.Name getAccountForPackage(DatabaseAccess database, int packageId) throws IOException, SQLException {
		Integer I = packageId;
		synchronized(packageAccounts) {
			Account.Name O=packageAccounts.get(I);
			if(O!=null) return O;
			Account.Name business = database.executeObjectQuery(ObjectFactories.accountNameFactory,
				"select accounting from billing.\"Package\" where id=?",
				packageId
			);
			packageAccounts.put(I, business);
			return business;
		}
	}

	private static final Map<Integer,Account.Name> packageNames = new HashMap<>();
	public static Account.Name getNameForPackage(DatabaseConnection conn, int packageId) throws IOException, SQLException {
		Integer I = packageId;
		synchronized(packageNames) {
			Account.Name O=packageNames.get(I);
			if(O!=null) return O;
			Account.Name name = conn.executeObjectQuery(ObjectFactories.accountNameFactory,
				"select name from billing.\"Package\" where id=?",
				packageId
			);
			packageNames.put(I, name);
			return name;
		}
	}

	private static final Map<Account.Name,Integer> packageIds = new HashMap<>();
	public static int getIdForPackage(DatabaseAccess database, Account.Name name) throws IOException, SQLException {
		synchronized(packageIds) {
			Integer O = packageIds.get(name);
			if(O != null) return O;
			int packageId = database.executeIntQuery("select id from billing.\"Package\" where name=?", name);
			packageIds.put(name, packageId);
			return packageId;
		}
	}

	public static Account.Name getAccountForPackageDefinition(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.accountNameFactory,
			"select accounting from billing.\"PackageDefinition\" where id=?",
			packageDefinition
		);
	}

	public static List<Account.Name> getAccountsForPackageDefinition(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
		return conn.executeObjectCollectionQuery(
			new ArrayList<>(),
			ObjectFactories.accountNameFactory,
			"select distinct\n"
			+ "  bu.accounting\n"
			+ "from\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"Account\" bu\n"
			+ "where\n"
			+ "  pk.package_definition=?\n"
			+ "  and pk.accounting=bu.accounting",
			packageDefinition
		);
	}

	public static void setPackageDefinitionActive(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int packageDefinition,
		boolean isActive
	) throws IOException, SQLException {
		checkAccessPackageDefinition(conn, source, "setPackageDefinitionActive", packageDefinition);
		// Must be approved to be activated
		if(isActive && !isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("PackageDefinition must be approved before it may be activated: "+packageDefinition);

		// Update the database
		conn.executeUpdate(
			"update billing.\"PackageDefinition\" set active=? where id=?",
			isActive,
			packageDefinition
		);

		invalidateList.addTable(conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			getAccountForPackageDefinition(conn, packageDefinition),
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			getAccountsForPackageDefinition(conn, packageDefinition),
			InvalidateList.allHosts,
			false
		);
	}

	public static void setPackageDefinitionLimits(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int packageDefinition,
		String[] resources,
		int[] soft_limits,
		int[] hard_limits,
		int[] additional_rates,
		String[] additional_transaction_types
	) throws IOException, SQLException {
		checkAccessPackageDefinition(conn, source, "setPackageDefinitionLimits", packageDefinition);
		// Must not be approved to be edited
		if(isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("PackageDefinition may not have its limits set after it is approved: "+packageDefinition);

		// Update the database
		conn.executeUpdate("delete from billing.\"PackageDefinitionLimit\" where package_definition=?", packageDefinition);
		for(int c=0;c<resources.length;c++) {
			conn.executeUpdate(
				"insert into\n"
				+ "  billing.\"PackageDefinitionLimit\"\n"
				+ "(\n"
				+ "  package_definition,\n"
				+ "  resource,\n"
				+ "  soft_limit,\n"
				+ "  hard_limit,\n"
				+ "  additional_rate,\n"
				+ "  additional_transaction_type\n"
				+ ") values(\n"
				+ "  ?,\n"
				+ "  ?,\n"
				+ "  ?::integer,\n"
				+ "  ?::integer,\n"
				+ "  ?::numeric(9,2),\n"
				+ "  ?\n"
				+ ")",
				packageDefinition,
				resources[c],
				soft_limits[c]==-1 ? null : Integer.toString(soft_limits[c]),
				hard_limits[c]==-1 ? null : Integer.toString(hard_limits[c]),
				additional_rates[c]<=0 ? null : SQLUtility.getDecimal(additional_rates[c]),
				additional_transaction_types[c]
			);
		}

		invalidateList.addTable(conn,
			Table.TableID.PACKAGE_DEFINITION_LIMITS,
			getAccountForPackageDefinition(conn, packageDefinition),
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.PACKAGE_DEFINITION_LIMITS,
			getAccountsForPackageDefinition(conn, packageDefinition),
			InvalidateList.allHosts,
			false
		);
	}

	public static void removePackageDefinition(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int packageDefinition
	) throws IOException, SQLException {
		// Security checks
		PackageHandler.checkAccessPackageDefinition(conn, source, "removePackageDefinition", packageDefinition);

		// Do the remove
		removePackageDefinition(conn, invalidateList, packageDefinition);
	}

	public static void removePackageDefinition(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int packageDefinition
	) throws IOException, SQLException {
		Account.Name account = getAccountForPackageDefinition(conn, packageDefinition);
		IntList servers=AccountHandler.getHostsForAccount(conn, account);
		if(conn.executeUpdate("delete from billing.\"PackageDefinitionLimit\" where package_definition=?", packageDefinition)>0) {
			invalidateList.addTable(
				conn,
				Table.TableID.PACKAGE_DEFINITION_LIMITS,
				account,
				servers,
				false
			);
		}

		conn.executeUpdate("delete from billing.\"PackageDefinition\" where id=?", packageDefinition);
		invalidateList.addTable(
			conn,
			Table.TableID.PACKAGE_DEFINITIONS,
			account,
			servers,
			false
		);
	}
}
