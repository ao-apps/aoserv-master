/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.aoserv.client.Resource;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
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
 * The <code>PackageHandler</code> handles all the accesses to the <code>packages</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class PackageHandler {

    private PackageHandler() {
    }

    private final static Map<AccountingCode,Boolean> disabledPackages=new HashMap<>();

    public static boolean canPackageAccessServer(DatabaseConnection conn, RequestSource source, AccountingCode packageName, int server) throws IOException, SQLException {
        return conn.executeBooleanQuery(
            "select\n"
            + "  (\n"
            + "    select\n"
            + "      pk.pkey\n"
            + "    from\n"
            + "      packages pk,\n"
            + "      business_servers bs\n"
            + "    where\n"
            + "      pk.name=?\n"
            + "      and pk.accounting=bs.accounting\n"
            + "      and bs.server=?\n"
            + "    limit 1\n"
            + "  )\n"
            + "  is not null\n",
            packageName,
            server
        );
    }

    public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, AccountingCode packageName) throws IOException, SQLException {
        return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackage(conn, packageName));
    }

    public static boolean canAccessPackage(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackage(conn, pkey));
    }

    public static boolean canAccessPackageDefinition(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackageDefinition(conn, pkey));
    }

    public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, AccountingCode packageName) throws IOException, SQLException {
        if(!canAccessPackage(conn, source, packageName)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access package: action='"
                +action
                +", name="
                +packageName
            ;
            throw new SQLException(message);
        }
    }

    public static void checkAccessPackage(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        if(!canAccessPackage(conn, source, pkey)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access package: action='"
                +action
                +", pkey="
                +pkey
            ;
            throw new SQLException(message);
        }
    }

    public static void checkAccessPackageDefinition(DatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        if(!canAccessPackageDefinition(conn, source, pkey)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access package: action='"
                +action
                +", pkey="
                +pkey
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
        AccountingCode packageName,
        AccountingCode accounting,
        int packageDefinition
    ) throws IOException, SQLException {
        BusinessHandler.checkAccessBusiness(conn, source, "addPackage", accounting);
        if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add Package '"+packageName+"', Business disabled: "+accounting);

        // Check the PackageDefinition rules
        checkAccessPackageDefinition(conn, source, "addPackage", packageDefinition);
        // Businesses parent must be the package definition owner
        AccountingCode parent=BusinessHandler.getParentBusiness(conn, accounting);
        AccountingCode packageDefinitionBusiness = getBusinessForPackageDefinition(conn, packageDefinition);
        if(!packageDefinitionBusiness.equals(parent)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition #"+packageDefinition+" not owned by parent Business");
        if(!isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not approved: "+packageDefinition);
        //if(!isPackageDefinitionActive(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not active: "+packageDefinition);

        int pkey = conn.executeIntUpdate(
            "INSERT INTO\n"
            + "  packages\n"
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
            + ") RETURNING pkey",
            packageName.toString(),
            accounting.toString(),
            packageDefinition,
            source.getUsername().toString(),
			Package.DEFAULT_EMAIL_IN_BURST,
			Package.DEFAULT_EMAIL_IN_RATE,
			Package.DEFAULT_EMAIL_OUT_BURST,
			Package.DEFAULT_EMAIL_OUT_RATE,
			Package.DEFAULT_EMAIL_RELAY_BURST,
			Package.DEFAULT_EMAIL_RELAY_RATE
		);

        // Notify all clients of the update
        invalidateList.addTable(conn, SchemaTable.TableID.PACKAGES, accounting, InvalidateList.allServers, false);

        return pkey;
    }

    /**
     * Creates a new <code>PackageDefinition</code>.
     */
    public static int addPackageDefinition(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        AccountingCode accounting,
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
        BusinessHandler.checkAccessBusiness(conn, source, "addPackageDefinition", accounting);
        if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add PackageDefinition, Business disabled: "+accounting);

        int pkey = conn.executeIntUpdate(
            "INSERT INTO\n"
            + "  package_definitions\n"
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
            + ") RETURNING pkey",
            accounting.toString(),
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
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );

        return pkey;
    }

    /**
     * Copies a <code>PackageDefinition</code>.
     */
    public static int copyPackageDefinition(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        checkAccessPackageDefinition(conn, source, "copyPackageDefinition", pkey);
        AccountingCode accounting = getBusinessForPackageDefinition(conn, pkey);
        if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to copy PackageDefinition, Business disabled: "+accounting);
        String category=conn.executeStringQuery("select category from package_definitions where pkey=?", pkey);
        String name=conn.executeStringQuery("select name from package_definitions where pkey=?", pkey);
        String version=conn.executeStringQuery("select version from package_definitions where pkey=?", pkey);
        String newVersion=null;
        for(int c=1;c<Integer.MAX_VALUE;c++) {
            String temp=version+"."+c;
            if(
                conn.executeBooleanQuery(
                    "select (select pkey from package_definitions where accounting=? and category=? and name=? and version=? limit 1) is null",
                    accounting,
                    category,
                    name,
                    temp
                )
            ) {
                newVersion=temp;
                break;
            }
        }
        if(newVersion==null) throw new SQLException("Unable to generate new version for copy PackageDefinition: "+pkey);

        int newPKey = conn.executeIntUpdate(
            "INSERT INTO package_definitions (\n"
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
            + "  package_definitions\n"
            + "WHERE\n"
            + "  pkey=?\n"
			+ "RETURNING pkey",
            newVersion,
            pkey
        );
        conn.executeUpdate(
            "insert into\n"
            + "  package_definition_limits\n"
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
            + "  package_definition_limits\n"
            + "where\n"
            + "  package_definition=?",
            newPKey,
            pkey
        );

        // Notify all clients of the update
        IntList servers=BusinessHandler.getServersForBusiness(conn, accounting);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            accounting,
            servers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITION_LIMITS,
            accounting,
            servers,
            false
        );

        return newPKey;
    }

    public static void updatePackageDefinition(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        AccountingCode accounting,
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
        checkAccessPackageDefinition(conn, source, "updatePackageDefinition", pkey);
        BusinessHandler.checkAccessBusiness(conn, source, "updatePackageDefinition", accounting);
        if(isPackageDefinitionApproved(conn, pkey)) throw new SQLException("Not allowed to update an approved PackageDefinition: "+pkey);

        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
            "update\n"
            + "  package_definitions\n"
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
            + "  pkey=?"
        );
        try {
            pstmt.setString(1, accounting.toString());
            pstmt.setString(2, category);
            pstmt.setString(3, name);
            pstmt.setString(4, version);
            pstmt.setString(5, display);
            pstmt.setString(6, description);
            pstmt.setBigDecimal(7, setupFee<=0 ? null : new BigDecimal(SQLUtility.getDecimal(setupFee)));
            pstmt.setString(8, setupFeeTransactionType);
            pstmt.setBigDecimal(9, new BigDecimal(SQLUtility.getDecimal(monthlyRate)));
            pstmt.setString(10, monthlyRateTransactionType);
            pstmt.setInt(11, pkey);
            pstmt.executeUpdate();
        } catch(SQLException err) {
            throw new WrappedSQLException(err, pstmt);
        } finally {
            pstmt.close();
        }

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void disablePackage(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        AccountingCode name
    ) throws IOException, SQLException {
        if(isPackageDisabled(conn, name)) throw new SQLException("Package is already disabled: "+name);
        BusinessHandler.checkAccessDisableLog(conn, source, "disablePackage", disableLog, false);
        checkAccessPackage(conn, source, "disablePackage", name);
        IntList hsts=HttpdHandler.getHttpdSharedTomcatsForPackage(conn, name);
        for(int c=0;c<hsts.size();c++) {
            int hst=hsts.getInt(c);
            if(!HttpdHandler.isHttpdSharedTomcatDisabled(conn, hst)) {
                throw new SQLException("Cannot disable Package '"+name+"': HttpdSharedTomcat not disabled: "+hst);
            }
        }
        IntList eps=EmailHandler.getEmailPipesForPackage(conn, name);
        for(int c=0;c<eps.size();c++) {
            int ep=eps.getInt(c);
            if(!EmailHandler.isEmailPipeDisabled(conn, ep)) {
                throw new SQLException("Cannot disable Package '"+name+"': EmailPipe not disabled: "+ep);
            }
        }
        List<UserId> uns=UsernameHandler.getUsernamesForPackage(conn, name);
		for (UserId username : uns) {
            if(!UsernameHandler.isUsernameDisabled(conn, username)) {
                throw new SQLException("Cannot disable Package '"+name+"': Username not disabled: "+username);
            }
        }
        IntList hss=HttpdHandler.getHttpdSitesForPackage(conn, name);
        for(int c=0;c<hss.size();c++) {
            int hs=hss.getInt(c);
            if(!HttpdHandler.isHttpdSiteDisabled(conn, hs)) {
                throw new SQLException("Cannot disable Package '"+name+"': HttpdSite not disabled: "+hs);
            }
        }
        IntList els=EmailHandler.getEmailListsForPackage(conn, name);
        for(int c=0;c<els.size();c++) {
            int el=els.getInt(c);
            if(!EmailHandler.isEmailListDisabled(conn, el)) {
                throw new SQLException("Cannot disable Package '"+name+"': EmailList not disabled: "+el);
            }
        }
        IntList ssrs=EmailHandler.getEmailSmtpRelaysForPackage(conn, name);
        for(int c=0;c<ssrs.size();c++) {
            int ssr=ssrs.getInt(c);
            if(!EmailHandler.isEmailSmtpRelayDisabled(conn, ssr)) {
                throw new SQLException("Cannot disable Package '"+name+"': EmailSmtpRelay not disabled: "+ssr);
            }
        }

        conn.executeUpdate(
            "update packages set disable_log=? where name=?",
            disableLog,
            name
        );

        // Notify all clients of the update
        AccountingCode accounting = getBusinessForPackage(conn, name);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGES,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static void enablePackage(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        AccountingCode name
    ) throws IOException, SQLException {
        int disableLog=getDisableLogForPackage(conn, name);
        if(disableLog==-1) throw new SQLException("Package is already enabled: "+name);
        BusinessHandler.checkAccessDisableLog(conn, source, "enablePackage", disableLog, true);
        checkAccessPackage(conn, source, "enablePackage", name);
        AccountingCode accounting = getBusinessForPackage(conn, name);
        if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to enable Package '"+name+"', Business not enabled: "+accounting);

        conn.executeUpdate(
            "update packages set disable_log=null where name=?",
            name
        );

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGES,
            accounting,
            BusinessHandler.getServersForBusiness(conn, accounting),
            false
        );
    }

    public static AccountingCode generatePackageName(
        DatabaseConnection conn,
        AccountingCode template
    ) throws IOException, SQLException {
		// Load the entire list of package names
		Set<AccountingCode> names = conn.executeObjectCollectionQuery(
			new HashSet<>(),
			ObjectFactories.accountingCodeFactory,
			"select name from packages"
		);
		// Find one that is not used
		for(int c=0;c<Integer.MAX_VALUE;c++) {
			AccountingCode name;
			try {
				name = AccountingCode.valueOf(template.toString()+c);
			} catch(ValidationException e) {
				throw new SQLException(e);
			}
			if(!names.contains(name)) return name;
		}
		// If could not find one, report and error
        throw new SQLException("Unable to find available package name for template: "+template);
    }

    public static int getDisableLogForPackage(DatabaseConnection conn, AccountingCode name) throws IOException, SQLException {
        return conn.executeIntQuery("select coalesce(disable_log, -1) from packages where name=?", name);
    }

    public static List<String> getPackages(
        DatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        UserId username=source.getUsername();
        MasterUser masterUser=MasterServer.getMasterUser(conn, username);
        com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, source.getUsername());
        if(masterUser!=null) {
            if(masterServers.length==0) return conn.executeStringListQuery("select name from packages");
            else return conn.executeStringListQuery(
                "select\n"
                + "  pk.name\n"
                + "from\n"
                + "  master_servers ms,\n"
                + "  business_servers bs,\n"
                + "  packages pk\n"
                + "where\n"
                + "  ms.username=?\n"
                + "  and ms.server=bs.server\n"
                + "  and bs.accounting=pk.accounting\n"
                + "group by\n"
                + "  pk.name",
                username
            );
        } else return conn.executeStringListQuery(
            "select\n"
            + "  pk2.name\n"
            + "from\n"
            + "  usernames un,\n"
            + "  packages pk1,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "  packages pk2\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk1.name\n"
            + "  and (\n"
            + TableHandler.PK1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pk2.accounting",
            username
        );
    }

    public static IntList getIntPackages(
        DatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        UserId username=source.getUsername();
        MasterUser masterUser=MasterServer.getMasterUser(conn, username);
        com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, source.getUsername());
        if(masterUser!=null) {
            if(masterServers.length==0) return conn.executeIntListQuery("select pkey from packages");
            else return conn.executeIntListQuery(
                "select\n"
                + "  pk.pkey\n"
                + "from\n"
                + "  master_servers ms,\n"
                + "  business_servers bs,\n"
                + "  packages pk\n"
                + "where\n"
                + "  ms.username=?\n"
                + "  and ms.server=bs.server\n"
                + "  and bs.accounting=pk.accounting\n"
                + "group by\n"
                + "  pk.pkey",
                username
            );
        } else return conn.executeIntListQuery(
            "select\n"
            + "  pk2.pkey\n"
            + "from\n"
            + "  usernames un,\n"
            + "  packages pk1,\n"
            + TableHandler.BU1_PARENTS_JOIN
            + "  packages pk2\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.package=pk1.name\n"
            + "  and (\n"
            + TableHandler.PK1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pk2.accounting",
            username
        );
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        if(tableID==SchemaTable.TableID.PACKAGES) {
            synchronized(PackageHandler.class) {
                disabledPackages.clear();
            }
            synchronized(packageBusinesses) {
                packageBusinesses.clear();
            }
            synchronized(packageNames) {
                packageNames.clear();
            }
            synchronized(packagePKeys) {
                packagePKeys.clear();
            }
        }
    }

    public static boolean isPackageDisabled(DatabaseConnection conn, AccountingCode name) throws IOException, SQLException {
	    synchronized(PackageHandler.class) {
            Boolean O=disabledPackages.get(name);
            if(O!=null) return O;
            boolean isDisabled=getDisableLogForPackage(conn, name)!=-1;
            disabledPackages.put(name, isDisabled);
            return isDisabled;
	    }
    }

    public static boolean isPackageNameAvailable(DatabaseConnection conn, AccountingCode packageName) throws IOException, SQLException {
        return conn.executeBooleanQuery("select (select pkey from packages where name=? limit 1) is null", packageName);
    }

    public static int findActivePackageDefinition(DatabaseConnection conn, AccountingCode accounting, int rate, int userLimit, int popLimit) throws IOException, SQLException {
        return conn.executeIntQuery(
            "select\n"
            + "  coalesce(\n"
            + "    (\n"
            + "      select\n"
            + "        pd.pkey\n"
            + "      from\n"
            + "        package_definitions pd,\n"
            + "        package_definitions_limits user_pdl,\n"
            + "        package_definitions_limits pop_pdl\n"
            + "      where\n"
            + "        pd.accounting=?\n"
            + "        and pd.monthly_rate=?\n"
            + "        and pd.pkey=user_pdl.package_definition\n"
            + "        and user_pdl.resource=?\n"
            + "        and pd.pkey=pop_pdl.package_definition\n"
            + "        and pop_pdl.resource=?\n"
            + "      limit 1\n"
            + "    ), -1\n"
            + "  )",
            accounting,
            SQLUtility.getDecimal(rate),
            Resource.USER,
            Resource.EMAIL
        );
    }

    public static boolean isPackageDefinitionApproved(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        return conn.executeBooleanQuery("select approved from package_definitions where pkey=?", packageDefinition);
    }

    public static boolean isPackageDefinitionActive(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        return conn.executeBooleanQuery("select active from package_definitions where pkey=?", packageDefinition);
    }

    public static void checkPackageAccessServer(DatabaseConnection conn, RequestSource source, String action, AccountingCode packageName, int server) throws IOException, SQLException {
        if(!canPackageAccessServer(conn, source, packageName, server)) {
            String message=
                "package.name="
                +packageName
                +" is not allowed to access server.pkey="
                +server
                +": action='"
                +action
                +"'"
            ;
            throw new SQLException(message);
        }
    }

    public static AccountingCode getBusinessForPackage(DatabaseAccess database, AccountingCode packageName) throws IOException, SQLException {
        return getBusinessForPackage(database, getPKeyForPackage(database, packageName));
    }

    private static final Map<Integer,AccountingCode> packageBusinesses=new HashMap<>();
    public static AccountingCode getBusinessForPackage(DatabaseAccess database, int pkey) throws IOException, SQLException {
        Integer I = pkey;
        synchronized(packageBusinesses) {
            AccountingCode O=packageBusinesses.get(I);
            if(O!=null) return O;
            AccountingCode business = database.executeObjectQuery(
                ObjectFactories.accountingCodeFactory,
                "select accounting from packages where pkey=?",
                pkey
            );
            packageBusinesses.put(I, business);
            return business;
        }
    }

    private static final Map<Integer,AccountingCode> packageNames=new HashMap<>();
    public static AccountingCode getNameForPackage(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        Integer I = pkey;
        synchronized(packageNames) {
            AccountingCode O=packageNames.get(I);
            if(O!=null) return O;
            AccountingCode name = conn.executeObjectQuery(
				ObjectFactories.accountingCodeFactory,
				"select name from packages where pkey=?",
				pkey
			);
            packageNames.put(I, name);
            return name;
        }
    }

    private static final Map<AccountingCode,Integer> packagePKeys=new HashMap<>();
    public static int getPKeyForPackage(DatabaseAccess database, AccountingCode name) throws IOException, SQLException {
        synchronized(packagePKeys) {
            Integer O=packagePKeys.get(name);
            if(O!=null) return O;
            int pkey=database.executeIntQuery("select pkey from packages where name=?", name);
            packagePKeys.put(name, pkey);
            return pkey;
        }
    }

    public static AccountingCode getBusinessForPackageDefinition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select accounting from package_definitions where pkey=?",
            pkey
        );
    }

    public static List<AccountingCode> getBusinessesForPackageDefinition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeObjectCollectionQuery(
            new ArrayList<AccountingCode>(),
            ObjectFactories.accountingCodeFactory,
            "select distinct\n"
            + "  bu.accounting\n"
            + "from\n"
            + "  packages pk,\n"
            + "  account.\"Account\" bu\n"
            + "where\n"
            + "  pk.package_definition=?\n"
            + "  and pk.accounting=bu.accounting",
            pkey
        );
    }

    public static void setPackageDefinitionActive(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean isActive
    ) throws IOException, SQLException {
        checkAccessPackageDefinition(conn, source, "setPackageDefinitionActive", pkey);
        // Must be approved to be activated
        if(isActive && !isPackageDefinitionApproved(conn, pkey)) throw new SQLException("PackageDefinition must be approved before it may be activated: "+pkey);

        // Update the database
        conn.executeUpdate(
            "update package_definitions set active=? where pkey=?",
            isActive,
            pkey
        );

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            getBusinessForPackageDefinition(conn, pkey),
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            getBusinessesForPackageDefinition(conn, pkey),
            InvalidateList.allServers,
            false
        );
    }

    public static void setPackageDefinitionLimits(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String[] resources,
        int[] soft_limits,
        int[] hard_limits,
        int[] additional_rates,
        String[] additional_transaction_types
    ) throws IOException, SQLException {
        checkAccessPackageDefinition(conn, source, "setPackageDefinitionLimits", pkey);
        // Must not be approved to be edited
        if(isPackageDefinitionApproved(conn, pkey)) throw new SQLException("PackageDefinition may not have its limits set after it is approved: "+pkey);

        // Update the database
        conn.executeUpdate("delete from package_definition_limits where package_definition=?", pkey);
        for(int c=0;c<resources.length;c++) {
            conn.executeUpdate(

                "insert into\n"
                + "  package_definition_limits\n"
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
                + "  ?::decimal(9,2),\n"
                + "  ?\n"
                + ")",
                pkey,
                resources[c],
                soft_limits[c]==-1 ? null : Integer.toString(soft_limits[c]),
                hard_limits[c]==-1 ? null : Integer.toString(hard_limits[c]),
                additional_rates[c]<=0 ? null : SQLUtility.getDecimal(additional_rates[c]),
                additional_transaction_types[c]
            );
        }

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITION_LIMITS,
            getBusinessForPackageDefinition(conn, pkey),
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITION_LIMITS,
            getBusinessesForPackageDefinition(conn, pkey),
            InvalidateList.allServers,
            false
        );
    }

    public static void removePackageDefinition(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Security checks
        PackageHandler.checkAccessPackageDefinition(conn, source, "removePackageDefinition", pkey);

        // Do the remove
        removePackageDefinition(conn, invalidateList, pkey);
    }

    public static void removePackageDefinition(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        AccountingCode accounting = getBusinessForPackageDefinition(conn, pkey);
        IntList servers=BusinessHandler.getServersForBusiness(conn, accounting);
        if(conn.executeUpdate("delete from package_definition_limits where package_definition=?", pkey)>0) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.PACKAGE_DEFINITION_LIMITS,
                accounting,
                servers,
                false
            );
        }

        conn.executeUpdate("delete from package_definitions where pkey=?", pkey);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.PACKAGE_DEFINITIONS,
            accounting,
            servers,
            false
        );
    }
}