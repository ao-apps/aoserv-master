package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.Package;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * The <code>PackageHandler</code> handles all the accesses to the <code>packages</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class PackageHandler {

    private final static Map<String,Boolean> disabledPackages=new HashMap<String,Boolean>();

    public static boolean canPackageAccessServer(MasterDatabaseConnection conn, RequestSource source, String packageName, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "canPackageAccessServer(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            return conn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canAccessPackage(MasterDatabaseConnection conn, RequestSource source, String packageName) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "canAccessPackage(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackage(conn, packageName));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean canAccessPackage(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "canAccessPackage(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackage(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean canAccessPackageDefinition(MasterDatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "canAccessPackageDefinition(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackageDefinition(conn, pkey));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessPackage(MasterDatabaseConnection conn, RequestSource source, String action, String packageName) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "checkAccessPackage(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            if(!canAccessPackage(conn, source, packageName)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access package: action='"
                    +action
                    +", name="
                    +packageName
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessPackage(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "checkAccessPackage(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessPackage(conn, source, pkey)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access package: action='"
                    +action
                    +", pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessPackageDefinition(MasterDatabaseConnection conn, RequestSource source, String action, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "checkAccessPackageDefinition(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessPackageDefinition(conn, source, pkey)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access package: action='"
                    +action
                    +", pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Creates a new <code>Package</code>.
     */
    public static int addPackage(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String packageName,
        String accounting,
        int packageDefinition
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "addPackage(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,int)", null);
        try {
            if(!Package.isValidPackageName(packageName)) throw new SQLException("Invalid package name: "+packageName);

            BusinessHandler.checkAccessBusiness(conn, source, "addPackage", accounting);
            if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add Package '"+packageName+"', Business disabled: "+accounting);

            // Check the PackageDefinition rules
            checkAccessPackageDefinition(conn, source, "addPackage", packageDefinition);
            // Businesses parent must be the package definition owner
            String parent=BusinessHandler.getParentBusiness(conn, accounting);
            String packageDefinitionBusiness=getBusinessForPackageDefinition(conn, packageDefinition);
            if(!packageDefinitionBusiness.equals(parent)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition #"+packageDefinition+" not owned by parent Business");
            if(!isPackageDefinitionApproved(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not approved: "+packageDefinition);
            if(!isPackageDefinitionActive(conn, packageDefinition)) throw new SQLException("Unable to add Package '"+packageName+"', PackageDefinition not active: "+packageDefinition);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('packages_pkey_seq')");
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                "insert into\n"
                + "  packages\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  now(),\n"
                + "  ?,\n"
                + "  "+Package.DEFAULT_DAILY_SMTP_IN_LIMIT+",\n"
                + "  "+Package.DEFAULT_DAILY_SMTP_IN_BANDWIDTH_LIMIT+"::int8,\n"
                + "  "+Package.DEFAULT_DAILY_SMTP_OUT_LIMIT+",\n"
                + "  "+Package.DEFAULT_DAILY_SMTP_OUT_BANDWIDTH_LIMIT+"::int8,\n"
                + "  null,\n"
                + "  "+Package.DEFAULT_EMAIL_IN_BURST+"::integer,\n"
                + "  "+Package.DEFAULT_EMAIL_IN_RATE+"::float4,\n"
                + "  "+Package.DEFAULT_EMAIL_OUT_BURST+"::integer,\n"
                + "  "+Package.DEFAULT_EMAIL_OUT_RATE+"::float4,\n"
                + "  "+Package.DEFAULT_EMAIL_RELAY_BURST+"::integer,\n"
                + "  "+Package.DEFAULT_EMAIL_RELAY_RATE+"::float4\n"
                + ")"
            );
            try {
                pstmt.setInt(1, pkey);
                pstmt.setString(2, packageName);
                pstmt.setString(3, accounting);
                pstmt.setInt(4, packageDefinition);
                pstmt.setString(5, source.getUsername());
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } catch(SQLException err) {
                throw new WrappedSQLException(err, pstmt);
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.TableID.PACKAGES, accounting, InvalidateList.allServers, false);
            
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Creates a new <code>PackageDefinition</code>.
     */
    public static int addPackageDefinition(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
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
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "addPackageDefinition(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,String,String,String,int,String,int,String)", null);
        try {
            BusinessHandler.checkAccessBusiness(conn, source, "addPackageDefinition", accounting);
            if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add PackageDefinition, Business disabled: "+accounting);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('package_definitions_pkey_seq')");
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                "insert into\n"
                + "  package_definitions\n"
                + "values(\n"
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
                + "  false,\n"
                + "  false\n"
                + ")"
            );
            try {
                pstmt.setInt(1, pkey);
                pstmt.setString(2, accounting);
                pstmt.setString(3, category);
                pstmt.setString(4, name);
                pstmt.setString(5, version);
                pstmt.setString(6, display);
                pstmt.setString(7, description);
                pstmt.setBigDecimal(8, setupFee<=0 ? null : new BigDecimal(SQLUtility.getDecimal(setupFee)));
                pstmt.setString(9, setupFeeTransactionType);
                pstmt.setBigDecimal(10, new BigDecimal(SQLUtility.getDecimal(monthlyRate)));
                pstmt.setString(11, monthlyRateTransactionType);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
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

            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Copies a <code>PackageDefinition</code>.
     */
    public static int copyPackageDefinition(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "copyPackageDefinition(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            checkAccessPackageDefinition(conn, source, "copyPackageDefinition", pkey);
            String accounting=getBusinessForPackageDefinition(conn, pkey);
            if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to copy PackageDefinition, Business disabled: "+accounting);
            String category=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select category from package_definitions where pkey=?", pkey);
            String name=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select name from package_definitions where pkey=?", pkey);
            String version=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select version from package_definitions where pkey=?", pkey);
            String newVersion=null;
            for(int c=1;c<Integer.MAX_VALUE;c++) {
                String temp=version+"."+c;
                if(
                    conn.executeBooleanQuery(
                        Connection.TRANSACTION_READ_COMMITTED,
                        true,
                        true,
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

            int newPKey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('package_definitions_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  package_definitions\n"
                + "select\n"
                + "  ?,\n"
                + "  accounting,\n"
                + "  category,\n"
                + "  name,\n"
                + "  ?,\n"
                + "  display,\n"
                + "  description,\n"
                + "  setup_fee,\n"
                + "  setup_fee_transaction_type,\n"
                + "  monthly_rate,\n"
                + "  monthly_rate_transaction_type,\n"
                + "  false,\n"
                + "  false\n"
                + "from\n"
                + "  package_definitions\n"
                + "where\n"
                + "  pkey=?",
                newPKey,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void updatePackageDefinition(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String accounting,
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
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "updatePackageDefinition(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String,String,String,String,int,String,int,String)", null);
        try {
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
                pstmt.setString(1, accounting);
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
                conn.incrementUpdateCount();
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disablePackage(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "disablePackage(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
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
            List<String> uns=UsernameHandler.getUsernamesForPackage(conn, name);
            for(int c=0;c<uns.size();c++) {
                String username=uns.get(c);
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
            String accounting=getBusinessForPackage(conn, name);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.PACKAGES,
                accounting,
                BusinessHandler.getServersForBusiness(conn, accounting),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enablePackage(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String name
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "enablePackage(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForPackage(conn, name);
            if(disableLog==-1) throw new SQLException("Package is already enabled: "+name);
            BusinessHandler.checkAccessDisableLog(conn, source, "enablePackage", disableLog, true);
            checkAccessPackage(conn, source, "enablePackage", name);
            String accounting=getBusinessForPackage(conn, name);
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String generatePackageName(
        MasterDatabaseConnection conn,
        String template
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "generatePackageName(MasterDatabaseConnection,String)", null);
        try {
            // Load the entire list of package names
            List<String> names=conn.executeStringListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select name from packages");
            int size=names.size();

            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            for(int c=0;c<size;c++) sorted.add(names.get(c));

            // Find one that is not used
            String goodOne=null;
            for(int c=1;c<Integer.MAX_VALUE;c++) {
                String name=template+c;
                if(!Package.isValidPackageName(name)) throw new SQLException("Invalid package name: "+name);
                if(!sorted.contains(name)) {
                    goodOne=name;
                    break;
                }
            }

            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available package name for template: "+template);
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForPackage(MasterDatabaseConnection conn, String name) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "getDisableLogForPackage(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery("select coalesce(disable_log, -1) from packages where name=?", name);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getPackages(
        MasterDatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "getPackages(MasterDatabaseConnection,RequestSource)", null);
        try {
            String username=source.getUsername();
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getIntPackages(
        MasterDatabaseConnection conn,
        RequestSource source
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "getIntPackages(MasterDatabaseConnection,RequestSource)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, source.getUsername());
            if(masterUser!=null) {
                if(masterServers.length==0) return conn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select pkey from packages");
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
                Connection.TRANSACTION_READ_COMMITTED,
                true,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "invalidateTable(SchemaTable.TableID)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isPackageDisabled(MasterDatabaseConnection conn, String name) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "isPackageDisabled(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(PackageHandler.class) {
		Boolean O=disabledPackages.get(name);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForPackage(conn, name)!=-1;
		disabledPackages.put(name, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isPackageNameAvailable(MasterDatabaseConnection conn, String packageName) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "isPackageNameAvailable(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery("select (select pkey from packages where name=? limit 1) is null", packageName);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int findActivePackageDefinition(MasterDatabaseConnection conn, String accounting, int rate, int userLimit, int popLimit) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "findActivePackageDefinition(MasterDatabaseConnection,String,int,int,int)", null);
        try {
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
                Resource.POP
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isPackageDefinitionApproved(MasterDatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "isPackageDefinitionApproved(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeBooleanQuery("select approved from package_definitions where pkey=?", packageDefinition);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isPackageDefinitionActive(MasterDatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "isPackageDefinitionActive(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeBooleanQuery("select active from package_definitions where pkey=?", packageDefinition);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkPackageAccessServer(MasterDatabaseConnection conn, RequestSource source, String action, String packageName, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "checkPackageAccessServer(MasterDatabaseConnection,RequestSource,String,String,int)", null);
        try {
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
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForPackage(MasterDatabaseConnection conn, String packageName) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "getBusinessForPackage(MasterDatabaseConnection,String)", null);
        try {
            return getBusinessForPackage(conn, getPKeyForPackage(conn, packageName));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static final Map<Integer,String> packageBusinesses=new HashMap<Integer,String>();
    public static String getBusinessForPackage(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "getBusinessForPackage(MasterDatabaseConnection,pkey)", null);
        try {
            Integer I=Integer.valueOf(pkey);
            synchronized(packageBusinesses) {
                String O=packageBusinesses.get(I);
                if(O!=null) return O;
                String business=conn.executeStringQuery("select accounting from packages where pkey=?", pkey);
                packageBusinesses.put(I, business);
                return business;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static final Map<Integer,String> packageNames=new HashMap<Integer,String>();
    public static String getNameForPackage(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "getNameForPackage(MasterDatabaseConnection,int)", null);
        try {
            Integer I=Integer.valueOf(pkey);
            synchronized(packageNames) {
                String O=packageNames.get(I);
                if(O!=null) return O;
                String name=conn.executeStringQuery("select name from packages where pkey=?", pkey);
                packageNames.put(I, name);
                return name;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private static final Map<String,Integer> packagePKeys=new HashMap<String,Integer>();
    public static int getPKeyForPackage(MasterDatabaseConnection conn, String name) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, PackageHandler.class, "getPKeyForPackage(MasterDatabaseConnection,String)", null);
        try {
            synchronized(packagePKeys) {
                Integer O=packagePKeys.get(name);
                if(O!=null) return O.intValue();
                int pkey=conn.executeIntQuery("select pkey from packages where name=?", name);
                packagePKeys.put(name, Integer.valueOf(pkey));
                return pkey;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBusinessForPackageDefinition(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "getBusinessForPackageDefinition(MasterDatabaseConnection,pkey)", null);
        try {
            return conn.executeStringQuery("select accounting from package_definitions where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getBusinessesForPackageDefinition(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "getBusinessesForPackageDefinition(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringListQuery(
                "select distinct\n"
                + "  bu.accounting\n"
                + "from\n"
                + "  packages pk,\n"
                + "  businesses bu\n"
                + "where\n"
                + "  pk.package_definition=?\n"
                + "  and pk.accounting=bu.accounting",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setPackageDefinitionActive(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        boolean isActive
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "setPackageDefinitionActive(MasterDatabaseConnection,RequestSource,InvalidateList,int,boolean)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setPackageDefinitionLimits(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String[] resources,
        int[] soft_limits,
        int[] hard_limits,
        int[] additional_rates,
        String[] additional_transaction_types
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "setPackageDefinitionLimits(MasterDatabaseConnection,RequestSource,InvalidateList,int,String[],int[],int[],int[],String[])", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removePackageDefinition(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "removePackageDefinition(MasterDatabaseConnection,RequestSource,InvalidateList,pkey)", null);
        try {
            // Security checks
            PackageHandler.checkAccessPackageDefinition(conn, source, "removePackageDefinition", pkey);

            // Do the remove
            removePackageDefinition(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removePackageDefinition(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, PackageHandler.class, "removePackageDefinition(MasterDatabaseConnection,InvalidateList,pkey)", null);
        try {
            String accounting=getBusinessForPackageDefinition(conn, pkey);
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}