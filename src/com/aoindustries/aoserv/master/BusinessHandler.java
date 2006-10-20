package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>BusinessHandler</code> handles all the accesses to the Business tables.
 *
 * @author  AO Industries, Inc.
 */
final public class BusinessHandler {

    private static final Object businessAdministratorsLock=new Object();
    private static Map<String,BusinessAdministrator> businessAdministrators;

    private static final Object usernameBusinessesLock=new Object();
    private static Map<String,List<String>> usernameBusinesses;
    private final static Map<String,Boolean> disabledBusinessAdministrators=new HashMap<String,Boolean>();
    private final static Map<String,Boolean> disabledBusinesses=new HashMap<String,Boolean>();

    public static boolean canAccessBusiness(MasterDatabaseConnection conn, RequestSource source, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "canAccessBusiness(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            String username=source.getUsername();
            return
                getAllowedBusinesses(conn, source)
                .contains(
                    UsernameHandler.getBusinessForUsername(conn, username)
                )
            ;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static boolean canAccessDisableLog(MasterDatabaseConnection conn, RequestSource source, int pkey, boolean enabling) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "canAccessDisableLog(MasterDatabaseConnection,RequestSource,int,boolean)", null);
        try {
            String username=source.getUsername();
            String disabledBy=getDisableLogDisabledBy(conn, pkey);
            if(enabling) {
                String baAccounting=UsernameHandler.getBusinessForUsername(conn, username);
                String dlAccounting=UsernameHandler.getBusinessForUsername(conn, disabledBy);
                return isBusinessOrParent(conn, baAccounting, dlAccounting);
            } else {
                return username.equals(disabledBy);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void cancelBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String cancelReason
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "cancelBusiness(MasterDatabaseConnection,RequestSource,InvalidateList,String,String)", null);
        try {
            // Check access to business
            checkAccessBusiness(conn, source, "cancelBusiness", accounting);
            
            if(accounting.equals(getRootBusiness())) throw new SQLException("Not allowed to cancel the root business: "+accounting);

            // Business must be disabled
            if(!isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to cancel Business, Business not disabled: "+accounting);
            
            // Business must not already be canceled
            if(isBusinessCanceled(conn, accounting)) throw new SQLException("Unable to cancel Business, Business already canceled: "+accounting);

            // Update the database
            conn.executeUpdate(
                "update businesses set canceled=now(), cancel_reason=? where accounting=?",
                cancelReason,
                accounting
            );
            
            // Notify the clients
            invalidateList.addTable(conn, SchemaTable.BUSINESSES, accounting, getServersForBusiness(conn, accounting), false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canControl(
        MasterDatabaseConnection conn,
        RequestSource source,
        int server,
        String process
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "canControl(MasterDatabaseConnection,RequestSource,int,String)", null);
        try {
            return conn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  bs.can_control_"+process+"\n"
                + "from\n"
                + "  usernames un,\n"
                + "  packages pk,\n"
                + "  business_servers bs\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and un.package=pk.name\n"
                + "  and pk.accounting=bs.accounting\n"
                + "  and bs.server=?",
                source.getUsername(),
                server
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessBusiness(MasterDatabaseConnection conn, RequestSource source, String action, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "checkAccessBusiness(MasterDatabaseConnection,RequestSource,String,String)", null);
        try {
            if(!canAccessBusiness(conn, source, accounting)) {
                String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access business: action='"
                +action
                +"', accounting="
                +accounting
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static void checkAccessDisableLog(MasterDatabaseConnection conn, RequestSource source, String action, int pkey, boolean enabling) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "checkAccessDisableLog(MasterDatabaseConnection,RequestSource,String,int,boolean)", null);
        try {
            if(!canAccessDisableLog(conn, source, pkey, enabling)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access disable_log: action='"
                    +action
                    +"', pkey="
                    +pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAddBusiness(MasterDatabaseConnection conn, RequestSource source, String action, String parent, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "checkAddBusiness(MasterDatabaseConnection,RequestSource,String,String,)", null);
        try {
            boolean canAdd = conn.executeBooleanQuery("select can_add_businesses from businesses where accounting=?", UsernameHandler.getBusinessForUsername(conn, source.getUsername()));
            if(canAdd) {
                MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
                if(mu!=null) {
                    if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) canAdd = false;
                } else {
                    canAdd =
                        canAccessBusiness(conn, source, parent)
                        && ServerHandler.canAccessServer(conn, source, server)
                    ;
                }
            }
            if(!canAdd) {
                String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to add business: action='"
                +action
                +"', parent="
                +parent
                +", server="
                +server
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static List<String> getAllowedBusinesses(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "getAllowedBusinesses(MasterDatabaseConnection,RequestSource)", null);
        try {
	    synchronized(usernameBusinessesLock) {
		String username=source.getUsername();
		if(usernameBusinesses==null) usernameBusinesses=new HashMap<String,List<String>>();
		List<String> SV=usernameBusinesses.get(username);
		if(SV==null) {
		    List<String> V;
                    MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
                    if(mu!=null) {
                        if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                            V=conn.executeStringListQuery(
                                "select distinct\n"
                                + "  bu.accounting\n"
                                + "from\n"
                                + "  master_servers ms,\n"
                                + "  business_servers bs,\n"
                                + "  businesses bu\n"
                                + "where\n"
                                + "  ms.username=?\n"
                                + "  and ms.server=bs.server\n"
                                + "  and bs.accounting=bu.accounting",
                                username
                            );
                        } else {
                            V=conn.executeStringListQuery("select accounting from businesses");
                        }
                    } else {
                        V=conn.executeStringListQuery(
                            "select\n"
                            + "  bu1.accounting\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + "  packages pk,\n"
                            + TableHandler.BU1_PARENTS_JOIN_NO_COMMA
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and un.package=pk.name\n"
                            + "  and (\n"
                            + TableHandler.PK_BU1_PARENTS_WHERE
                            + "  )",
                            username
                        );
                    }
                    
		    int size=V.size();
		    SV=new SortedArrayList<String>();
		    for(int c=0;c<size;c++) SV.add(V.get(c));
		    usernameBusinesses.put(username, SV);
		}
		return SV;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public static String getBusinessForDisableLog(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getBusinessForDisableLog(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select accounting from disable_log where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Creates a new <code>Business</code>.
     */
    public static void addBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String contractVersion,
        String defaultServer,
        String parent,
        boolean can_add_backup_servers,
        boolean can_add_businesses,
        boolean can_see_prices,
        boolean billParent
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addBusiness(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,String,boolean,boolean,boolean,boolean)", null);
        try {
            if(!Business.isValidAccounting(accounting)) throw new SQLException("Invalid accounting code: "+accounting);
            
            checkAddBusiness(conn, source, "addBusiness", parent, ServerHandler.getPKeyForServer(conn, defaultServer));

            if(isBusinessDisabled(conn, parent)) throw new SQLException("Unable to add Business '"+accounting+"', parent is disabled: "+parent);

            // Must not exceed the maximum business tree depth
            int newDepth=getDepthInBusinessTree(conn, parent)+1;
            if(newDepth>Business.MAXIMUM_BUSINESS_TREE_DEPTH) throw new SQLException("Unable to add Business '"+accounting+"', the maximum depth of the business tree ("+Business.MAXIMUM_BUSINESS_TREE_DEPTH+") would be exceeded.");

            conn.executeUpdate(
                "insert into businesses (\n"
                + "  accounting,\n"
                + "  contract_version,\n"
                + "  parent,\n"
                + "  can_add_backup_server,\n"
                + "  can_add_businesses,\n"
                + "  can_see_prices,\n"
                + "  auto_enable,\n"
                + "  bill_parent\n"
                + ") values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  true,\n"
                + "  ?\n"
                + ")",
                accounting,
                contractVersion,
                parent,
                can_add_backup_servers,
                can_add_businesses,
                can_see_prices,
                billParent
            );
            conn.executeUpdate(
                "insert into business_servers(\n"
                + "  accounting,\n"
                + "  server,\n"
                + "  is_default,\n"
                + "  can_configure_backup,\n"
                + "  can_control_apache,\n"
                + "  can_control_cron,\n"
                + "  can_control_interbase,\n"
                + "  can_control_mysql,\n"
                + "  can_control_postgresql,\n"
                + "  can_control_xfs,\n"
                + "  can_control_xvfb\n"
                + ") values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  true,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false,\n"
                + "  false\n"
                + ")",
                accounting,
                ServerHandler.getPKeyForServer(conn, defaultServer)
            );

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESSES, accounting, defaultServer, false);
            invalidateList.addTable(conn, SchemaTable.BUSINESS_SERVERS, accounting, defaultServer, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Creates a new <code>BusinessAdministrator</code>.
     */
    public static void addBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        String name,
        String title,
        long birthday,
        boolean isPrivate,
        String workPhone,
        String homePhone,
        String cellPhone,
        String fax,
        String email,
        String address1,
        String address2,
        String city,
        String state,
        String country,
        String zip
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addBusinessAdministrator(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,long,boolean,String,String,String,String,String,String,String,String,String,String,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "addBusinessAdministrator", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add BusinessAdministrator named mail");
            if(!BusinessAdministrator.isValidUsername(username)) throw new SQLException("Invalid BusinessAdministrator username: "+username);
            if (country!=null && country.equals(CountryCode.US)) state=convertUSState(conn, state);
            
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into business_administrators values(?,?,?,?,?,false,?,now(),?,?,?,?,?,?,?,?,?,?,?,null,true)");
            try {
                pstmt.setString(1, username);
                pstmt.setString(2, BusinessAdministrator.NO_PASSWORD);
                pstmt.setString(3, name);
                pstmt.setString(4, title);
                if(birthday==-1) pstmt.setNull(5, Types.TIMESTAMP);
                else pstmt.setTimestamp(5, new Timestamp(birthday));
                pstmt.setBoolean(6, isPrivate);
                pstmt.setString(7, workPhone);
                pstmt.setString(8, homePhone);
                pstmt.setString(9, cellPhone);
                pstmt.setString(10, fax);
                pstmt.setString(11, email);
                pstmt.setString(12, address1);
                pstmt.setString(13, address2);
                pstmt.setString(14, city);
                pstmt.setString(15, state);
                pstmt.setString(16, country);
                pstmt.setString(17, zip);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
            
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static String convertUSState(MasterDatabaseConnection conn, String state) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "convertUSState(MasterDatabaseConnection,String)", null);
        try {
            String newState = conn.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select coalesce((select code from us_states where upper(name)=upper(?) or code=upper(?)),'')",
                state,
                state
            );
            if(newState.length()==0) {
                throw new SQLException(
                    state==null || state.length()==0
                    ?"State required for the United States"
                    :"Invalid US state: "+state
                );
            }
            return newState;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Creates a new <code>BusinessProfile</code>.
     */
    public static int addBusinessProfile(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String name,
        boolean isPrivate,
        String phone,
        String fax,
        String address1,
        String address2,
        String city,
        String state,
        String country,
        String zip,
        boolean sendInvoice,
        String billingContact,
        String billingEmail,
        String technicalContact,
        String technicalEmail
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addBusinessProfile(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,boolean,String,String,String,String,String,String,String,String,boolean,String,String,String,String)", null);
        try {
            checkAccessBusiness(conn, source, "createBusinessProfile", accounting);
            
            if (country.equals(CountryCode.US)) state=convertUSState(conn, state);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('business_profiles_pkey_seq')");
            int priority=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select coalesce(max(priority)+1, 1) from business_profiles where accounting=?", accounting);
            
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into business_profiles values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            try {
                pstmt.setInt(1, pkey);
                pstmt.setString(2, accounting);
                pstmt.setInt(3, priority);
                pstmt.setString(4, name);
                pstmt.setBoolean(5, isPrivate);
                pstmt.setString(6, phone);
                pstmt.setString(7, fax);
                pstmt.setString(8, address1);
                pstmt.setString(9, address2);
                pstmt.setString(10, city);
                pstmt.setString(11, state);
                pstmt.setString(12, country);
                pstmt.setString(13, zip);
                pstmt.setBoolean(14, sendInvoice);
                pstmt.setTimestamp(15, new Timestamp(System.currentTimeMillis()));
                pstmt.setString(16, billingContact);
                pstmt.setString(17, billingEmail);
                pstmt.setString(18, technicalContact);
                pstmt.setString(19, technicalEmail);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_PROFILES, accounting, InvalidateList.allServers, false);
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Creates a new <code>BusinessServer</code>.
     */
    public static int addBusinessServer(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        int server,
        boolean can_configure_backup
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addBusinessServer(MasterDatabaseConnection,RequestSource,InvalidateList,String,int,boolean)", null);
        try {
            // Must be allowed to access the Business
            checkAccessBusiness(conn, source, "addBusinessServer", accounting);
            if(!accounting.equals(getRootBusiness())) ServerHandler.checkAccessServer(conn, source, "addBusinessServer", server);

            return addBusinessServer(conn, invalidateList, accounting, server, can_configure_backup);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Creates a new <code>BusinessServer</code>.
     */
    public static int addBusinessServer(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String accounting,
        int server,
        boolean can_configure_backup
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addBusinessServer(MasterDatabaseConnection,InvalidateList,String,int,boolean)", null);
        try {
            if(isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add BusinessServer, Business disabled: "+accounting);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('business_servers_pkey_seq')");
            
            // Parent business must also have access to the server
            if(
                !accounting.equals(getRootBusiness())
                && conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      bs.pkey\n"
                    + "    from\n"
                    + "      businesses bu,\n"
                    + "      business_servers bs\n"
                    + "    where\n"
                    + "      bu.accounting=?\n"
                    + "      and bu.parent=bs.accounting\n"
                    + "      and bs.server=?\n"
                    + "  ) is null",
                    accounting,
                    server
                )
            ) throw new SQLException("Unable to add business_server, parent does not have access to server.  accounting="+accounting+", server="+server);

            if(
                can_configure_backup
                && !accounting.equals(getRootBusiness())
                && !conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  bs.can_configure_backup\n"
                    + "from\n"
                    + "  businesses bu,\n"
                    + "  business_servers bs\n"
                    + "where\n"
                    + "  bu.accounting=?\n"
                    + "  and bu.parent=bs.accounting\n"
                    + "  and bs.server=?",
                    accounting,
                    server
                )
            ) throw new SQLException("Unable to add business_server, can_configure_backup permission requested but parent not allowed to configure backup");

            boolean hasDefault=conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select (select pkey from business_servers where accounting=? and is_default limit 1) is not null", accounting);

            conn.executeUpdate(
                "insert into business_servers values(?,?,?,?,?,false,false,false,false,false,false,false)",
                pkey,
                accounting,
                server,
                !hasDefault,
                can_configure_backup
            );
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_SERVERS, accounting, server, false);
            invalidateList.addTable(conn, SchemaTable.SERVERS, accounting, server, false);
            invalidateList.addTable(conn, SchemaTable.AO_SERVERS, accounting, server, false);
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Creates a new <code>DistroLog</code>.
     */
    public static int addDisableLog(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String disableReason
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addDisableLog(MasterDatabaseConnection,RequestSource,InvalidateList,String,String)", null);
        try {
            checkAccessBusiness(conn, source, "addDisableLog", accounting);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('disable_log_pkey_seq')");
            String username=source.getUsername();
            conn.executeUpdate(
                "insert into disable_log values(?,now(),?,?,?)",
                pkey,
                accounting,
                username,
                disableReason
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.DISABLE_LOG,
                accounting,
                InvalidateList.allServers,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a notice log.
     */
    public static void addNoticeLog(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String billingContact,
        String emailAddress,
        int balance,
        String type,
        int transid
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "addNoticeLog(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,int,String,int)", null);
        try {
            checkAccessBusiness(conn, source, "addNoticeLog", accounting);
            if(transid!=NoticeLog.NO_TRANSACTION) TransactionHandler.checkAccessTransaction(conn, source, "addNoticeLog", transid);
            
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                "insert into\n"
                + "  notice_log\n"
                + "(\n"
                + "  accounting,\n"
                + "  billing_contact,\n"
                + "  billing_email,\n"
                + "  balance,\n"
                + "  notice_type,\n"
                + "  transid\n"
                + ") values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?::decimal(9,2),\n"
                + "  ?,\n"
                + "  ?\n"
                + ")"
            );
            try {
                pstmt.setString(1, accounting);
                pstmt.setString(2, billingContact);
                pstmt.setString(3, emailAddress);
                pstmt.setString(4, SQLUtility.getDecimal(balance));
                pstmt.setString(5, type);
                if(transid==NoticeLog.NO_TRANSACTION) pstmt.setNull(6, Types.INTEGER);
                else pstmt.setInt(6, transid);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.NOTICE_LOG, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void disableBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "disableBusiness(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isBusinessDisabled(conn, accounting)) throw new SQLException("Business is already disabled: "+accounting);
            if(accounting.equals(getRootBusiness())) throw new SQLException("Not allowed to disable the root business: "+accounting);
            checkAccessDisableLog(conn, source, "disableBusiness", disableLog, false);
            checkAccessBusiness(conn, source, "disableBusiness", accounting);
            List<String> packages=getPackagesForBusiness(conn, accounting);
            for(int c=0;c<packages.size();c++) {
                String packageName=(String)packages.get(c);
                if(!PackageHandler.isPackageDisabled(conn, packageName)) {
                    throw new SQLException("Cannot disable Business '"+accounting+"': Package not disabled: "+packageName);
                }
            }

            conn.executeUpdate(
                "update businesses set disable_log=? where accounting=?",
                disableLog,
                accounting
            );

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESSES, accounting, getServersForBusiness(conn, accounting), false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void disableBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "disableBusinessAdministrator(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
            if(isBusinessAdministratorDisabled(conn, username)) throw new SQLException("BusinessAdministrator is already disabled: "+username);
            checkAccessDisableLog(conn, source, "disableBusinessAdministrator", disableLog, false);
            UsernameHandler.checkAccessUsername(conn, source, "disableBusinessAdministrator", username);

            conn.executeUpdate(
                "update business_administrators set disable_log=? where username=?",
                disableLog,
                username
            );

            // Notify all clients of the update
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            invalidateList.addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, accounting, getServersForBusiness(conn, accounting), false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "enableBusiness(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            checkAccessBusiness(conn, source, "enableBusiness", accounting);

            int disableLog=getDisableLogForBusiness(conn, accounting);
            if(disableLog==-1) throw new SQLException("Business is already enabled: "+accounting);
            checkAccessDisableLog(conn, source, "enableBusiness", disableLog, true);

            if(isBusinessCanceled(conn, accounting)) throw new SQLException("Unable to enable Business, Business canceled: "+accounting);

            conn.executeUpdate(
                "update businesses set disable_log=null where accounting=?",
                accounting
            );

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESSES, accounting, getServersForBusiness(conn, accounting), false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void enableBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "enableBusinessAdministrator(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            int disableLog=getDisableLogForBusinessAdministrator(conn, username);
            if(disableLog==-1) throw new SQLException("BusinessAdministrator is already enabled: "+username);
            checkAccessDisableLog(conn, source, "enableBusinessAdministrator", disableLog, true);
            UsernameHandler.checkAccessUsername(conn, source, "enableBusinessAdministrator", username);

            conn.executeUpdate(
                "update business_administrators set disable_log=null where username=?",
                username
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.BUSINESS_ADMINISTRATORS,
                UsernameHandler.getBusinessForUsername(conn, username),
                UsernameHandler.getServersForUsername(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String generateAccountingCode(
        MasterDatabaseConnection conn,
        String template
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "generateAccountingCode(MasterDatabaseConnection,String)", null);
        try {
            // Load the entire list of accounting codes
            List<String> codes=conn.executeStringListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select accounting from businesses");
            int size=codes.size();
            
            // Sort them
            List<String> sorted=new SortedArrayList<String>(size);
            for(int c=0;c<size;c++) sorted.add(codes.get(c));
            
            // Find one that is not used
            String goodOne=null;
            for(int c=1;c<Integer.MAX_VALUE;c++) {
                String accounting=template+c;
                if(!Business.isValidAccounting(accounting)) throw new SQLException("Invalid accounting code: "+accounting);
                if(!sorted.contains(accounting)) {
                    goodOne=accounting;
                    break;
                }
            }
            
            // If could not find one, report and error
            if(goodOne==null) throw new SQLException("Unable to find available accounting code for template: "+template);
            
            // Write the one we found
            return goodOne;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDepthInBusinessTree(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getDepthInBusinessTree(MasterDatabaseConnection,String)", null);
        try {
            int depth=0;
            while(accounting!=null) {
                String parent=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select parent from businesses where accounting=?", accounting);
                depth++;
                accounting=parent;
            }
            return depth;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getDisableLogDisabledBy(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getDisableLogDisabledBy(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select disabled_by from disable_log where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getDisableLogForBusiness(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getDisableLogForBusiness(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select coalesce(disable_log, -1) from businesses where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static Map<String,Integer> businessAdministratorDisableLogs=new HashMap<String,Integer>();
    public static int getDisableLogForBusinessAdministrator(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getDisableLogForBusinessAdministrator(MasterDatabaseConnection,String)", null);
        try {
            synchronized(businessAdministratorDisableLogs) {
                if(businessAdministratorDisableLogs.containsKey(username)) return businessAdministratorDisableLogs.get(username).intValue();
                int disableLog=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select coalesce(disable_log, -1) from business_administrators where username=?", username);
                businessAdministratorDisableLogs.put(username, Integer.valueOf(disableLog));
                return disableLog;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getPackagesForBusiness(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getPackagesForBusiness(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select name from packages where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getServersForBusiness(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getServersForBusiness(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select server from business_servers where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getRootBusiness() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BusinessHandler.class, "getRootBusiness()", null);
        try {
            return MasterConfiguration.getRootBusiness();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public static boolean isAccountingAvailable(
        MasterDatabaseConnection conn,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isAccountingAvailable(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select count(*) from businesses where accounting=?", accounting)==0;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isBusinessAdministratorPasswordSet(
        MasterDatabaseConnection conn,
        RequestSource source,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isBusinessAdministratorPasswordSet(MasterDatabaseConnection,RequestSource,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "isBusinessAdministratorPasswordSet", username);
            return !BusinessAdministrator.NO_PASSWORD.equals(
                conn
                    .executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select password from business_administrators where username=?", username)
                    .trim()
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "removeBusinessAdministrator(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            if(username.equals(source.getUsername())) throw new SQLException("Not allowed to remove self: "+username);
            UsernameHandler.checkAccessUsername(conn, source, "removeBusinessAdministrator", username);

            removeBusinessAdministrator(conn, invalidateList, username);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void removeBusinessAdministrator(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "removeBusinessAdministrator(MasterDatabaseConnection,InvalidateList,String)", null);
        try {
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove Username named '"+LinuxAccount.MAIL+'\'');

            String accounting=UsernameHandler.getBusinessForUsername(conn, username);

            conn.executeUpdate("delete from business_administrators where username=?", username);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a <code>BusinessServer</code>.
     */
    public static void removeBusinessServer(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "removeBusinessServer(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            String accounting=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select accounting from business_servers where pkey=?", pkey);
            int server=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select server from business_servers where pkey=?", pkey);
            
            // Must be allowed to access this Business
            checkAccessBusiness(conn, source, "removeBusinessServer", accounting);
            
            // Do not remove the default unless it is the only one left
            if(
                conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select is_default from business_servers where pkey=?", pkey)
                && conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select count(*) from business_servers where accounting=?", accounting)>1
            ) {
                throw new SQLException("Cannot remove the default business_server unless it is the last business_server for a business: "+pkey);
            }

            removeBusinessServer(
                conn,
                invalidateList,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a <code>BusinessServer</code>.
     */
    public static void removeBusinessServer(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "removeBusinessServer(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select accounting from business_servers where pkey=?", pkey);
            int server=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select server from business_servers where pkey=?", pkey);

            // No children should be able to access the server
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      bs.pkey\n"
                    + "    from\n"
                    + "      businesses bu,\n"
                    + "      business_servers bs\n"
                    + "    where\n"
                    + "      bu.parent=?\n"
                    + "      and bu.accounting=bs.accounting\n"
                    + "      and bs.server=?\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still has at least one child Business able to access Server="+server);
            
            /*
             * Business must not have any resources on the server
             */
            // email_pipes
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      ep.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      email_pipes ep\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=ep.package\n"
                    + "      and ep.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one EmailPipe on Server="+server);

            // httpd_sites
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      hs.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      httpd_sites hs\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=hs.package\n"
                    + "      and hs.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one HttpdSite on Server="+server);

            // ip_addresses
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      ia.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      ip_addresses ia,\n"
                    + "      net_devices nd\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=ia.package\n"
                    + "      and ia.net_device=nd.pkey\n"
                    + "      and nd.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one IPAddress on Server="+server);

            // linux_server_accounts
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      lsa.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      usernames un,\n"
                    + "      linux_server_accounts lsa\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=un.package\n"
                    + "      and un.username=lsa.username\n"
                    + "      and lsa.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one LinuxServerAccount on Server="+server);

            // linux_server_groups
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      lsg.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      linux_groups lg,\n"
                    + "      linux_server_groups lsg\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=lg.package\n"
                    + "      and lg.name=lsg.name\n"
                    + "      and lsg.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one LinuxServerGroup on Server="+server);

            // mysql_databases
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      md.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      mysql_databases md,\n"
                    + "      mysql_servers ms\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=md.package\n"
                    + "      and md.mysql_server=ms.pkey\n"
                    + "      and ms.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one MySQLDatabase on Server="+server);

            // mysql_server_users
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      msu.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      usernames un,\n"
                    + "      mysql_server_users msu,\n"
                    + "      mysql_servers ms\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=un.package\n"
                    + "      and un.username=msu.username\n"
                    + "      and msu.mysql_server=ms.pkey\n"
                    + "      and ms.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one MySQLServerUser on Server="+server);

            // net_binds
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      nb.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      net_binds nb\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=nb.package\n"
                    + "      and nb.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one NetBind on Server="+server);

            // postgres_databases
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      pd.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      usernames un,\n"
                    + "      postgres_servers ps,\n"
                    + "      postgres_server_users psu,\n"
                    + "      postgres_databases pd\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=un.package\n"
                    + "      and ps.ao_server=?\n"
                    + "      and un.username=psu.username and ps.pkey=psu.postgres_server\n"
                    + "      and pd.datdba=psu.pkey\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one PostgresDatabase on Server="+server);

            // postgres_server_users
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      psu.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      usernames un,\n"
                    + "      postgres_servers ps,\n"
                    + "      postgres_server_users psu\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=un.package\n"
                    + "      and ps.ao_server=?\n"
                    + "      and un.username=psu.username and ps.pkey=psu.postgres_server\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one PostgresServerUser on Server="+server);

            // email_domains
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      ed.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      email_domains ed\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=ed.package\n"
                    + "      and ed.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one EmailDomain on Server="+server);

            // email_smtp_relays
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      esr.pkey\n"
                    + "    from\n"
                    + "      packages pk,\n"
                    + "      email_smtp_relays esr\n"
                    + "    where\n"
                    + "      pk.accounting=?\n"
                    + "      and pk.name=esr.package\n"
                    + "      and esr.ao_server is not null\n"
                    + "      and esr.ao_server=?\n"
                    + "    limit 1\n"
                    + "  )\n"
                    + "  is not null\n",
                    accounting,
                    server
                )
            ) throw new SQLException("Business="+accounting+" still owns at least one EmailSmtpRelay on Server="+server);
            
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("delete from business_servers where pkey=?");
            try {
                pstmt.setInt(1, pkey);
                conn.incrementUpdateCount();
                pstmt.executeUpdate();
            } finally {
                pstmt.close();
            }
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_SERVERS, accounting, server, false);
            invalidateList.addTable(conn, SchemaTable.SERVERS, accounting, server, false);
            invalidateList.addTable(conn, SchemaTable.AO_SERVERS, accounting, server, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeDisableLog(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "removeDisableLog(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            String accounting=getBusinessForDisableLog(conn, pkey);

            conn.executeUpdate("delete from disable_log where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.DISABLE_LOG, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setBusinessAccounting(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String oldAccounting,
        String newAccounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "setBusinessAccounting(MasterDatabaseConnection,RequestSource,InvalidateList,String,String)", null);
        try {
            checkAccessBusiness(conn, source, "setBusinessAccounting", oldAccounting);
            if(!Business.isValidAccounting(newAccounting)) throw new SQLException("Invalid accounting code: "+newAccounting);

            conn.executeUpdate("update businesses set accounting=? where accounting=?", newAccounting, oldAccounting);

            // Notify all clients of the update
            Collection<String> accts=InvalidateList.getCollection(oldAccounting, newAccounting);
            invalidateList.addTable(conn, SchemaTable.BUSINESSES, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.BUSINESS_PROFILES, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.BUSINESS_SERVERS, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.CREDIT_CARDS, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.DISABLE_LOG, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.MONTHLY_CHARGES, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.NOTICE_LOG, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.PACKAGE_DEFINITIONS, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.PACKAGES, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.SERVERS, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.TICKETS, accts, InvalidateList.allServers, false);
            invalidateList.addTable(conn, SchemaTable.TRANSACTIONS, accts, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setBusinessAdministratorPassword(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        String plaintext
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "setBusinessAdministratorPassword(MasterDatabaseConnection,RequestSource,InvalidateList,String,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "setBusinessAdministratorPassword", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set password for BusinessAdministrator named '"+LinuxAccount.MAIL+'\'');

            if(isBusinessAdministratorDisabled(conn, username)) throw new SQLException("Unable to set password, BusinessAdministrator disabled: "+username);

            if(plaintext!=null && plaintext.length()>0) {
                // Perform the password check here, too.
                String reason=BusinessAdministrator.checkPasswordDescribe(username, plaintext);
                if(reason!=null) throw new SQLException("Invalid password: "+reason.replace('\n', '|'));
            }

            String encrypted=plaintext==null || plaintext.length()==0?BusinessAdministrator.NO_PASSWORD:UnixCrypt.crypt(plaintext);
            
            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            conn.executeUpdate("update business_administrators set password=? where username=?", encrypted, username);
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Sets a business_administrators profile.
     */
    public static void setBusinessAdministratorProfile(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username,
        String name,
        String title,
        long birthday,
        boolean isPrivate,
        String workPhone,
        String homePhone,
        String cellPhone,
        String fax,
        String email,
        String address1,
        String address2,
        String city,
        String state,
        String country,
        String zip
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "setBusinessAdministratorProfile(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,long,boolean,String,String,String,String,String,String,String,String,String,String,String)", null);
        
        try {
            UsernameHandler.checkAccessUsername(conn, source, "setBusinessSdministratorProfile", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set BusinessAdministrator profile for user '"+LinuxAccount.MAIL+'\'');
            
            if(!EmailAddress.isValidEmailAddress(email)) throw new SQLException("Invalid format for email: "+email);
            
            if (country!=null && country.equals(CountryCode.US)) state=convertUSState(conn, state);

            String accounting=UsernameHandler.getBusinessForUsername(conn, username);
            conn.executeUpdate(
                "update business_administrators set name=?, title=?, birthday=?, private=?, work_phone=?, home_phone=?, cell_phone=?, fax=?, email=?, address1=?, address2=?, city=?, state=?, country=?, zip=? where username=?",
                name,
                title,
                birthday==-1?null:SQLUtility.getDate(birthday),
                isPrivate,
                workPhone,
                homePhone,
                cellPhone,
                fax,
                email,
                address1,
                address2,
                city,
                state,
                country,
                zip,
                username
            );
            
            // Notify all clients of the update
            invalidateList.addTable(conn, SchemaTable.BUSINESS_ADMINISTRATORS, accounting, InvalidateList.allServers, false);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Sets the default Server for a Business
     */
    public static void setDefaultBusinessServer(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "setDefaultBusinessServer(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            String accounting=conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select accounting from business_servers where pkey=?", pkey);
            
            checkAccessBusiness(conn, source, "setDefaultBusinessServer", accounting);

            if(isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to set the default BusinessServer, Business disabled: "+accounting);

            // Update the table
            conn.executeUpdate(
                "update business_servers set is_default=true where pkey=?",
                pkey
            );
            conn.executeUpdate(
                "update business_servers set is_default=false where accounting=? and pkey!=?",
                accounting,
                pkey
            );
            
            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.BUSINESS_SERVERS,
                accounting,
                InvalidateList.allServers,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static BusinessAdministrator getBusinessAdministrator(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getBusinessAdministrator(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(businessAdministratorsLock) {
		if(businessAdministrators==null) {
		    Statement stmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
		    try {
			Map<String,BusinessAdministrator> table=new HashMap<String,BusinessAdministrator>();
			conn.incrementQueryCount();
			ResultSet results=stmt.executeQuery("select * from business_administrators");
			while(results.next()) {
			    BusinessAdministrator ba=new BusinessAdministrator();
			    ba.init(results);
			    table.put(results.getString(1), ba);
			}
			businessAdministrators=table;
		    } finally {
			stmt.close();
		    }
		}
		return (BusinessAdministrator)businessAdministrators.get(username);
	    }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "invalidateTable(int)", null);
        try {
            if(tableID==SchemaTable.BUSINESS_ADMINISTRATORS) {
                synchronized(businessAdministratorsLock) {
                    businessAdministrators=null;
                }
                synchronized(disabledBusinessAdministrators) {
                    disabledBusinessAdministrators.clear();
                }
                synchronized(businessAdministratorDisableLogs) {
                    businessAdministratorDisableLogs.clear();
                }
            } else if(tableID==SchemaTable.BUSINESSES) {
                synchronized(usernameBusinessesLock) {
                    usernameBusinesses=null;
                }
                synchronized(disabledBusinesses) {
                    disabledBusinesses.clear();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getParentBusiness(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getParentBusiness(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select parent from businesses where accounting=?",
                accounting
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getTechnicalEmail(MasterDatabaseConnection conn, String accountingCode) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getTechnicalEmail(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select technical_email from business_profiles where accounting=? order by priority desc limit 1",
                accountingCode
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
 
    public static boolean isBusinessAdministrator(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isBusinessAdministrator(MasterDatabaseConnection,String)", null);
        try {
            return getBusinessAdministrator(conn, username)!=null;
            //return conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, "select (select username from business_administrators where username=? limit 1) is not null", username);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isBusinessAdministratorDisabled(MasterDatabaseConnection conn, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "isBusinessAdministratorDisabled(MasterDatabaseConnection,String)", null);
        try {
            Boolean O;
	    synchronized(disabledBusinessAdministrators) {
		O=disabledBusinessAdministrators.get(username);
            }
            if(O!=null) return O.booleanValue();
            boolean isDisabled=getDisableLogForBusinessAdministrator(conn, username)!=-1;
            synchronized(disabledBusinessAdministrators) {
                disabledBusinessAdministrators.put(username, isDisabled);
            }
            return isDisabled;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isBusinessDisabled(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "isBusinessDisabled(MasterDatabaseConnection,String)", null);
        try {
	    synchronized(disabledBusinesses) {
		Boolean O=disabledBusinesses.get(accounting);
		if(O!=null) return O.booleanValue();
		boolean isDisabled=getDisableLogForBusiness(conn, accounting)!=-1;
		disabledBusinesses.put(accounting, isDisabled);
		return isDisabled;
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isBusinessCanceled(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isBusinessCanceled(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select canceled is not null from businesses where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isBusinessBillParent(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isBusinessBillParent(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select bill_parent from businesses where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canSeePrices(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "canSeePrices(MasterDatabaseConnection,RequestSource)", null);
        try {
            return canSeePrices(conn, UsernameHandler.getBusinessForUsername(conn, source.getUsername()));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean canSeePrices(MasterDatabaseConnection conn, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "canSeePrices(MasterDatabaseConnection,String)", null);
        try {
            return conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select can_see_prices from businesses where accounting=?", accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isBusinessOrParent(MasterDatabaseConnection conn, String parentAccounting, String accounting) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "isBusinessOrParent(MasterDatabaseConnection,String,String)", null);
        try {
            return conn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select is_business_or_parent(?,?)", parentAccounting, accounting);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canSwitchUser(MasterDatabaseConnection conn, String authenticatedAs, String connectAs) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "canSwitchUser(MasterDatabaseConnection,String,String)", null);
        try {
            String authAccounting=UsernameHandler.getBusinessForUsername(conn, authenticatedAs);
            String connectAccounting=UsernameHandler.getBusinessForUsername(conn, connectAs);
            // Cannot switch within same business
            if(authAccounting.equals(connectAccounting)) return false;
            return conn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  (select can_switch_users from business_administrators where username=?)\n"
                + "  and is_business_or_parent(?,?)",
                authenticatedAs,
                authAccounting,
                connectAccounting
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the list of both technical and billing contacts for all not-canceled businesses.
     *
     * @return  a <code>HashMap</code> of <code>ArrayList</code>
     */
    public static Map<String,List<String>> getBusinessContacts(MasterDatabaseConnection conn) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getBusinessContacts(MasterDatabaseConnection)", null);
        try {
            // Load the list of businesses and their contacts
            Map<String,List<String>> businessContacts=new HashMap<String,List<String>>();
            List<String> foundAddresses=new SortedArrayList<String>();
            PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement("select bp.accounting, bp.billing_email, bp.technical_email from business_profiles bp, businesses bu where bp.accounting=bu.accounting and bu.canceled is null order by bp.accounting, bp.priority desc");
            try {
                ResultSet results=pstmt.executeQuery();
                try {
                    while(results.next()) {
                        String accounting=results.getString(1);
                        if(!businessContacts.containsKey(accounting)) {
                            List<String> uniqueAddresses=new ArrayList<String>();
                            foundAddresses.clear();
                            // billing contacts
                            List<String> addresses=StringUtility.splitStringCommaSpace(results.getString(2));
                            for(int c=0;c<addresses.size();c++) {
                                String addy=addresses.get(c).toLowerCase();
                                if(!foundAddresses.contains(addy)) {
                                    uniqueAddresses.add(addy);
                                    foundAddresses.add(addy);
                                }
                            }
                            // technical contacts
                            addresses=StringUtility.splitStringCommaSpace(results.getString(3));
                            for(int c=0;c<addresses.size();c++) {
                                String addy=addresses.get(c).toLowerCase();
                                if(!foundAddresses.contains(addy)) {
                                    uniqueAddresses.add(addy);
                                    foundAddresses.add(addy);
                                }
                            }
                            businessContacts.put(accounting, uniqueAddresses);
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                throw new WrappedSQLException(err, pstmt);
            } finally {
                pstmt.close();
            }
            return businessContacts;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets the best estimate of a business for a list of email addresses or <code>null</code> if can't determine.
     * The algorithm takes these steps.
     * <OL>
     *   <LI>Look for exact matches in billing and technical contacts, with a weight of 10.</LI>
     *   <LI>Look for matches in email_domains, with a weight of 5</LI>
     *   <LI>Look for matches in httpd_site_urls with a weight of 1</LI>
     *   <LI>Look for matches in dns_zones with a weight of 1</LI>
     *   <LI>Add up the weights per business</LI>
     *   <LI>Find the highest weight</LI>
     *   <LI>Follow the bill_parents up to top billing level</LI>
     * </OL>
     */
    public static String getBusinessFromEmailAddresses(MasterDatabaseConnection conn, List<String> addresses) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "getBusinessFromEmailAddresses(MasterDatabaseConnection,List<String>)", null);
        try {
            // Load the list of businesses and their contacts
            Map<String,List<String>> businessContacts=getBusinessContacts(conn);

            // The cumulative weights are added up here, per business
            Map<String,Integer> businessWeights=new HashMap<String,Integer>();
            
            // Go through all addresses
            for(int c=0;c<addresses.size();c++) {
                String address=addresses.get(c).toLowerCase();
                // Look for billing and technical contact matches, 10 points each
                Iterator<String> I=businessContacts.keySet().iterator();
                while(I.hasNext()) {
                    String accounting=I.next();
                    List<String> list=businessContacts.get(accounting);
                    for(int d=0;d<list.size();d++) {
                        String contact=(String)list.get(d);
                        if(address.equals(contact)) addWeight(businessWeights, accounting, 10);
                    }
                }
                
                // Parse the domain
                int pos=address.lastIndexOf('@');
                if(pos!=-1) {
                    String domain=address.substring(pos+1);
                    if(domain.length()>0) {
                        // Look for matches in email_domains, 5 points each
                        List<String> domains=conn.executeStringListQuery(
                            Connection.TRANSACTION_READ_COMMITTED,
                            true,
                            "select\n"
                            + "  pk.accounting\n"
                            + "from\n"
                            + "  email_domains ed,\n"
                            + "  packages pk\n"
                            + "where\n"
                            + "  ed.domain=?\n"
                            + "  and ed.package=pk.name",
                            domain
                        );
                        for(int d=0;d<domains.size();d++) {
                            String accounting=domains.get(d);
                            addWeight(businessWeights, accounting, 5);
                        }
                        // Look for matches in httpd_site_urls, 1 point each
                        List<String> sites=conn.executeStringListQuery(
                            Connection.TRANSACTION_READ_COMMITTED,
                            true,
                            "select\n"
                            + "  pk.accounting\n"
                            + "from\n"
                            + "  httpd_site_urls hsu,\n"
                            + "  httpd_site_binds hsb,\n"
                            + "  httpd_sites hs,\n"
                            + "  packages pk\n"
                            + "where\n"
                            + "  hsu.hostname=?\n"
                            + "  and hsu.httpd_site_bind=hsb.pkey\n"
                            + "  and hsb.httpd_site=hs.pkey\n"
                            + "  and hs.package=pk.name",
                            domain
                        );
                        for(int d=0;d<sites.size();d++) {
                            String accounting=sites.get(d);
                            addWeight(businessWeights, accounting, 1);
                        }
                        // Look for matches in dns_zones, 1 point each
                        List<String> zones=conn.executeStringListQuery(
                            Connection.TRANSACTION_READ_COMMITTED,
                            true,
                            "select\n"
                            + "  pk.accounting\n"
                            + "from\n"
                            + "  dns_zones dz,\n"
                            + "  packages pk\n"
                            + "where\n"
                            + "  dz.zone=?\n"
                            + "  and dz.package=pk.name",
                            domain
                        );
                        for(int d=0;d<zones.size();d++) {
                            String accounting=zones.get(d);
                            addWeight(businessWeights, accounting, 1);
                        }
                    }
                }
            }

            // Find the highest weight
            Iterator<String> I=businessWeights.keySet().iterator();
            int highest=0;
            String highestAccounting=null;
            while(I.hasNext()) {
                String accounting=I.next();
                int weight=businessWeights.get(accounting).intValue();
                if(weight>highest) {
                    highest=weight;
                    highestAccounting=accounting;
                }
            }

            // Follow the bill_parent flags toward the top, but skipping canceled
            while(
                highestAccounting!=null
                && (
                    isBusinessCanceled(conn, highestAccounting)
                    || isBusinessBillParent(conn, highestAccounting)
                )
            ) {
                highestAccounting=getParentBusiness(conn, highestAccounting);
            }

            // Do not accept root business
            if(highestAccounting!=null && highestAccounting.equals(getRootBusiness())) highestAccounting=null;
            
            // Return result
            return highestAccounting;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static void addWeight(Map<String,Integer> businessWeights, String accounting, int weight) {
        Profiler.startProfile(Profiler.FAST, BusinessHandler.class, "addWeight(Map<String,Integer>,String,int)", null);
        try {
            Integer I=businessWeights.get(accounting);
            int previous=I==null ? 0 : I.intValue();
            businessWeights.put(accounting, Integer.valueOf(previous + weight));
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean canBusinessAccessServer(MasterDatabaseConnection conn, String accounting, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "canBusinessAccessServer(MasterDatabaseConnection,String,int)", null);
        try {
            return conn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      pkey\n"
                + "    from\n"
                + "      business_servers\n"
                + "    where\n"
                + "      accounting=?\n"
                + "      and server=?\n"
                + "    limit 1\n"
                + "  )\n"
                + "  is not null\n",
                accounting,
                server
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static void checkBusinessAccessServer(MasterDatabaseConnection conn, RequestSource source, String action, String accounting, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BusinessHandler.class, "checkBusinessAccessServer(MasterDatabaseConnection,RequestSource,String,String,int)", null);
        try {
            if(!canBusinessAccessServer(conn, accounting, server)) {
                String message=
                "accounting="
                +accounting
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
}