package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.ResourceType;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * The <code>PackageHandler</code> handles all the accesses to the <code>package_definitions</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class PackageHandler {

    private PackageHandler() {
    }

    public static boolean canAccessPackageDefinition(DatabaseConnection conn, RequestSource source, int pkey) throws IOException, SQLException {
        return BusinessHandler.canAccessBusiness(conn, source, getBusinessForPackageDefinition(conn, pkey));
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
     * Creates a new <code>PackageDefinition</code>.
     */
    public static int addPackageDefinition(
        DatabaseConnection conn,
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
            + "  resource_type,\n"
            + "  soft_limit,\n"
            + "  hard_limit,\n"
            + "  additional_rate,\n"
            + "  additional_transaction_type\n"
            + ") select\n"
            + "  ?,\n"
            + "  resource_type,\n"
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

    public static int findActivePackageDefinition(DatabaseConnection conn, String accounting, int rate, int userLimit, int emailLimit) throws IOException, SQLException {
        return conn.executeIntQuery(
            "select\n"
            + "  coalesce(\n"
            + "    (\n"
            + "      select\n"
            + "        pd.pkey\n"
            + "      from\n"
            + "        package_definitions pd,\n"
            + "        package_definitions_limits user_pdl,\n"
            + "        package_definitions_limits email_pdl\n"
            + "      where\n"
            + "        pd.accounting=?\n"
            + "        and pd.monthly_rate=?\n"
            + "        and pd.pkey=user_pdl.package_definition\n"
            + "        and user_pdl.resource=?\n"
            + "        and pd.pkey=email_pdl.package_definition\n"
            + "        and email_pdl.resource=?\n"
            + "      limit 1\n"
            + "    ), -1\n"
            + "  )",
            accounting,
            SQLUtility.getDecimal(rate),
            ResourceType.USER,
            ResourceType.EMAIL
        );
    }

    public static boolean isPackageDefinitionApproved(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        return conn.executeBooleanQuery("select approved from package_definitions where pkey=?", packageDefinition);
    }

    public static boolean isPackageDefinitionActive(DatabaseConnection conn, int packageDefinition) throws IOException, SQLException {
        return conn.executeBooleanQuery("select active from package_definitions where pkey=?", packageDefinition);
    }

    public static String getBusinessForPackageDefinition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select accounting from package_definitions where pkey=?", pkey);
    }

    public static List<String> getBusinessesForPackageDefinition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringListQuery("select accounting from businesses where package_definition=?", pkey);
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
        String[] resourceTypes,
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
        for(int c=0;c<resourceTypes.length;c++) {
            conn.executeUpdate(

                "insert into\n"
                + "  package_definition_limits\n"
                + "(\n"
                + "  package_definition,\n"
                + "  resource_type,\n"
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
                resourceTypes[c],
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
    }
}