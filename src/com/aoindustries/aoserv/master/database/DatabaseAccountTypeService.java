/*
 * Copyright 2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.sql.DatabaseConnection;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Provides different implementations based on account type.  Also checks
 * for disabled account in the process.
 *
 * @author  AO Industries, Inc.
 */
abstract class DatabaseAccountTypeService<
    K extends Comparable<K>,
    V extends AOServObject<K>
> extends DatabaseService<K,V> {

    // <editor-fold defaultstate="collapsed" desc="Business Tree Joins">
    /**
     * The joins used for the business tree.
     */
    static final String
        BU1_PARENTS_JOIN=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting,\n",
        BU1_PARENTS_JOIN_NO_COMMA=
              "  businesses bu1\n"
            + "  left join businesses bu2 on bu1.parent=bu2.accounting\n"
            + "  left join businesses bu3 on bu2.parent=bu3.accounting\n"
            + "  left join businesses bu4 on bu3.parent=bu4.accounting\n"
            + "  left join businesses bu5 on bu4.parent=bu5.accounting\n"
            + "  left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+" on bu5.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".accounting\n",
        BU2_PARENTS_JOIN=
              "      businesses bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+"\n"
            + "      left join businesses bu8 on bu7.parent=bu8.accounting\n"
            + "      left join businesses bu9 on bu8.parent=bu9.accounting\n"
            + "      left join businesses bu10 on bu9.parent=bu10.accounting\n"
            + "      left join businesses bu11 on bu10.parent=bu11.accounting\n"
            + "      left join businesses bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+" on bu11.parent=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".accounting,\n"
    ;

    /**
     * The where clauses that accompany the joins.
     */
    static final String
        UN_BU1_PARENTS_WHERE=
              "    un.accounting=bu1.accounting\n"
            + "    or un.accounting=bu1.parent\n"
            + "    or un.accounting=bu2.parent\n"
            + "    or un.accounting=bu3.parent\n"
            + "    or un.accounting=bu4.parent\n"
            + "    or un.accounting=bu5.parent\n"
            + "    or un.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN1_BU1_PARENTS_OR_WHERE=
              "    or un1.accounting=bu1.accounting\n"
            + "    or un1.accounting=bu1.parent\n"
            + "    or un1.accounting=bu2.parent\n"
            + "    or un1.accounting=bu3.parent\n"
            + "    or un1.accounting=bu4.parent\n"
            + "    or un1.accounting=bu5.parent\n"
            + "    or un1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN1_BU1_PARENTS_WHERE=
              "    un1.accounting=bu1.accounting\n"
            + "    or un1.accounting=bu1.parent\n"
            + "    or un1.accounting=bu2.parent\n"
            + "    or un1.accounting=bu3.parent\n"
            + "    or un1.accounting=bu4.parent\n"
            + "    or un1.accounting=bu5.parent\n"
            + "    or un1.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH-1)+".parent\n",
        UN2_BU2_PARENTS_OR_WHERE=
              "        or un2.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or un2.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or un2.accounting=bu8.parent\n"
            + "        or un2.accounting=bu9.parent\n"
            + "        or un2.accounting=bu10.parent\n"
            + "        or un2.accounting=bu11.parent\n"
            + "        or un2.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n",
        UN3_BU2_PARENTS_WHERE=
              "        un3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting\n"
            + "        or un3.accounting=bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".parent\n"
            + "        or un3.accounting=bu8.parent\n"
            + "        or un3.accounting=bu9.parent\n"
            + "        or un3.accounting=bu10.parent\n"
            + "        or un3.accounting=bu11.parent\n"
            + "        or un3.accounting=bu"+(Business.MAXIMUM_BUSINESS_TREE_DEPTH*2-2)+".parent\n"
    ;
    // </editor-fold>

    DatabaseAccountTypeService(DatabaseConnector connector, Class<K> keyClass, Class<V> valueClass) {
        super(connector, keyClass, valueClass);
    }

    @Override
    final protected ArrayList<V> getList(DatabaseConnection db) throws RemoteException, SQLException {
        switch(connector.getAccountType(db)) {
            case MASTER : return getListMaster(db);
            case DAEMON : return getListDaemon(db);
            case BUSINESS : return getListBusiness(db);
            case DISABLED : throw new SQLException(new AccountDisabledException());
            default : throw new AssertionError();
        }
    }

    /**
     * Gets the unfiltered set.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListMaster(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the set filtered by server access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListDaemon(DatabaseConnection db) throws RemoteException, SQLException;

    /**
     * Gets the sets filtered by business access.
     * The return value will be automatically wrapped in AOServServiceUtils.unmodifiableSet.
     */
    abstract protected ArrayList<V> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException;
}
