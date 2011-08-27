/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetTcpRedirectService extends DatabaseAccountTypeService<Integer,NetTcpRedirect> implements NetTcpRedirectService {

    private final ObjectFactory<NetTcpRedirect> objectFactory = new AutoObjectFactory<NetTcpRedirect>(NetTcpRedirect.class, connector);

    DatabaseNetTcpRedirectService(DatabaseConnector connector) {
        super(connector, Integer.class, NetTcpRedirect.class);
    }

    @Override
    protected List<NetTcpRedirect> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<NetTcpRedirect>(),
            objectFactory,
            "select * from net_tcp_redirects"
        );
    }

    @Override
    protected List<NetTcpRedirect> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<NetTcpRedirect>(),
            objectFactory,
            "select\n"
            + "  ntr.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  net_binds nb,\n"
            + "  net_tcp_redirects ntr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=nb.server\n"
            + "  and nb.pkey=ntr.net_bind",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<NetTcpRedirect> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<NetTcpRedirect>(),
            objectFactory,
            "select\n"
            + "  ntr.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  net_binds nb,\n"
            + "  net_tcp_redirects ntr\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=nb.accounting\n"
            + "  and nb.pkey=ntr.net_bind",
            connector.getSwitchUser()
        );
    }
}
