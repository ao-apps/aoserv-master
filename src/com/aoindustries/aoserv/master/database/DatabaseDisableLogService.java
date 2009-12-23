package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.DisableLog;
import com.aoindustries.aoserv.client.DisableLogService;
import com.aoindustries.security.AccountDisabledException;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDisableLogService extends DatabaseServiceIntegerKey<DisableLog> implements DisableLogService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DisableLog> objectFactory = new AutoObjectFactory<DisableLog>(DisableLog.class, this);

    DatabaseDisableLogService(DatabaseConnector connector) {
        super(connector, DisableLog.class);
    }

    @Override
    public Set<DisableLog> getSet() throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select * from disable_log"
                        )
                    );
                case DAEMON :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select distinct\n"
                            + "  dl.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  ao_servers ao\n"
                            + "  left join ao_servers ff on ao.server=ff.failover_server,\n"
                            + "  business_servers bs,\n"
                            + "  disable_log dl\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=ao.server\n"
                            + "  and (\n"
                            + "    ao.server=bs.server\n"
                            + "    or ff.server=bs.server\n"
                            + "  ) and bs.accounting=dl.accounting",
                            connector.getConnectAs()
                        )
                    );
                case BUSINESS :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select\n"
                            + "  dl.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + BU1_PARENTS_JOIN
                            + "  disable_log dl\n"
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and (\n"
                            + UN_BU1_PARENTS_WHERE
                            + "  )\n"
                            + "  and bu1.accounting=dl.accounting",
                            connector.getConnectAs()
                        )
                    );
                case DISABLED : throw new RemoteException(null, new AccountDisabledException());
                default : throw new AssertionError();
            }
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
}
