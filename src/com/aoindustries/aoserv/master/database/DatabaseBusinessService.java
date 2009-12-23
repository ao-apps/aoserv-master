package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.BusinessService;
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
final class DatabaseBusinessService extends DatabaseServiceStringKey<Business> implements BusinessService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Business> objectFactory = new AutoObjectFactory<Business>(Business.class, this);

    DatabaseBusinessService(DatabaseConnector connector) {
        super(connector, Business.class);
    }

    @Override
    public Set<Business> getSet() throws RemoteException {
        try {
            switch(connector.getAccountType()) {
                case MASTER :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select * from businesses"
                        )
                    );
                case DAEMON :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select distinct\n"
                            + "  bu.*\n"
                            + "from\n"
                            + "  master_servers ms,\n"
                            + "  business_servers bs,\n"
                            + "  businesses bu\n"
                            + "where\n"
                            + "  ms.username=?\n"
                            + "  and ms.server=bs.server\n"
                            + "  and bs.accounting=bu.accounting",
                            connector.getConnectAs()
                        )
                    );
                case BUSINESS :
                    return Collections.unmodifiableSet(
                        connector.factory.database.executeObjectSetQuery(
                            objectFactory,
                            "select\n"
                            + "  bu1.*\n"
                            + "from\n"
                            + "  usernames un,\n"
                            + BU1_PARENTS_JOIN_NO_COMMA
                            + "where\n"
                            + "  un.username=?\n"
                            + "  and (\n"
                            + UN_BU1_PARENTS_WHERE
                            + "  )",
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
