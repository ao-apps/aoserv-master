package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.LinuxGroupType;
import com.aoindustries.aoserv.client.LinuxGroupTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxGroupTypeService extends DatabaseServiceStringKey<LinuxGroupType> implements LinuxGroupTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<LinuxGroupType> objectFactory = new AutoObjectFactory<LinuxGroupType>(LinuxGroupType.class, this);

    DatabaseLinuxGroupTypeService(DatabaseConnector connector) {
        super(connector, LinuxGroupType.class);
    }

    protected Set<LinuxGroupType> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from linux_group_types"
        );
    }

    protected Set<LinuxGroupType> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from linux_group_types"
        );
    }

    protected Set<LinuxGroupType> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from linux_group_types"
        );
    }
}
