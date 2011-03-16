/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLDBUserService extends DatabaseAccountTypeService<Integer,MySQLDBUser> implements MySQLDBUserService {

    private final ObjectFactory<MySQLDBUser> objectFactory = new AutoObjectFactory<MySQLDBUser>(MySQLDBUser.class, connector);

    DatabaseMySQLDBUserService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLDBUser.class);
    }

    @Override
    protected ArrayList<MySQLDBUser> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDBUser>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  mysql_database,\n"
            + "  mysql_user,\n"
            + "  select_priv,\n"
            + "  insert_priv,\n"
            + "  update_priv,\n"
            + "  delete_priv,\n"
            + "  create_priv,\n"
            + "  drop_priv,\n"
            + "  grant_priv,\n"
            + "  references_priv,\n"
            + "  index_priv,\n"
            + "  alter_priv,\n"
            + "  create_tmp_table_priv,\n"
            + "  lock_tables_priv,\n"
            + "  create_view_priv,\n"
            + "  show_view_priv,\n"
            + "  create_routine_priv,\n"
            + "  alter_routine_priv,\n"
            + "  execute_priv,\n"
            + "  event_priv,\n"
            + "  trigger_priv\n"
            + "from\n"
            + "  mysql_db_users"
        );
    }

    @Override
    protected ArrayList<MySQLDBUser> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDBUser>(),
            objectFactory,
            "select\n"
            + "  mdu.pkey,\n"
            + "  mdu.mysql_database,\n"
            + "  mdu.mysql_user,\n"
            + "  mdu.select_priv,\n"
            + "  mdu.insert_priv,\n"
            + "  mdu.update_priv,\n"
            + "  mdu.delete_priv,\n"
            + "  mdu.create_priv,\n"
            + "  mdu.drop_priv,\n"
            + "  mdu.grant_priv,\n"
            + "  mdu.references_priv,\n"
            + "  mdu.index_priv,\n"
            + "  mdu.alter_priv,\n"
            + "  mdu.create_tmp_table_priv,\n"
            + "  mdu.lock_tables_priv,\n"
            + "  mdu.create_view_priv,\n"
            + "  mdu.show_view_priv,\n"
            + "  mdu.create_routine_priv,\n"
            + "  mdu.alter_routine_priv,\n"
            + "  mdu.execute_priv,\n"
            + "  mdu.event_priv,\n"
            + "  mdu.trigger_priv\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_servers mys,\n"
            + "  mysql_db_users mdu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mys.ao_server\n"
            + "  and mys.ao_server_resource=mdu.mysql_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MySQLDBUser> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDBUser>(),
            objectFactory,
            "select\n"
            + "  mdu.pkey,\n"
            + "  mdu.mysql_database,\n"
            + "  mdu.mysql_user,\n"
            + "  mdu.select_priv,\n"
            + "  mdu.insert_priv,\n"
            + "  mdu.update_priv,\n"
            + "  mdu.delete_priv,\n"
            + "  mdu.create_priv,\n"
            + "  mdu.drop_priv,\n"
            + "  mdu.grant_priv,\n"
            + "  mdu.references_priv,\n"
            + "  mdu.index_priv,\n"
            + "  mdu.alter_priv,\n"
            + "  mdu.create_tmp_table_priv,\n"
            + "  mdu.lock_tables_priv,\n"
            + "  mdu.create_view_priv,\n"
            + "  mdu.show_view_priv,\n"
            + "  mdu.create_routine_priv,\n"
            + "  mdu.alter_routine_priv,\n"
            + "  mdu.execute_priv,\n"
            + "  mdu.event_priv,\n"
            + "  mdu.trigger_priv\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources asr,\n"
            + "  mysql_db_users mdu\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=asr.accounting\n"
            + "  and asr.resource=mdu.mysql_database",
            connector.getConnectAs()
        );
    }
}
