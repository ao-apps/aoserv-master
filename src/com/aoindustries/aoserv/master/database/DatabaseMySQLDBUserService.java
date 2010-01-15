package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MySQLDBUser;
import com.aoindustries.aoserv.client.MySQLDBUserService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLDBUserService extends DatabaseService<Integer,MySQLDBUser> implements MySQLDBUserService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLDBUser> objectFactory = new AutoObjectFactory<MySQLDBUser>(MySQLDBUser.class, this);

    DatabaseMySQLDBUserService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLDBUser.class);
    }

    protected Set<MySQLDBUser> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<MySQLDBUser> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<MySQLDBUser> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
            + "  ao_server_resources aor,\n"
            + "  mysql_db_users mdu\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=aor.accounting\n"
            + "  and aor.resource=mdu.mysql_database",
            connector.getConnectAs()
        );
    }
}
