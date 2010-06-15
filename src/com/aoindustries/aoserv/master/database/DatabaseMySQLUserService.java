package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.MySQLUser;
import com.aoindustries.aoserv.client.MySQLUserService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLUserService extends DatabaseService<Integer,MySQLUser> implements MySQLUserService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLUser> objectFactory = new AutoObjectFactory<MySQLUser>(MySQLUser.class, this);

    DatabaseMySQLUserService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLUser.class);
    }

    protected Set<MySQLUser> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  username,\n"
            + "  mysql_server,\n"
            + "  host,\n"
            + "  select_priv,\n"
            + "  insert_priv,\n"
            + "  update_priv,\n"
            + "  delete_priv,\n"
            + "  create_priv,\n"
            + "  drop_priv,\n"
            + "  reload_priv,\n"
            + "  shutdown_priv,\n"
            + "  process_priv,\n"
            + "  file_priv,\n"
            + "  grant_priv,\n"
            + "  references_priv,\n"
            + "  index_priv,\n"
            + "  alter_priv,\n"
            + "  show_db_priv,\n"
            + "  super_priv,\n"
            + "  create_tmp_table_priv,\n"
            + "  lock_tables_priv,\n"
            + "  execute_priv,\n"
            + "  repl_slave_priv,\n"
            + "  repl_client_priv,\n"
            + "  create_view_priv,\n"
            + "  show_view_priv,\n"
            + "  create_routine_priv,\n"
            + "  alter_routine_priv,\n"
            + "  create_user_priv,\n"
            + "  event_priv,\n"
            + "  trigger_priv,\n"
            + "  predisable_password,\n"
            + "  max_questions,\n"
            + "  max_updates\n,"
            + "  max_connections,\n"
            + "  max_user_connections\n"
            + "from\n"
            + "  mysql_users"
        );
    }

    protected Set<MySQLUser> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  mu.ao_server_resource,\n"
            + "  mu.username,\n"
            + "  mu.mysql_server,\n"
            + "  mu.host,\n"
            + "  mu.select_priv,\n"
            + "  mu.insert_priv,\n"
            + "  mu.update_priv,\n"
            + "  mu.delete_priv,\n"
            + "  mu.create_priv,\n"
            + "  mu.drop_priv,\n"
            + "  mu.reload_priv,\n"
            + "  mu.shutdown_priv,\n"
            + "  mu.process_priv,\n"
            + "  mu.file_priv,\n"
            + "  mu.grant_priv,\n"
            + "  mu.references_priv,\n"
            + "  mu.index_priv,\n"
            + "  mu.alter_priv,\n"
            + "  mu.show_db_priv,\n"
            + "  mu.super_priv,\n"
            + "  mu.create_tmp_table_priv,\n"
            + "  mu.lock_tables_priv,\n"
            + "  mu.execute_priv,\n"
            + "  mu.repl_slave_priv,\n"
            + "  mu.repl_client_priv,\n"
            + "  mu.create_view_priv,\n"
            + "  mu.show_view_priv,\n"
            + "  mu.create_routine_priv,\n"
            + "  mu.alter_routine_priv,\n"
            + "  mu.create_user_priv,\n"
            + "  mu.event_priv,\n"
            + "  mu.trigger_priv,\n"
            + "  mu.predisable_password,\n"
            + "  mu.max_questions,\n"
            + "  mu.max_updates\n,"
            + "  mu.max_connections,\n"
            + "  mu.max_user_connections\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_users mu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mu.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<MySQLUser> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
             "select\n"
            + "  mu.ao_server_resource,\n"
            + "  mu.username,\n"
            + "  mu.mysql_server,\n"
            + "  mu.host,\n"
            + "  mu.select_priv,\n"
            + "  mu.insert_priv,\n"
            + "  mu.update_priv,\n"
            + "  mu.delete_priv,\n"
            + "  mu.create_priv,\n"
            + "  mu.drop_priv,\n"
            + "  mu.reload_priv,\n"
            + "  mu.shutdown_priv,\n"
            + "  mu.process_priv,\n"
            + "  mu.file_priv,\n"
            + "  mu.grant_priv,\n"
            + "  mu.references_priv,\n"
            + "  mu.index_priv,\n"
            + "  mu.alter_priv,\n"
            + "  mu.show_db_priv,\n"
            + "  mu.super_priv,\n"
            + "  mu.create_tmp_table_priv,\n"
            + "  mu.lock_tables_priv,\n"
            + "  mu.execute_priv,\n"
            + "  mu.repl_slave_priv,\n"
            + "  mu.repl_client_priv,\n"
            + "  mu.create_view_priv,\n"
            + "  mu.show_view_priv,\n"
            + "  mu.create_routine_priv,\n"
            + "  mu.alter_routine_priv,\n"
            + "  mu.create_user_priv,\n"
            + "  mu.event_priv,\n"
            + "  mu.trigger_priv,\n"
            + "  case when mu.predisable_password is null then null else ? end,\n"
            + "  mu.max_questions,\n"
            + "  mu.max_updates\n,"
            + "  mu.max_connections,\n"
            + "  mu.max_user_connections\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  mysql_users mu\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=mu.accounting",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
}
