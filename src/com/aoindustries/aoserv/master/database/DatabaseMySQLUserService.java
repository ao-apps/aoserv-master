/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.master.DaemonHandler;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLUserService extends DatabaseService<Integer,MySQLUser> implements MySQLUserService {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<MySQLUser> objectFactory = new AutoObjectFactory<MySQLUser>(MySQLUser.class, connector);

    DatabaseMySQLUserService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLUser.class);
    }

    @Override
    protected ArrayList<MySQLUser> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLUser>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  mysql_users mu\n"
            + "  inner join ao_server_resources asr on mu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<MySQLUser> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLUser>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  inner join ao_server_resources asr on mu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mu.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MySQLUser> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLUser>(),
            objectFactory,
             "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  inner join ao_server_resources asr on mu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
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
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setMySQLUserPassword(DatabaseConnection db, InvalidateSet invalidateSet, int mysqlUser, String plaintext) throws RemoteException, SQLException {
        try {
            MySQLUser mu = connector.factory.rootConnector.getMysqlUsers().get(mysqlUser);
            DaemonHandler.getDaemonConnector(mu.getAoServer()).setMySQLUserPassword(mysqlUser, plaintext);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
    // </editor-fold>
}
