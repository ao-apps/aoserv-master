/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxAccountService;
import com.aoindustries.aoserv.client.command.SetLinuxAccountPasswordCommand;
import com.aoindustries.aoserv.master.DaemonHandler;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxAccountService extends DatabaseService<Integer,LinuxAccount> implements LinuxAccountService<DatabaseConnector,DatabaseConnectorFactory> {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<LinuxAccount> objectFactory = new AutoObjectFactory<LinuxAccount>(LinuxAccount.class, this);

    DatabaseLinuxAccountService(DatabaseConnector connector) {
        super(connector, Integer.class, LinuxAccount.class);
    }

    @Override
    protected Set<LinuxAccount> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<LinuxAccount>(),
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  linux_account_type,\n"
            + "  username,\n"
            + "  uid,\n"
            + "  home,\n"
            + "  name,\n"
            + "  office_location,\n"
            + "  office_phone,\n"
            + "  home_phone,\n"
            + "  shell,\n"
            + "  predisable_password\n"
            + "from\n"
            + "  linux_accounts"
        );
    }

    @Override
    protected Set<LinuxAccount> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<LinuxAccount>(),
            objectFactory,
            "select\n"
            + "  la.ao_server_resource,\n"
            + "  la.linux_account_type,\n"
            + "  la.username,\n"
            + "  la.uid,\n"
            + "  la.home,\n"
            + "  la.name,\n"
            + "  la.office_location,\n"
            + "  la.office_phone,\n"
            + "  la.home_phone,\n"
            + "  la.shell,\n"
            + "  la.predisable_password\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  linux_accounts la\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=la.ao_server\n"
            + "    or ff.server=la.ao_server\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<LinuxAccount> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<LinuxAccount>(),
            objectFactory,
             "select\n"
            + "  la.ao_server_resource,\n"
            + "  la.linux_account_type,\n"
            + "  la.username,\n"
            + "  la.uid,\n"
            + "  la.home,\n"
            + "  la.name,\n"
            + "  la.office_location,\n"
            + "  la.office_phone,\n"
            + "  la.home_phone,\n"
            + "  la.shell,\n"
            + "  case when la.predisable_password is null then null else ? end\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_accounts la\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=la.accounting",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setLinuxAccountPassword(DatabaseConnection db, InvalidateSet invalidateSet, SetLinuxAccountPasswordCommand command) throws RemoteException, SQLException {
        try {
            LinuxAccount la = connector.factory.rootConnector.getLinuxAccounts().get(command.getLinuxAccount());
            DaemonHandler.getDaemonConnector(la.getAoServerResource().getAoServer()).setLinuxAccountPassword(la.getUserId(), command.getPlaintext());
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }

        // Update the ao_servers table for emailmon and ftpmon
        /*if(username.equals(LinuxAccount.EMAILMON)) {
            conn.executeUpdate("update ao_servers set emailmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
            invalidateList.addTable(conn, SchemaTable.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
        } else if(username.equals(LinuxAccount.FTPMON)) {
            conn.executeUpdate("update ao_servers set ftpmon_password=? where server=?", password==null||password.length()==0?null:password, aoServer);
            invalidateList.addTable(conn, SchemaTable.TableID.AO_SERVERS, ServerHandler.getBusinessesForServer(conn, aoServer), aoServer, false);
        }*/
    }
    // </editor-fold>
}
