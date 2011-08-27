/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.master.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxAccountService extends DatabaseAOServerResourceService<LinuxAccount> implements LinuxAccountService {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<LinuxAccount> objectFactory = new AutoObjectFactory<LinuxAccount>(LinuxAccount.class, connector);

    DatabaseLinuxAccountService(DatabaseConnector connector) {
        super(connector, LinuxAccount.class);
    }

    @Override
    protected List<LinuxAccount> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxAccount>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  linux_accounts la\n"
            + "  inner join ao_server_resources asr on la.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected List<LinuxAccount> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxAccount>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  inner join ao_server_resources asr on la.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=la.ao_server\n"
            + "    or ff.server=la.ao_server\n"
            + "  )",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<LinuxAccount> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxAccount>(),
            objectFactory,
             "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  inner join ao_server_resources asr on la.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=la.accounting",
            AOServObject.FILTERED,
            connector.getSwitchUser()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setLinuxAccountPassword(DatabaseConnection db, InvalidateSet invalidateSet, int linuxAccount, String plaintext) throws RemoteException, SQLException {
        try {
            LinuxAccount rootLinuxAccount = connector.factory.getRootConnector().getLinuxAccounts().get(linuxAccount);
            DaemonHandler.getDaemonConnector(rootLinuxAccount.getAoServer()).setLinuxAccountPassword(rootLinuxAccount.getUserId(), plaintext);
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
