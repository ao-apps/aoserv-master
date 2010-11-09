/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorService;
import com.aoindustries.aoserv.client.ServiceName;
import com.aoindustries.aoserv.client.command.SetBusinessAdministratorPasswordCommand;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.Email;
import com.aoindustries.aoserv.client.validator.HashedPassword;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessAdministratorService extends DatabaseService<UserId,BusinessAdministrator> implements BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<BusinessAdministrator> objectFactory = new ObjectFactory<BusinessAdministrator>() {
        @Override
        public BusinessAdministrator createObject(ResultSet result) throws SQLException {
            try {
                return new BusinessAdministrator(
                    DatabaseBusinessAdministratorService.this,
                    UserId.valueOf(result.getString("username")),
                    HashedPassword.valueOf(result.getString("password")),
                    result.getString("full_name"),
                    result.getString("title"),
                    result.getDate("birthday"),
                    result.getBoolean("is_preferred"),
                    result.getBoolean("private"),
                    result.getLong("created"),
                    result.getString("work_phone"),
                    result.getString("home_phone"),
                    result.getString("cell_phone"),
                    result.getString("fax"),
                    Email.valueOf(result.getString("email")),
                    result.getString("address1"),
                    result.getString("address2"),
                    result.getString("city"),
                    result.getString("state"),
                    result.getString("country"),
                    result.getString("zip"),
                    (Integer)result.getObject("disable_log"),
                    result.getBoolean("can_switch_users"),
                    result.getString("support_code")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }

    };

    DatabaseBusinessAdministratorService(DatabaseConnector connector) {
        super(connector, UserId.class, BusinessAdministrator.class);
    }

    @Override
    protected Set<BusinessAdministrator> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<BusinessAdministrator>(),
            objectFactory,
            "select\n"
            + "  username,\n"
            + "  password,\n"
            + "  full_name,\n"
            + "  title,\n"
            + "  birthday,\n"
            + "  is_preferred,\n"
            + "  private,\n"
            + "  (extract(epoch from created)*1000)::int8 as created,\n"
            + "  work_phone,\n"
            + "  home_phone,\n"
            + "  cell_phone,\n"
            + "  fax,\n"
            + "  email,\n"
            + "  address1,\n"
            + "  address2,\n"
            + "  city,\n"
            + "  state,\n"
            + "  country,\n"
            + "  zip,\n"
            + "  disable_log,\n"
            + "  can_switch_users,\n"
            + "  support_code\n"
            + "from\n"
            + "  business_administrators"
        );
    }

    @Override
    protected Set<BusinessAdministrator> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<BusinessAdministrator>(),
            objectFactory,
            "select distinct\n"
            + "  ba.username,\n"
            + "  '"+HashedPassword.NO_PASSWORD+"'::text as password,\n"
            + "  ba.full_name,\n"
            + "  ba.title,\n"
            + "  ba.birthday,\n"
            + "  ba.is_preferred,\n"
            + "  ba.private,\n"
            + "  (extract(epoch from ba.created)*1000)::int8 as created,\n"
            + "  ba.work_phone,\n"
            + "  ba.home_phone,\n"
            + "  ba.cell_phone,\n"
            + "  ba.fax,\n"
            + "  ba.email,\n"
            + "  ba.address1,\n"
            + "  ba.address2,\n"
            + "  ba.city,\n"
            + "  ba.state,\n"
            + "  ba.country,\n"
            + "  ba.zip,\n"
            + "  ba.disable_log,\n"
            + "  ba.can_switch_users,\n"
            + "  null as support_code\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  business_administrators ba\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=ba.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<BusinessAdministrator> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<BusinessAdministrator>(),
            objectFactory,
            "select\n"
            + "  ba.username,\n"
            + "  '"+HashedPassword.NO_PASSWORD+"'::text as password,\n"
            + "  ba.full_name,\n"
            + "  ba.title,\n"
            + "  ba.birthday,\n"
            + "  ba.is_preferred,\n"
            + "  ba.private,\n"
            + "  (extract(epoch from ba.created)*1000)::int8 as created,\n"
            + "  ba.work_phone,\n"
            + "  ba.home_phone,\n"
            + "  ba.cell_phone,\n"
            + "  ba.fax,\n"
            + "  ba.email,\n"
            + "  ba.address1,\n"
            + "  ba.address2,\n"
            + "  ba.city,\n"
            + "  ba.state,\n"
            + "  ba.country,\n"
            + "  ba.zip,\n"
            + "  ba.disable_log,\n"
            + "  ba.can_switch_users,\n"
            + "  ba.support_code\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  business_administrators ba\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    ba.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=ba.accounting",
            connector.getConnectAs()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setBusinessAdministratorPassword(DatabaseConnection db, InvalidateSet invalidateSet, SetBusinessAdministratorPasswordCommand command) throws RemoteException, SQLException {
        String plaintext = command.getPlaintext();
        String hashed =
            plaintext==null || plaintext.length()==0
            ? HashedPassword.NO_PASSWORD
            : HashedPassword.hash(plaintext)
        ;
        UserId username = command.getUsername();
        AccountingCode accounting = connector.factory.rootConnector.getUsernames().get(username).getBusiness().getAccounting();
        db.executeUpdate("update business_administrators set password=? where username=?", hashed, username);

        // Notify all clients of the update
        invalidateSet.add(ServiceName.business_administrators, accounting);
    }
    // </editor-fold>
}
