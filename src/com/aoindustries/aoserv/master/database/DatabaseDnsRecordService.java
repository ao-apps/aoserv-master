/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsRecordService extends DatabaseResourceService<DnsRecord> implements DnsRecordService {

    private final ObjectFactory<DnsRecord> objectFactory = new ObjectFactory<DnsRecord>() {
        @Override
        public DnsRecord createObject(ResultSet result) throws SQLException {
            try {
                return new DnsRecord(
                    connector,
                    result.getInt("pkey"),
                    result.getString("resource_type"),
                    AccountingCode.valueOf(result.getString("accounting")),
                    result.getLong("created"),
                    UserId.valueOf(result.getString("created_by")),
                    (Integer)result.getObject("disable_log"),
                    result.getLong("last_enabled"),
                    result.getInt("zone"),
                    result.getString("domain"),
                    result.getString("type"),
                    (Integer)result.getObject("mx_priority"),
                    InetAddress.valueOf(result.getString("data_ip_address")),
                    DomainName.valueOf(result.getString("data_domain_name")),
                    result.getString("data_text"),
                    (Integer)result.getObject("dhcp_address"),
                    (Integer)result.getObject("ttl")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseDnsRecordService(DatabaseConnector connector) {
        super(connector, DnsRecord.class);
    }

    @Override
    protected List<DnsRecord> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DnsRecord>(),
            objectFactory,
            "select\n"
            + RESOURCE_SELECT_COLUMNS + ",\n"
            + "  dr.zone,\n"
            + "  dr.domain,\n"
            + "  dr.type,\n"
            + "  dr.mx_priority,\n"
            + "  dr.data_ip_address,\n"
            + "  dr.data_domain_name,\n"
            + "  dr.data_text,\n"
            + "  dr.dhcp_address,\n"
            + "  dr.ttl\n"
            + "from\n"
            + "  dns_records dr\n"
            + "  inner join resources re on dr.resource=re.pkey"
        );
    }

    @Override
    protected List<DnsRecord> getListDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        MasterUser rootMu = connector.factory.getRootConnector().getBusinessAdministrators().get(connector.getSwitchUser()).getMasterUser();
        if(rootMu!=null && rootMu.isActive() && rootMu.isDnsAdmin()) {
            return db.executeObjectCollectionQuery(
                new ArrayList<DnsRecord>(),
                objectFactory,
                "select\n"
                + RESOURCE_SELECT_COLUMNS + ",\n"
                + "  dr.zone,\n"
                + "  dr.domain,\n"
                + "  dr.type,\n"
                + "  dr.mx_priority,\n"
                + "  dr.data_ip_address,\n"
                + "  dr.data_domain_name,\n"
                + "  dr.data_text,\n"
                + "  dr.dhcp_address,\n"
                + "  dr.ttl\n"
                + "from\n"
                + "  dns_records dr\n"
                + "  inner join resources re on dr.resource=re.pkey"
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<DnsRecord> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DnsRecord>(),
            objectFactory,
            "select\n"
            + RESOURCE_SELECT_COLUMNS + ",\n"
            + "  dr.zone,\n"
            + "  dr.domain,\n"
            + "  dr.type,\n"
            + "  dr.mx_priority,\n"
            + "  dr.data_ip_address,\n"
            + "  dr.data_domain_name,\n"
            + "  dr.data_text,\n"
            + "  dr.dhcp_address,\n"
            + "  dr.ttl\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  dns_records dr\n"
            + "  inner join resources re on dr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=dr.accounting",
            connector.getSwitchUser()
        );
    }
}
