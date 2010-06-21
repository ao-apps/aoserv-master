package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.DnsRecord;
import com.aoindustries.aoserv.client.DnsRecordService;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsRecordService extends DatabaseService<Integer,DnsRecord> implements DnsRecordService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DnsRecord> objectFactory = new ObjectFactory<DnsRecord>() {
        @Override
        public DnsRecord createObject(ResultSet result) throws SQLException {
            try {
                return new DnsRecord(
                    DatabaseDnsRecordService.this,
                    result.getInt("resource"),
                    result.getInt("zone"),
                    result.getString("domain"),
                    result.getString("type"),
                    (Integer)result.getObject("mx_priority"),
                    getInetAddress(result.getString("data_ip_address")),
                    getDomainName(result.getString("data_domain_name")),
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
        super(connector, Integer.class, DnsRecord.class);
    }

    @Override
    protected Set<DnsRecord> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  resource,\n"
            + "  zone,\n"
            + "  domain,\n"
            + "  type,\n"
            + "  mx_priority,\n"
            + "  data_ip_address,\n"
            + "  data_domain_name,\n"
            + "  data_text,\n"
            + "  dhcp_address,\n"
            + "  ttl\n"
            + "from dns_records"
        );
    }

    @Override
    protected Set<DnsRecord> getSetDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        MasterUser mu = connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).getMasterUser();
        if(mu!=null && mu.isActive() && mu.isDnsAdmin()) {
            return db.executeObjectSetQuery(
                objectFactory,
                "select\n"
                + "  resource,\n"
                + "  zone,\n"
                + "  domain,\n"
                + "  type,\n"
                + "  mx_priority,\n"
                + "  data_ip_address,\n"
                + "  data_domain_name,\n"
                + "  data_text,\n"
                + "  dhcp_address,\n"
                + "  ttl\n"
                + "from dns_records"
            );
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Set<DnsRecord> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  dr.resource,\n"
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
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=dr.accounting",
            connector.getConnectAs()
        );
    }
}
