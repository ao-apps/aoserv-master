/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseBusinessProfileService extends DatabaseAccountTypeService<Integer,BusinessProfile> implements BusinessProfileService {

    private final ObjectFactory<BusinessProfile> objectFactory = new AutoObjectFactory<BusinessProfile>(BusinessProfile.class, connector);

    DatabaseBusinessProfileService(DatabaseConnector connector) {
        super(connector, Integer.class, BusinessProfile.class);
    }

    @Override
    protected ArrayList<BusinessProfile> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessProfile>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  accounting,\n"
            + "  priority,\n"
            + "  name,\n"
            + "  private,\n"
            + "  phone,\n"
            + "  fax,\n"
            + "  address1,\n"
            + "  address2,\n"
            + "  city,\n"
            + "  state,\n"
            + "  country,\n"
            + "  zip,\n"
            + "  send_invoice,\n"
            + "  (extract(epoch from created)*1000)::int8 as created,\n"
            + "  billing_contact,\n"
            + "  billing_email,\n"
            + "  technical_contact,\n"
            + "  technical_email\n"
            + "from\n"
            + "  business_profiles"
        );
    }

    @Override
    protected ArrayList<BusinessProfile> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessProfile>(),
            objectFactory,
            "select distinct\n"
            + "  bp.pkey,\n"
            + "  bp.accounting,\n"
            + "  bp.priority,\n"
            + "  bp.name,\n"
            + "  bp.private,\n"
            + "  bp.phone,\n"
            + "  bp.fax,\n"
            + "  bp.address1,\n"
            + "  bp.address2,\n"
            + "  bp.city,\n"
            + "  bp.state,\n"
            + "  bp.country,\n"
            + "  bp.zip,\n"
            + "  bp.send_invoice,\n"
            + "  (extract(epoch from bp.created)*1000)::int8 as created,\n"
            + "  bp.billing_contact,\n"
            + "  bp.billing_email,\n"
            + "  bp.technical_contact,\n"
            + "  bp.technical_email\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  business_profiles bp\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=bp.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<BusinessProfile> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessProfile>(),
            objectFactory,
            "select\n"
            + "  bp.pkey,\n"
            + "  bp.accounting,\n"
            + "  bp.priority,\n"
            + "  bp.name,\n"
            + "  bp.private,\n"
            + "  bp.phone,\n"
            + "  bp.fax,\n"
            + "  bp.address1,\n"
            + "  bp.address2,\n"
            + "  bp.city,\n"
            + "  bp.state,\n"
            + "  bp.country,\n"
            + "  bp.zip,\n"
            + "  bp.send_invoice,\n"
            + "  (extract(epoch from bp.created)*1000)::int8 as created,\n"
            + "  bp.billing_contact,\n"
            + "  bp.billing_email,\n"
            + "  bp.technical_contact,\n"
            + "  bp.technical_email\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  business_profiles bp\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=bp.accounting",
            connector.getConnectAs()
        );
    }
}
