/*
 * Copyright 2010 by AO Industries, Inc.,
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
final class DatabaseCreditCardService extends DatabaseService<Integer,CreditCard> implements CreditCardService {

    private final ObjectFactory<CreditCard> objectFactory = new AutoObjectFactory<CreditCard>(CreditCard.class, connector);

    DatabaseCreditCardService(DatabaseConnector connector) {
        super(connector, Integer.class, CreditCard.class);
    }

    @Override
    protected ArrayList<CreditCard> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CreditCard>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  processor_id,\n"
            + "  accounting,\n"
            + "  group_name,\n"
            + "  card_info,\n"
            + "  provider_unique_id,\n"
            + "  first_name,\n"
            + "  last_name,\n"
            + "  company_name,\n"
            + "  email,\n"
            + "  phone,\n"
            + "  fax,\n"
            + "  customer_tax_id,\n"
            + "  street_address1,\n"
            + "  street_address2,\n"
            + "  city,\n"
            + "  state,\n"
            + "  postal_code,\n"
            + "  country_code,\n"
            + "  (extract(epoch from created)*1000)::int8 as created,\n"
            + "  created_by,\n"
            + "  principal_name,\n"
            + "  use_monthly,\n"
            + "  active,\n"
            + "  (extract(epoch from deactivated_on)*1000)::int8 as deactivated_on,\n"
            + "  deactivate_reason,\n"
            + "  description,\n"
            + "  encrypted_card_number,\n"
            + "  encryption_card_number_from,\n"
            + "  encryption_card_number_recipient,\n"
            + "  encrypted_expiration,\n"
            + "  encryption_expiration_from,\n"
            + "  encryption_expiration_recipient\n"
            + "from\n"
            + "  credit_cards"
        );
    }

    @Override
    protected ArrayList<CreditCard> getListDaemon(DatabaseConnection db) {
        return new ArrayList<CreditCard>(0);
    }

    @Override
    protected ArrayList<CreditCard> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CreditCard>(),
            objectFactory,
            "select\n"
            + "  cc.pkey,\n"
            + "  cc.processor_id,\n"
            + "  cc.accounting,\n"
            + "  cc.group_name,\n"
            + "  cc.card_info,\n"
            + "  cc.provider_unique_id,\n"
            + "  cc.first_name,\n"
            + "  cc.last_name,\n"
            + "  cc.company_name,\n"
            + "  cc.email,\n"
            + "  cc.phone,\n"
            + "  cc.fax,\n"
            + "  cc.customer_tax_id,\n"
            + "  cc.street_address1,\n"
            + "  cc.street_address2,\n"
            + "  cc.city,\n"
            + "  cc.state,\n"
            + "  cc.postal_code,\n"
            + "  cc.country_code,\n"
            + "  (extract(epoch from cc.created)*1000)::int8 as created,\n"
            + "  cc.created_by,\n"
            + "  cc.principal_name,\n"
            + "  cc.use_monthly,\n"
            + "  cc.active,\n"
            + "  (extract(epoch from cc.deactivated_on)*1000)::int8 as deactivated_on,\n"
            + "  cc.deactivate_reason,\n"
            + "  cc.description,\n"
            + "  cc.encrypted_card_number,\n"
            + "  cc.encryption_card_number_from,\n"
            + "  cc.encryption_card_number_recipient,\n"
            + "  cc.encrypted_expiration,\n"
            + "  cc.encryption_expiration_from,\n"
            + "  cc.encryption_expiration_recipient\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  credit_cards cc\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=cc.accounting",
            connector.getConnectAs()
        );
    }
}
