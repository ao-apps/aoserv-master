/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBrandService extends DatabaseAccountTypeService<AccountingCode,Brand> implements BrandService {

    private final ObjectFactory<Brand> objectFactory = new AutoObjectFactory<Brand>(Brand.class, connector);

    DatabaseBrandService(DatabaseConnector connector) {
        super(connector, AccountingCode.class, Brand.class);
    }

    @Override
    protected ArrayList<Brand> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Brand>(),
            objectFactory,
            "select * from brands"
        );
    }

    @Override
    protected ArrayList<Brand> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Brand>(),
            objectFactory,
            "select\n"
            + "  br.accounting,\n"
            + "  br.nameserver1,\n"
            + "  br.nameserver2,\n"
            + "  br.nameserver3,\n"
            + "  br.nameserver4,\n"
            + "  br.smtp_email_inbox,\n"
            + "  null,\n" // smtp_host
            + "  null,\n" // smtp_password
            + "  br.imap_email_inbox,\n"
            + "  null,\n" // imap_host
            + "  null,\n" // imap_password
            + "  br.support_email_address,\n"
            + "  br.support_email_display,\n"
            + "  br.signup_email_address,\n"
            + "  br.signup_email_display,\n"
            + "  br.ticket_encryption_from,\n"
            + "  br.ticket_encryption_recipient,\n"
            + "  br.signup_encryption_from,\n"
            + "  br.signup_encryption_recipient,\n"
            + "  br.support_toll_free,\n"
            + "  br.support_day_phone,\n"
            + "  br.support_emergency_phone1,\n"
            + "  br.support_emergency_phone2,\n"
            + "  br.support_fax,\n"
            + "  br.support_mailing_address1,\n"
            + "  br.support_mailing_address2,\n"
            + "  br.support_mailing_address3,\n"
            + "  br.support_mailing_address4,\n"
            + "  br.english_enabled,\n"
            + "  br.japanese_enabled,\n"
            + "  br.aoweb_struts_http_url_base,\n"
            + "  br.aoweb_struts_https_url_base,\n"
            + "  br.aoweb_struts_google_verify_content,\n"
            + "  br.aoweb_struts_noindex,\n"
            + "  br.aoweb_struts_google_analytics_new_tracking_code,\n"
            + "  br.aoweb_struts_signup_admin_address,\n"
            + "  br.aoweb_struts_vnc_bind,\n"
            + "  null,\n" // aoweb_struts_keystore_type
            + "  null\n" // aoweb_struts_keystore_password
            + "from\n"
            + "  usernames un,\n"
            + "  brands br\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=br.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<Brand> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Brand>(),
            objectFactory,
            "select\n"
            + "  br.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  brands br\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=br.accounting",
            connector.getConnectAs()
        );
    }
}
