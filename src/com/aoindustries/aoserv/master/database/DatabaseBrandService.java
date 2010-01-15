package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Brand;
import com.aoindustries.aoserv.client.BrandService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBrandService extends DatabaseServiceAccountingCodeKey<Brand> implements BrandService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Brand> objectFactory = new AutoObjectFactory<Brand>(Brand.class, this);

    DatabaseBrandService(DatabaseConnector connector) {
        super(connector, Brand.class);
    }

    protected Set<Brand> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from brands"
        );
    }

    protected Set<Brand> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<Brand> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
