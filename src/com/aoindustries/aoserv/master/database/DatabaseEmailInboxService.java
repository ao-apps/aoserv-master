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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseEmailInboxService extends DatabaseService<Integer,EmailInbox> implements EmailInboxService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<EmailInbox> objectFactory = new AutoObjectFactory<EmailInbox>(EmailInbox.class, this);

    DatabaseEmailInboxService(DatabaseConnector connector) {
        super(connector, Integer.class, EmailInbox.class);
    }

    @Override
    protected Set<EmailInbox> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<EmailInbox>(),
            objectFactory,
            "select\n"
            + "  linux_account,\n"
            + "  autoresponder_from,\n"
            + "  autoresponder_subject,\n"
            + "  autoresponder_path,\n"
            + "  is_autoresponder_enabled,\n"
            + "  use_inbox,\n"
            + "  trash_email_retention,\n"
            + "  junk_email_retention,\n"
            + "  sa_integration_mode,\n"
            + "  sa_required_score,\n"
            + "  sa_discard_score\n"
            + "from\n"
            + "  email_inboxes"
        );
    }

    @Override
    protected Set<EmailInbox> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<EmailInbox>(),
            objectFactory,
            "select\n"
            + "  ei.linux_account,\n"
            + "  ei.autoresponder_from,\n"
            + "  ei.autoresponder_subject,\n"
            + "  ei.autoresponder_path,\n"
            + "  ei.is_autoresponder_enabled,\n"
            + "  ei.use_inbox,\n"
            + "  ei.trash_email_retention,\n"
            + "  ei.junk_email_retention,\n"
            + "  ei.sa_integration_mode,\n"
            + "  ei.sa_required_score,\n"
            + "  ei.sa_discard_score\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  linux_accounts la,\n"
            + "  email_inboxes ei\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=la.ao_server\n"
            + "  and la.ao_server_resource=ei.linux_account",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<EmailInbox> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<EmailInbox>(),
            objectFactory,
            "select\n"
            + "  ei.linux_account,\n"
            + "  ei.autoresponder_from,\n"
            + "  ei.autoresponder_subject,\n"
            + "  ei.autoresponder_path,\n"
            + "  ei.is_autoresponder_enabled,\n"
            + "  ei.use_inbox,\n"
            + "  ei.trash_email_retention,\n"
            + "  ei.junk_email_retention,\n"
            + "  ei.sa_integration_mode,\n"
            + "  ei.sa_required_score,\n"
            + "  ei.sa_discard_score\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_accounts la,\n"
            + "  email_inboxes ei\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=la.accounting\n"
            + "  and la.ao_server_resource=ei.linux_account",
            connector.getConnectAs()
        );
    }
}
