/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.threadLocale;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.aoserv.client.wrapped.*;
import com.aoindustries.security.LoginException;
import com.aoindustries.util.i18n.ThreadLocale;
import java.rmi.RemoteException;
import java.util.Locale;

/**
 * An implementation of <code>AOServConnectorFactory</code> that sets
 * the ThreadLocale to that of this connector.
 *
 * @see ThreadLocale
 *
 * @author  AO Industries, Inc.
 */
final public class ThreadLocaleConnectorFactory extends WrappedConnectorFactory<ThreadLocaleConnector,ThreadLocaleConnectorFactory> {

    public ThreadLocaleConnectorFactory(AOServConnectorFactory wrapped) {
        super(wrapped);
    }

    @Override
    protected ThreadLocaleConnector newWrappedConnector(Locale locale, UserId username, String password, UserId switchUser, DomainName daemonServer, boolean readOnly) throws LoginException, RemoteException {
        Locale oldLocale = ThreadLocale.get();
        try {
            ThreadLocale.set(locale);
            return new ThreadLocaleConnector(this, locale, username, password, switchUser, daemonServer, readOnly);
        } finally {
            ThreadLocale.set(oldLocale);
        }
    }
}
