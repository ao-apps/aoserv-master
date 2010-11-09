/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.threadLocale;

import com.aoindustries.aoserv.client.AOServConnectorFactory;
import com.aoindustries.aoserv.client.validator.DomainName;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.wrapped.WrappedConnectorFactory;
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

    public ThreadLocaleConnectorFactory(AOServConnectorFactory<?,?> wrapped) {
        super(wrapped);
    }

    @Override
    protected ThreadLocaleConnector newWrappedConnector(Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer) throws LoginException, RemoteException {
        Locale oldLocale = ThreadLocale.get();
        try {
            ThreadLocale.set(locale);
            return new ThreadLocaleConnector(this, locale, connectAs, authenticateAs, password, daemonServer);
        } finally {
            ThreadLocale.set(oldLocale);
        }
    }
}
