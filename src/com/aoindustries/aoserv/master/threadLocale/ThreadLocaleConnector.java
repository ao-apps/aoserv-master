/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.threadLocale;

import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.aoserv.client.wrapped.*;
import com.aoindustries.security.LoginException;
import com.aoindustries.util.i18n.ThreadLocale;
import java.rmi.RemoteException;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

/**
 * @see ThreadLocaleConnectorFactory
 *
 * @author  AO Industries, Inc.
 */
final public class ThreadLocaleConnector extends WrappedConnector<ThreadLocaleConnector,ThreadLocaleConnectorFactory> {

    ThreadLocaleConnector(ThreadLocaleConnectorFactory factory, Locale locale, UserId username, String password, UserId switchUser, DomainName daemonServer, boolean readOnly) throws RemoteException, LoginException {
        super(factory, locale, username, password, switchUser, daemonServer, readOnly);
    }

    @Override
    protected <T> T call(Callable<T> callable, boolean allowRetry) throws RemoteException, NoSuchElementException {
        Locale oldLocale = ThreadLocale.get();
        try {
            ThreadLocale.set(getLocale());
            return super.call(callable, allowRetry);
        } finally {
            ThreadLocale.set(oldLocale);
        }
    }
}
