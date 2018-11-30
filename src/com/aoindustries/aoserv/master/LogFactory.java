/*
 * Copyright 2009-2013, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.reseller.Category;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Handler;
import java.util.logging.Logger;

/**
 * Provides static access to the logging facilities.  The logs are inserted
 * directly into the AOServ ticket system database under the type "logs".
 *
 * @author  AO Industries, Inc.
 */
public class LogFactory {

	private static final ConcurrentMap<String,Logger> loggers = new ConcurrentHashMap<>();

	private LogFactory() {
	}

	/**
	 * Gets the logger for the provided class.
	 */
	public static Logger getLogger(Class<?> clazz) {
		return getLogger(clazz.getName());
	}

	/**
	 * <p>
	 * Gets the logger for the provided and name.  The logger is cached.
	 * Subsequent calls to this method will return the previously created logger.
	 * </p>
	 */
	public static Logger getLogger(String name) {
		Logger logger = loggers.get(name);
		if(logger==null) {
			Handler handler = TicketLoggingHandler.getHandler("AOServ Master", Category.AOSERV_MASTER_PKEY);
			logger = Logger.getLogger(name);
			synchronized(logger) {
				boolean foundHandler = false;
				for(Handler oldHandler : logger.getHandlers()) {
					if(oldHandler==handler) foundHandler = true;
					else logger.removeHandler(oldHandler);
				}
				if(!foundHandler) logger.addHandler(handler);
				logger.setUseParentHandlers(false);
			}
			loggers.put(name, logger);
		}
		return logger;
	}
}
