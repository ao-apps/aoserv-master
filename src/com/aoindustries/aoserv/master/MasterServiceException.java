/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

/**
 * @author  AO Industries, Inc.
 */
public class MasterServiceException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MasterServiceException(String message) {
		super(message);
	}

	public MasterServiceException(String message, Throwable cause) {
		super(message, cause);
	}

	public MasterServiceException(Throwable cause) {
		super(cause);
	}
}
