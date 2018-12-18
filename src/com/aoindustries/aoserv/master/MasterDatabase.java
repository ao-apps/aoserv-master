/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.dbc.Database;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author  AO Industries, Inc.
 */
public final class MasterDatabase extends Database {

	/**
	 * This logger doesn't use ticket logger because it might create a loop
	 * by logging database errors to the database.
	 */
	private static final Logger logger = Logger.getLogger(MasterDatabase.class.getName());

	/**
	 * Only one database accessor is made.
	 */
	private static MasterDatabase masterDatabase;

	/**
	 * Make no instances.
	 */
	private MasterDatabase() throws IOException {
		super(
			MasterConfiguration.getDBDriver(),
			MasterConfiguration.getDBURL(),
			MasterConfiguration.getDBUser(),
			MasterConfiguration.getDBPassword(),
			MasterConfiguration.getDBConnectionPoolSize(),
			MasterConfiguration.getDBMaxConnectionAge(),
			logger
		);
	}

	public static MasterDatabase getDatabase() throws IOException {
		synchronized(MasterDatabase.class) {
			if(masterDatabase==null) masterDatabase=new MasterDatabase();
			return masterDatabase;
		}
	}

//	public static class PgEmail extends PGobject {
//
//		private static final long serialVersionUID = 1L;
//
//		private Email email;
//
//		@Override
//		public void setValue(String value) throws SQLException {
//			try {
//				email = Email.valueOf(value);
//			} catch(ValidationException err) {
//				throw new SQLException(err.getMessage(), err);
//			}
//		}
//
//		@Override
//		public String getValue() {
//			return Objects.toString(email, null);
//		}
//
//		@Override
//		public boolean equals(Object obj) {
//			if(!(obj instanceof PgEmail)) return false;
//			return Objects.equals(email, ((PgEmail)obj).email);
//		}
//
//		@Override
//		public Object clone() throws CloneNotSupportedException {
//			return super.clone();
//			//PgEmail clone = (PgEmail)super.clone();
//			//clone.email = email;
//			//return clone;
//		}
//
//		@Override
//		public int hashCode() {
//			return Objects.hashCode(email);
//		}
//	}
//
//	/**
//	 * Registers custom {@link PGobject} classes.
//	 * <p>
//	 * TODO:
//	 * TODO: Make a new project extending ao-dbc for this purpose.
//	 * </p>
//	 * <p>
//	 * TODO: Deprecate this once the PostgreSQL drivers support {@link SQLData}.
//	 * </p>
//	 */
//	@Override
//	public Connection getConnection(int isolationLevel, boolean readOnly, int maxConnections) throws SQLException {
//		Connection conn = super.getConnection(isolationLevel, readOnly, maxConnections);
//		if(conn instanceof PGConnection) {
//			PGConnection pgConn = (PGConnection)conn;
//			// TODO: Load these via ServiceLoader
//			pgConn.addDataType(Email.SQL_TYPE, PgEmail.class);
//		} else {
//			// TODO: Limit logging rate here
//			System.err.println("WARNING: Connection is not a " + PGConnection.class.getName() + ": " + conn.getClass().getName());
//		}
//		return conn;
//	}
}
