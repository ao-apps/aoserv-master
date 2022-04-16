/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2021, 2022  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master;

import com.aoapps.dbc.Database;
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
