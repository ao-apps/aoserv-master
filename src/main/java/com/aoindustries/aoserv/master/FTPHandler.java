/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;
import com.aoindustries.aoserv.client.linux.User;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>FTPHandler</code> handles all the accesses to the FTP tables.
 *
 * @author  AO Industries, Inc.
 */
public abstract class FTPHandler {

	/** Make no instances. */
	private FTPHandler() {throw new AssertionError();}

	public static void addGuestUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		User.Name linuxUser
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessUser(conn, source, "addFTPGuestUser", linuxUser);
		if(linuxUser.equals(User.MAIL)) throw new SQLException("Not allowed to add FTP guest user for mail");

		if(LinuxAccountHandler.isUserDisabled(conn, linuxUser)) throw new SQLException("Unable to add GuestUser, User disabled: "+linuxUser);

		// FTP Guest Users may only be added to user and ftponly accounts
		String type=LinuxAccountHandler.getTypeForUser(conn, linuxUser);
		if(
			!UserType.USER.equals(type)
			&& !UserType.FTPONLY.equals(type)
		) throw new SQLException("Only Linux Accounts of type '"+UserType.USER+"' or '"+UserType.FTPONLY+"' may be flagged as a FTP Guest User: "+type);

		conn.update("insert into ftp.\"GuestUser\" values(?)", linuxUser);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FTP_GUEST_USERS,
			AccountUserHandler.getAccountForUser(conn, linuxUser),
			LinuxAccountHandler.getServersForUser(conn, linuxUser),
			false
		);
	}

	public static void removeGuestUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		User.Name linuxUser
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessUser(conn, source, "removeFTPGuestUser", linuxUser);
		if(linuxUser.equals(User.MAIL)) throw new SQLException("Not allowed to remove GuestUser for user '"+User.MAIL+'\'');

		conn.update("delete from ftp.\"GuestUser\" where username=?", linuxUser);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FTP_GUEST_USERS,
			AccountUserHandler.getAccountForUser(conn, linuxUser),
			LinuxAccountHandler.getServersForUser(conn, linuxUser),
			false
		);
	}

	public static void removePrivateServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int bind
	) throws IOException, SQLException {
		conn.update("delete from ftp.\"PrivateServer\" net_bind=?", bind);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PRIVATE_FTP_SERVERS,
			NetBindHandler.getAccountForBind(conn, bind),
			NetBindHandler.getHostForBind(conn, bind),
			false
		);
	}
}
