/*
 * Copyright 2001-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.linux.User;
import com.aoindustries.aoserv.client.linux.UserType;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>FTPHandler</code> handles all the accesses to the FTP tables.
 *
 * @author  AO Industries, Inc.
 */
final public class FTPHandler {

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

		conn.executeUpdate("insert into ftp.\"GuestUser\" values(?)", linuxUser);

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

		conn.executeUpdate("delete from ftp.\"GuestUser\" where username=?", linuxUser);

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
		conn.executeUpdate("delete from ftp.\"PrivateServer\" net_bind=?", bind);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PRIVATE_FTP_SERVERS,
			NetBindHandler.getAccountForBind(conn, bind),
			NetBindHandler.getHostForBind(conn, bind),
			false
		);
	}

	private FTPHandler() {}
}
