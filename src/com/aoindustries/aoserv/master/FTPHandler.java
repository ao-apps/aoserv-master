/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
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

	public static void addFTPGuestUser(
		DatabaseConnection conn,
		RequestSource source, 
		InvalidateList invalidateList,
		User.Name username
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessLinuxAccount(conn, source, "addFTPGuestUser", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to add FTP guest user for mail");

		if(LinuxAccountHandler.isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add GuestUser, User disabled: "+username);

		// FTP Guest Users may only be added to user and ftponly accounts
		String type=LinuxAccountHandler.getTypeForLinuxAccount(conn, username);
		if(
			!UserType.USER.equals(type)
			&& !UserType.FTPONLY.equals(type)
		) throw new SQLException("Only Linux Accounts of type '"+UserType.USER+"' or '"+UserType.FTPONLY+"' may be flagged as a FTP Guest User: "+type);

		conn.executeUpdate("insert into ftp.\"GuestUser\" values(?)", username);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FTP_GUEST_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			LinuxAccountHandler.getAOServersForLinuxAccount(conn, username),
			false
		);
	}

	public static void removeFTPGuestUser(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		User.Name username
	) throws IOException, SQLException {
		LinuxAccountHandler.checkAccessLinuxAccount(conn, source, "removeFTPGuestUser", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to remove GuestUser for user '"+User.MAIL+'\'');

		conn.executeUpdate("delete from ftp.\"GuestUser\" where username=?", username);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.FTP_GUEST_USERS,
			UsernameHandler.getBusinessForUsername(conn, username),
			LinuxAccountHandler.getAOServersForLinuxAccount(conn, username),
			false
		);
	}

	public static void removePrivateFTPServer(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int net_bind
	) throws IOException, SQLException {
		conn.executeUpdate("delete from ftp.\"PrivateServer\" net_bind=?", net_bind);

		// Notify all clients of the update
		invalidateList.addTable(
			conn,
			Table.TableID.PRIVATE_FTP_SERVERS,
			NetBindHandler.getBusinessForNetBind(conn, net_bind),
			NetBindHandler.getServerForNetBind(conn, net_bind),
			false
		);
	}

	private FTPHandler() {}
}
