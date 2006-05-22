package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>FTPHandler</code> handles all the accesses to the FTP tables.
 *
 * @author  AO Industries, Inc.
 */
final public class FTPHandler {

    public static void addFTPGuestUser(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FTPHandler.class, "addFTPGuestUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            LinuxAccountHandler.checkAccessLinuxAccount(conn, source, "addFTPGuestUser", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add FTP guest user for mail");

            if(LinuxAccountHandler.isLinuxAccountDisabled(conn, username)) throw new SQLException("Unable to add FTPGuestUser, LinuxAccount disabled: "+username);

            // FTP Guest Users may only be added to user and ftponly accounts
            String type=LinuxAccountHandler.getTypeForLinuxAccount(conn, username);
            if(
                !LinuxAccountType.USER.equals(type)
                && !LinuxAccountType.FTPONLY.equals(type)
            ) throw new SQLException("Only Linux Accounts of type '"+LinuxAccountType.USER+"' or '"+LinuxAccountType.FTPONLY+"' may be flagged as a FTP Guest User: "+type);

            conn.executeUpdate("insert into ftp_guest_users values(?)", username);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FTP_GUEST_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                LinuxAccountHandler.getAOServersForLinuxAccount(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeFTPGuestUser(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FTPHandler.class, "removeFTPGuestUser(MasterDatabaseConnection,RequestSource,InvalidateList,String)", null);
        try {
            LinuxAccountHandler.checkAccessLinuxAccount(conn, source, "removeFTPGuestUser", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to remove FTPGuestUser for user '"+LinuxAccount.MAIL+'\'');

            conn.executeUpdate("delete from ftp_guest_users where username=?", username);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FTP_GUEST_USERS,
                UsernameHandler.getBusinessForUsername(conn, username),
                LinuxAccountHandler.getAOServersForLinuxAccount(conn, username),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removePrivateFTPServer(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int net_bind
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FTPHandler.class, "removePrivateFTPServer(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            conn.executeUpdate("delete from private_ftp_servers net_bind=?", net_bind);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.PRIVATE_FTP_SERVERS,
                NetBindHandler.getBusinessForNetBind(conn, net_bind),
                ServerHandler.getHostnameForServer(conn, NetBindHandler.getAOServerForNetBind(conn, net_bind)),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}