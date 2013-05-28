/*
 * Copyright 2002-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.CvsRepository;
import com.aoindustries.aoserv.client.HttpdSharedTomcat;
import com.aoindustries.aoserv.client.HttpdSite;
import com.aoindustries.aoserv.client.LinuxAccountType;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>CvsHandler</code> handles all the accesses to the <code>cvs_repositories</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class CvsHandler {

    private final static Map<Integer,Boolean> disabledCvsRepositories=new HashMap<Integer,Boolean>();

    public static int addCvsRepository(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int aoServer,
        String path,
        int lsa,
        int lsg,
        long mode
    ) throws IOException, SQLException {
        synchronized(CvsHandler.class) {
            // Security checks
            ServerHandler.checkAccessServer(conn, source, "addCvsRepository", aoServer);
            LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", lsa);
            LinuxAccountHandler.checkAccessLinuxServerGroup(conn, source, "addCvsRepository", lsg);
            if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to add CvsRepository, LinuxServerAccount disabled: "+lsa);

            // Integrity checks
            if(!CvsRepository.isValidPath(path)) throw new SQLException("Invalid path: "+path);
            if(path.length()>6 && path.substring(0, 6).equals("/home/")) {
                // Must be able to access one of the linux_server_accounts with that home directory
                int slashPos=path.indexOf('/', 6);
                if(slashPos!=7) throw new SQLException("Invalid path: "+path);
                char ch=path.charAt(6);
                if(ch<'a' || ch>'z') throw new SQLException("Invalid path: "+path);
                slashPos=path.indexOf('/', 8);
                if(slashPos==-1) slashPos=path.length();
                String homeDir=path.substring(0, slashPos);
                IntList lsas=conn.executeIntListQuery(
                    "select pkey from linux_server_accounts where ao_server=? and home=?",
                    aoServer,
                    homeDir
                );
                boolean found=false;
                for(int c=0;c<lsas.size();c++) {
                    if(LinuxAccountHandler.canAccessLinuxServerAccount(conn, source, lsas.getInt(c))) {
                        found=true;
                        break;
                    }
                }
                if(!found) throw new SQLException("Home directory not allowed for path: "+homeDir);
            } else if(path.length()>9 && path.substring(0, 9).equals("/var/cvs/")) {
                int slashPos=path.indexOf('/', 9);
                if(slashPos!=-1) throw new SQLException("Invalid path: "+path);
            } else if(path.length()>(HttpdSite.WWW_DIRECTORY.length()+1) && path.substring(0, HttpdSite.WWW_DIRECTORY.length()+1).equals(HttpdSite.WWW_DIRECTORY+'/')) {
                int slashPos=path.indexOf('/', HttpdSite.WWW_DIRECTORY.length()+1);
                if(slashPos==-1) slashPos=path.length();
                String siteName=path.substring(HttpdSite.WWW_DIRECTORY.length()+1, slashPos);
                int hs=conn.executeIntQuery("select pkey from httpd_sites where ao_server=? and site_name=?", aoServer, siteName);
                HttpdHandler.checkAccessHttpdSite(conn, source, "addCvsRepository", hs);
            } else if(path.length()>(HttpdSharedTomcat.WWW_GROUP_DIR.length()+1) && path.substring(0, HttpdSharedTomcat.WWW_GROUP_DIR.length()+1).equals(HttpdSharedTomcat.WWW_GROUP_DIR+'/')) {
                int slashPos=path.indexOf('/', HttpdSharedTomcat.WWW_GROUP_DIR.length()+1);
                if(slashPos==-1) slashPos=path.length();
                String groupName=path.substring(HttpdSharedTomcat.WWW_GROUP_DIR.length()+1, slashPos);
                int groupLSA=conn.executeIntQuery("select linux_server_account from httpd_shared_tomcats where name=? and ao_server=?", groupName, aoServer);
                LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "addCvsRepository", groupLSA);
            } else throw new SQLException("Invalid path: "+path);

            // Must not already have an existing CVS repository on this server
            if(
                conn.executeBooleanQuery(
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      cr.pkey\n"
                    + "    from\n"
                    + "      cvs_repositories cr,\n"
                    + "      linux_server_accounts lsa\n"
                    + "    where\n"
                    + "      cr.path=?\n"
                    + "      and cr.linux_server_account=lsa.pkey\n"
                    + "      and lsa.ao_server=?\n"
                    + "    limit 1\n"
                    + "  ) is not null",
                    path,
                    aoServer
                )
            ) throw new SQLException("CvsRepository already exists: "+path+" on AOServer #"+aoServer);

            int lsaAOServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa);
            if(lsaAOServer!=aoServer) throw new SQLException("LinuxServerAccount "+lsa+" is not located on AOServer #"+aoServer);
            String type=LinuxAccountHandler.getTypeForLinuxServerAccount(conn, lsa);
            if(
                !(
                    LinuxAccountType.USER.equals(type)
                    || LinuxAccountType.APPLICATION.equals(type)
                )
            ) throw new SQLException("CVS repositories must be owned by a linux account of type '"+LinuxAccountType.USER+"' or '"+LinuxAccountType.APPLICATION+'\'');

            int lsgAOServer=LinuxAccountHandler.getAOServerForLinuxServerGroup(conn, lsg);
            if(lsgAOServer!=aoServer) throw new SQLException("LinuxServerGroup "+lsg+" is not located on AOServer #"+aoServer);

            long[] modes=CvsRepository.getValidModes();
            boolean found=false;
            for(int c=0;c<modes.length;c++) {
                if(modes[c]==mode) {
                found=true;
                break;
                }
            }
            if(!found) throw new SQLException("Invalid mode: "+mode);

            // Update the database
            int pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('cvs_repositories_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  cvs_repositories\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  now(),\n"
                + "  null\n"
                + ")",
                pkey,
                path,
                lsa,
                lsg,
                mode
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.CVS_REPOSITORIES,
                LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
                aoServer,
                false
            );
            return pkey;
        }
    }

    /**
     * Disables a CVS repository.
     */
    public static void disableCvsRepository(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int disableLog,
        int pkey
    ) throws IOException, SQLException {
        if(isCvsRepositoryDisabled(conn, pkey)) throw new SQLException("CvsRepository is already disabled: "+pkey);
        BusinessHandler.checkAccessDisableLog(conn, source, "disableCvsRepository", disableLog, false);
        int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
        LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "disableCvsRepository", lsa);

        conn.executeUpdate(
            "update cvs_repositories set disable_log=? where pkey=?",
            disableLog,
            pkey
        );

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CVS_REPOSITORIES,
            LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
            LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
            false
        );
    }

    public static void enableCvsRepository(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        int disableLog=getDisableLogForCvsRepository(conn, pkey);
        if(disableLog==-1) throw new SQLException("CvsRepository is already enabled: "+pkey);
        BusinessHandler.checkAccessDisableLog(conn, source, "enableCvsRepository", disableLog, true);
        int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
        LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "enableCvsRepository", lsa);
        if(LinuxAccountHandler.isLinuxServerAccountDisabled(conn, lsa)) throw new SQLException("Unable to enable CvsRepository #"+pkey+", LinuxServerAccount not enabled: "+lsa);

        conn.executeUpdate(
            "update cvs_repositories set disable_log=null where pkey=?",
            pkey
        );

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CVS_REPOSITORIES,
            LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
            LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
            false
        );
    }

    public static IntList getCvsRepositoriesForLinuxServerAccount(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntListQuery("select pkey from cvs_repositories where linux_server_account=?", pkey);
    }

    public static int getLinuxServerAccountForCvsRepository(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select linux_server_account from cvs_repositories where pkey=?", pkey);
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        if(tableID==SchemaTable.TableID.CVS_REPOSITORIES) {
            synchronized(CvsHandler.class) {
                disabledCvsRepositories.clear();
            }
        }
    }

    public static int getDisableLogForCvsRepository(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select coalesce(disable_log, -1) from cvs_repositories where pkey=?", pkey);
    }

    public static boolean isCvsRepositoryDisabled(DatabaseConnection conn, int pkey) throws IOException, SQLException {
	    synchronized(CvsHandler.class) {
            Integer I=Integer.valueOf(pkey);
            Boolean O=disabledCvsRepositories.get(I);
            if(O!=null) return O.booleanValue();
            boolean isDisabled=getDisableLogForCvsRepository(conn, pkey)!=-1;
            disabledCvsRepositories.put(I, isDisabled);
            return isDisabled;
	    }
    }

    public static void removeCvsRepository(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Security checks
        int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
        LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "removeCvsRepository", lsa);

        removeCvsRepository(conn, invalidateList, pkey);
    }

    public static void removeCvsRepository(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        // Grab values for later use
        int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
        int aoServer=LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa);

        // Update the database
        conn.executeUpdate("delete from cvs_repositories where pkey=?", pkey);

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CVS_REPOSITORIES,
            LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
            aoServer,
            false
        );
    }

    public static void setMode(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        long mode
    ) throws IOException, SQLException {
        // Security checks
        int lsa=getLinuxServerAccountForCvsRepository(conn, pkey);
        LinuxAccountHandler.checkAccessLinuxServerAccount(conn, source, "setMode", lsa);

        // Integrity checks
        long[] modes=CvsRepository.getValidModes();
        boolean found=false;
        for(int c=0;c<modes.length;c++) {
            if(modes[c]==mode) {
                found=true;
                break;
            }
        }
        if(!found) throw new SQLException("Invalid mode: "+mode);

        // Update the database
        conn.executeUpdate(
            "update cvs_repositories set mode=? where pkey=?",
            mode,
            pkey
        );

        invalidateList.addTable(
            conn,
            SchemaTable.TableID.CVS_REPOSITORIES,
            LinuxAccountHandler.getBusinessForLinuxServerAccount(conn, lsa),
            LinuxAccountHandler.getAOServerForLinuxServerAccount(conn, lsa),
            false
        );
    }
}