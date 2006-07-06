package com.aoindustries.aoserv.master;

/*
 * Copyright 2002-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.io.unix.*;
import com.aoindustries.md5.MD5;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>BackupHandler</code> manages the backup data.
 *
 * @author  AO Industries, Inc.
 */
public final class BackupHandler {

    public static final boolean OLD_BACKUP_ENABLED = false;

    private static final Object
        fileBackupsPKeySeqLock=new Object(),
        backupDataPKeySeqLock=new Object()
    ;

    private static final Map<Integer,Integer> backupPartitionServers=new HashMap<Integer,Integer>();
    public static int getAOServerForBackupPartition(
        BackupDatabaseConnection backupConn,
        int pkey
    ) throws IOException, SQLException {
        final int PROFILER_LEVEL=Profiler.FAST;
        Profiler.startProfile(PROFILER_LEVEL, BackupHandler.class, "getAOServerForBackupPartition(BackupDatabaseConnection,int)", null);
        try {
            Integer I=Integer.valueOf(pkey);
            synchronized(backupPartitionServers) {
                Integer O=backupPartitionServers.get(I);
                if(O!=null) return O.intValue();
                int aoServer=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select ao_server from backup_partitions where pkey=?", pkey);
                backupPartitionServers.put(I, Integer.valueOf(aoServer));
                return aoServer;
            }
        } finally {
            Profiler.endProfile(PROFILER_LEVEL);
        }
    }

    public static short addFileBackupDevice(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        long device,
        boolean can_backup,
        String description
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupDevice(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,long,boolean,String)", null);
        try {
            // Must be able to configure backups on at least one server
            if(
                conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      bs.pkey\n"
                    + "    from\n"
                    + "      usernames un,\n"
                    + "      packages pk,\n"
                    + "      business_servers bs\n"
                    + "    where\n"
                    + "      un.username=?\n"
                    + "      and un.package=pk.name\n"
                    + "      and pk.accounting=bs.accounting\n"
                    + "      and bs.can_configure_backup\n"
                    + "    limit 1\n"
                    + "  ) is null",
                    source.getUsername()
                )
            ) throw new SQLException("Must be able to configure backups on at least one server to be able to add a FileBackupDevice.");

            short pkey=backupConn.executeShortQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backup_devices_pkey_seq')");
            backupConn.executeUpdate(
                "insert into file_backup_devices values(?,?,?,?)",
                pkey,
                device,
                can_backup,
                description
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_DEVICES,
                InvalidateList.allBusinesses,
                InvalidateList.allServers,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addFileBackupSetting(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        String path,
        int packageNum,
        short backupLevel,
        short backupRetention,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupSetting(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,String,int,short,short,boolean)", null);
        try {
            // Must be able to configure backups on this server
            if(
                !conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  bs.can_configure_backup\n"
                    + "from\n"
                    + "  usernames un,\n"
                    + "  packages pk,\n"
                    + "  business_servers bs\n"
                    + "where\n"
                    + "  un.username=?\n"
                    + "  and un.package=pk.name\n"
                    + "  and pk.accounting=bs.accounting\n"
                    + "  and bs.server=?",
                    source.getUsername(),
                    server
                )
            ) throw new SQLException("Not allowed allowed to configure backup settings for server #"+server);

            path=path.trim();
            if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
            int slashPos=path.indexOf('/');
            if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
            if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

            PackageHandler.checkAccessPackage(conn, source, "addFileBackupSetting", packageNum);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backup_settings_pkey_seq')");
            conn.executeUpdate(
                "insert into file_backup_settings values(?,?,?,?,?,?,?)",
                pkey,
                server,
                path,
                packageNum,
                backupLevel,
                backupRetention,
                recurse
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_SETTINGS,
                InvalidateList.getCollection(PackageHandler.getBusinessForPackage(conn, packageNum)),
                InvalidateList.getServerCollection(conn, server),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addFileBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        String path,
        short device,
        long inode,
        int packageNum,
        long mode,
        int uid,
        int gid,
        int backupData,
        long md5_hi,
        long md5_lo,
        long modifyTime,
        short backupLevel,
        short backupRetention,
        String symlinkTarget,
        long deviceID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,String,short,long,int,long,int,int,int,long,long,long,short,short,String,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access file_backups");
            ServerHandler.checkAccessServer(conn, source, "add_file_backup", server);

            if(UnixFile.isRegularFile(mode)) {
                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, mustring);
                if(masterServers.length!=0) {
                    // Client should provide the appropriate MD5, as proof that it has access to the backup_data
                    if(
                        md5_hi!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_hi from backup_data where pkey=?", backupData)
                        || md5_lo!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_lo from backup_data where pkey=?", backupData)
                    ) throw new SQLException("addFileBackup attempt with mismatched MD5: backup_data.pkey="+backupData+", attempted md5_hi="+md5_hi+" and md5_lo="+md5_lo);
                }
            }

            // Resolve the pkey in file_paths
            int file_path=FilePathHandler.findOrAddFilePath(backupConn, path);

            int pkey;
            synchronized(fileBackupsPKeySeqLock) {
                pkey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backups_pkey_seq')");
            }
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into file_backups values(?,?,?,?,?,?,?,?,?,?,now(),?,null,?,?,?,?)");
            try {
                pstmt.setInt(1, pkey);
                pstmt.setInt(2, server);
                pstmt.setInt(3, file_path);
                if(device==-1) pstmt.setNull(4, Types.SMALLINT);
                else pstmt.setShort(4, device);
                if(inode==-1) pstmt.setNull(5, Types.BIGINT);
                else pstmt.setLong(5, inode);
                pstmt.setInt(6, packageNum);
                pstmt.setLong(7, mode);
                if(uid==-1) pstmt.setNull(8, Types.INTEGER);
                else pstmt.setInt(8, uid);
                if(gid==-1) pstmt.setNull(9, Types.INTEGER);
                else pstmt.setInt(9, gid);
                if(UnixFile.isRegularFile(mode)) pstmt.setNull(10, Types.INTEGER);
                else pstmt.setInt(10, backupData);
                if(modifyTime==-1) pstmt.setNull(11, Types.TIMESTAMP);
                else pstmt.setTimestamp(11, new Timestamp(modifyTime));
                pstmt.setShort(12, backupLevel);
                pstmt.setShort(13, backupRetention);
                pstmt.setString(14, SQLUtility.encodeString(symlinkTarget));
                if(deviceID==-1) pstmt.setNull(15, Types.BIGINT);
                else pstmt.setLong(15, deviceID);
                
                backupConn.incrementUpdateCount();
                pstmt.executeUpdate();
            } catch(SQLException err) {
                System.err.println("Error from update: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }

            fileBackupsAdded(conn, backupConn, invalidateList, new int[] {file_path}, server, new int[] {packageNum}, 1);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUPS,
                InvalidateList.getCollection(PackageHandler.getBusinessForPackage(conn, packageNum)),
                InvalidateList.getServerCollection(conn, server),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addFileBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        int batchSize,
        String[] paths,
        short[] devices,
        long[] inodes,
        int[] packages,
        long[] modes,
        int[] uids,
        int[] gids,
        int[] backupDatas,
        long[] md5_his,
        long[] md5_los,
        long[] modifyTimes,
        short[] backupLevels,
        short[] backupRetentions,
        String[] symlinkTargets,
        long[] deviceIDs
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,int,String[],short[],long[],int[],long[],int[],int[],int[],long[],long[],long[],short[],short[],String[],long[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if(mu==null) throw new SQLException("User "+mustring+" is not master user and may not access file_backups");
            ServerHandler.checkAccessServer(conn, source, "add_file_backup", server);

            com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, mustring);
            if(masterServers.length!=0) {
                // Make sure all the backup_datas and md5s match, as proof that the client has access to the backup data
                StringBuilder sql=new StringBuilder();
                sql.append("select pkey, md5_hi, md5_lo from backup_data where pkey in (");
                boolean didOne=false;
                for(int c=0;c<batchSize;c++) {
                    if(UnixFile.isRegularFile(modes[c])) {
                        if(didOne) sql.append(',');
                        else didOne=true;
                        sql.append(backupDatas[c]);
                    }
                }
                if(didOne) {
                    sql.append(')');
                    String sqlString=sql.toString();
                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).createStatement();
                    try {
                        backupConn.incrementQueryCount();
                        ResultSet results=stmt.executeQuery(sqlString);
                        try {
                            while(results.next()) {
                                int pkey=results.getInt(1);
                                long md5_hi=results.getLong(2);
                                long md5_lo=results.getLong(3);
                                boolean found=false;
                                for(int c=0;c<batchSize;c++) {
                                    if(UnixFile.isRegularFile(modes[c]) && pkey==backupDatas[c]) {
                                        if(
                                            md5_hi!=md5_his[c]
                                            || md5_lo!=md5_los[c]
                                        ) throw new SQLException("addFileBackups attempt with mismatched MD5: backup_data.pkey="+pkey+", attempted md5_hi="+md5_his[c]+" and md5_lo="+md5_los[c]);
                                        found=true;
                                        break;
                                    }
                                }
                                if(!found) throw new SQLException("pkey returned but not found in backupDatas: "+pkey);
                            }
                        } finally {
                            results.close();
                        }
                    } catch(SQLException err) {
                        System.err.println("Error from query: "+sqlString);
                        throw err;
                    } finally {
                        stmt.close();
                    }
                }
            }

            // Resolve the pkeys in file_paths
            int[] file_paths=FilePathHandler.findOrAddFilePaths(backupConn, paths, batchSize);

            // Get a consecutive block of pkeys
            int startPKey;
            synchronized(fileBackupsPKeySeqLock) {
                startPKey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backups_pkey_seq')");
                if(batchSize>1) {
                    backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select setval('file_backups_pkey_seq', ?)", startPKey+batchSize-1);
                }
            }

            // Add all of the file backups
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into file_backups values(?,?,?,?,?,?,?,?,?,?,now(),?,null,?,?,?,?);\n");
            pstmt.clearBatch();
            try {
                for(int c=0;c<batchSize;c++) {
                    pstmt.setInt(1, startPKey+c);
                    pstmt.setInt(2, server);
                    pstmt.setInt(3, file_paths[c]);
                    if(devices[c]==-1) pstmt.setNull(4, Types.SMALLINT);
                    else pstmt.setShort(4, devices[c]);
                    if(inodes[c]==-1) pstmt.setNull(5, Types.BIGINT);
                    else pstmt.setLong(5, inodes[c]);
                    pstmt.setInt(6, packages[c]);
                    pstmt.setLong(7, modes[c]);
                    if(uids[c]==-1) pstmt.setNull(8, Types.INTEGER);
                    else pstmt.setInt(8, uids[c]);
                    if(gids[c]==-1) pstmt.setNull(9, Types.INTEGER);
                    else pstmt.setInt(9, gids[c]);
                    int backupData=backupDatas[c];
                    if(backupData==-1) pstmt.setNull(10, Types.INTEGER);
                    else pstmt.setInt(10, backupData);
                    long modifyTime=modifyTimes[c];
                    if(modifyTime==-1) pstmt.setNull(11, Types.TIMESTAMP);
                    else pstmt.setTimestamp(11, new Timestamp(modifyTime));
                    pstmt.setShort(12, backupLevels[c]);
                    pstmt.setInt(13, backupRetentions[c]);
                    pstmt.setString(14, SQLUtility.encodeString(symlinkTargets[c]));
                    long deviceID=deviceIDs[c];
                    if(deviceID==-1) pstmt.setNull(15, Types.BIGINT);
                    else pstmt.setLong(15, deviceID);

                    pstmt.addBatch();
                }
                backupConn.incrementUpdateCount();
                pstmt.executeBatch();
            } catch(SQLException err) {
                System.err.println("Error from update: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }

            fileBackupsAdded(conn, backupConn, invalidateList, file_paths, server, packages, batchSize);

            // Notify all clients of the update
            for(int c=0;c<batchSize;c++) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.FILE_BACKUPS,
                    PackageHandler.getBusinessForPackage(conn, packages[c]),
                    server,
                    false
                );
            }
            return startPKey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int addFileBackupStat(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        InvalidateList invalidateList,
        int server,
        long startTime,
        long endTime,
        int scanned,
        int file_backup_attribute_matches,
        int not_matched_md5_files,
        int not_matched_md5_failures,
        int send_missing_backup_data_files,
        int send_missing_backup_data_failures,
        int temp_files,
        int temp_send_backup_data_files,
        int temp_failures,
        int added,
        int deleted,
        boolean is_successful
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupStat(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,long,long,int,int,int,int,int,int,int,int,int,int,int,boolean)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access file_backup_stats.");
            ServerHandler.checkAccessServer(conn, source, "add_file_backup_stat", server);

            int pkey = backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backup_stats_pkey_seq')");
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                "insert into\n"
                + "  file_backup_stats\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?\n"
                + ")"
            );
            try {
                pstmt.setInt(1, pkey);
                pstmt.setInt(2, server);
                pstmt.setTimestamp(3, new Timestamp(startTime));
                pstmt.setTimestamp(4, new Timestamp(endTime));
                pstmt.setInt(5, scanned);
                pstmt.setInt(6, file_backup_attribute_matches);
                pstmt.setInt(7, not_matched_md5_files);
                pstmt.setInt(8, not_matched_md5_failures);
                pstmt.setInt(9, send_missing_backup_data_files);
                pstmt.setInt(10, send_missing_backup_data_failures);
                pstmt.setInt(11, temp_files);
                pstmt.setInt(12, temp_send_backup_data_files);
                pstmt.setInt(13, temp_failures);
                pstmt.setInt(14, added);
                pstmt.setInt(15, deleted);
                pstmt.setBoolean(16, is_successful);

                backupConn.incrementUpdateCount();
                pstmt.executeUpdate();
            } catch(SQLException err) {
                System.err.println("Error from update: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_STATS,
                ServerHandler.getBusinessesForServer(conn, server),
                InvalidateList.getServerCollection(conn, server),
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Backs up a MasterDatabase.getDatabase().
     * TODO: Remove once all database backups use direct daemon connections.
     *
     * @return  the <code>pkey</code> of the <code>BackupData</code>
     */
    public static int backupDatabase(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        String filename,
        int commandCode,
        int param1
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, BackupHandler.class, "backupDatabase(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,String,String,int,int)", null);
        try {
            AOServDaemonConnector connector=DaemonHandler.getDaemonConnector(conn, server);

            // Establish the connection to the source
            AOServDaemonConnection sourceConn=connector.getConnection();
            try {
                CompressedDataOutputStream sourceOut=sourceConn.getOutputStream();
                sourceOut.writeCompressedInt(commandCode);
                sourceOut.writeCompressedInt(param1);
                sourceOut.flush();

                CompressedDataInputStream sourceIn=sourceConn.getInputStream();
                int sourceCode=sourceIn.read();
                if(sourceCode!=AOServDaemonProtocol.NEXT) {
                    if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(sourceIn.readUTF());
                    if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(sourceIn.readUTF());
                    throw new IOException("Unknown result: " + sourceCode);
                }
                long dataSize=sourceIn.readLong();
                long md5_hi=sourceIn.readLong();
                long md5_lo=sourceIn.readLong();

                Object[] OA=findOrAddBackupData(conn, backupConn, source, invalidateList, server, dataSize, md5_hi, md5_lo);
                int backupData=((Integer)OA[0]).intValue();
                boolean hasData=((Boolean)OA[1]).booleanValue();
                if(!hasData) {
                    sourceOut.writeBoolean(true);
                    sourceOut.flush();
                    boolean backupClosed=false;
                    BackupOutputStream backupOut=new BackupOutputStream(conn, backupConn, invalidateList, backupData, md5_hi, md5_lo, filename, true);
                    try {
                        byte[] buff=BufferManager.getBytes();
                        try {
                            while((sourceCode=sourceIn.read())==AOServDaemonProtocol.NEXT) {
                                int len=sourceIn.readShort();
                                sourceIn.readFully(buff, 0, len);
                                backupOut.write(buff, 0, len);
                            }
                        } finally {
                            BufferManager.release(buff);
                        }
                        if (sourceCode != AOServDaemonProtocol.DONE) {
                            if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(sourceIn.readUTF());
                            else if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(sourceIn.readUTF());
                            else throw new IOException("Unknown result: " + sourceCode);
                        }
                    } catch(IOException err) {
                        if(!backupClosed) {
                            backupOut.rollback();
                            backupClosed=true;
                        }
                    } catch(SQLException err) {
                        if(!backupClosed) {
                            backupOut.rollback();
                            backupClosed=true;
                        }
                    } finally {
                        if(!backupClosed) {
			    backupOut.flush();
			    backupOut.close();
			}
                    }
                } else {
                    sourceOut.writeBoolean(false);
                    sourceOut.flush();
                    sourceCode=sourceIn.read();
                    if(sourceCode!=AOServDaemonProtocol.DONE) {
                        if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(sourceIn.readUTF());
                        else if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(sourceIn.readUTF());
                        else throw new IOException("Unknown result: " + sourceCode);
                    }
                }
                return backupData;
            } catch(IOException err) {
                sourceConn.close();
                throw err;
            } finally {
                connector.releaseConnection(sourceConn);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public static boolean canAccessBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "canAccessBackupData(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser!=null) {
                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, username);
                if(masterServers.length==0) return true;
                return backupConn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select (\n"
                    + "  select\n"
                    + "    bd.pkey\n"
                    + "  from\n"
                    + "    backup_data bd\n"
                    + "  where\n"
                    + "    bd.pkey=?\n"
                    + "    and (\n"
                    + "      (\n"
                    + "        select\n"
                    + "          bd2.pkey\n"
                    + "        from\n"
                    + "          master_servers ms,\n"
                    + "          backup_partitions bp,\n"
                    + "          backup_data bd2\n"
                    + "        where\n"
                    + "          ms.username=?\n"
                    + "          and ms.server=bp.ao_server\n"
                    + "          and bp.pkey=bd2.backup_partition\n"
                    + "          and bd2.pkey=bd.pkey\n"
                    + "      ) is not null\n"
                    + "      or (\n"
                    + "        select\n"
                    + "          fb.backup_data\n"
                    + "        from\n"
                    + "          master_servers ms,\n"
                    + "          file_backups fb\n"
                    + "        where\n"
                    + "          ms.username=?\n"
                    + "          and ms.server=fb.server\n"
                    + "          and fb.backup_data=bd.pkey\n"
                    + "        limit 1\n"
                    + "      ) is not null\n"
                    + "      or (\n"
                    + "        select\n"
                    + "          ib.backup_data\n"
                    + "        from\n"
                    + "          master_servers ms,\n"
                    + "          interbase_backups ib\n"
                    + "        where\n"
                    + "          ms.username=?\n"
                    + "          and ms.server=ib.ao_server\n"
                    + "          and ib.backup_data=bd.pkey\n"
                    + "        limit 1\n"
                    + "      ) is not null\n"
                    + "      or (\n"
                    + "        select\n"
                    + "          mb.backup_data\n"
                    + "        from\n"
                    + "          master_servers ms,\n"
                    + "          mysql_backups mb\n"
                    + "        where\n"
                    + "          ms.username=?\n"
                    + "          and ms.server=mb.ao_server\n"
                    + "          and mb.backup_data=bd.pkey\n"
                    + "        limit 1\n"
                    + "      ) is not null\n"
                    + "      or (\n"
                    + "        select\n"
                    + "          pb.backup_data\n"
                    + "        from\n"
                    + "          master_servers ms,\n"
                    + "          postgres_servers ps,\n"
                    + "          postgres_backups pb\n"
                    + "        where\n"
                    + "          ms.username=?\n"
                    + "          and ms.server=ps.ao_server\n"
                    + "          and ps.pkey=pb.postgres_server\n"
                    + "          and pb.backup_data=bd.pkey\n"
                    + "        limit 1\n"
                    + "      ) is not null\n"
                    + "    )\n"
                    + ") is not null",
                    pkey,
                    username,
                    username,
                    username,
                    username,
                    username
                );
            }
            return backupConn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      bd.pkey\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "      packages pk2,\n"
                + "      file_backups fb,\n"
                + "      backup_data bd\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk1.name\n"
                + "      and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "      )\n"
                + "      and bu1.accounting=pk2.accounting\n"
                + "      and pk2.pkey=fb.package\n"
                + "      and fb.backup_data=bd.pkey\n"
                + "      and bd.pkey=?\n"
                + "    limit 1\n"
                + "  ) is not null or (\n"
                + "    select\n"
                + "      bd.pkey\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "      packages pk2,\n"
                + "      interbase_backups ib,\n"
                + "      backup_data bd\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk1.name\n"
                + "      and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "      )\n"
                + "      and bu1.accounting=pk2.accounting\n"
                + "      and pk2.pkey=ib.package\n"
                + "      and ib.backup_data=bd.pkey\n"
                + "      and bd.pkey=?\n"
                + "    limit 1\n"
                + "  ) is not null or (\n"
                + "    select\n"
                + "      bd.pkey\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "      packages pk2,\n"
                + "      mysql_backups mb,\n"
                + "      backup_data bd\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk1.name\n"
                + "      and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "      )\n"
                + "      and bu1.accounting=pk2.accounting\n"
                + "      and pk2.pkey=mb.package\n"
                + "      and mb.backup_data=bd.pkey\n"
                + "      and bd.pkey=?\n"
                + "    limit 1\n"
                + "  ) is not null or (\n"
                + "    select\n"
                + "      bd.pkey\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "      packages pk2,\n"
                + "      postgres_backups pb,\n"
                + "      backup_data bd\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk1.name\n"
                + "      and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "      )\n"
                + "      and bu1.accounting=pk2.accounting\n"
                + "      and pk2.pkey=pb.package\n"
                + "      and pb.backup_data=bd.pkey\n"
                + "      and bd.pkey=?\n"
                + "    limit 1\n"
                + "  ) is not null",
                username,
                pkey,
                username,
                pkey,
                username,
                pkey,
                username,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canAccessFileBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "canAccessFileBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser!=null) {
                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, username);
                if(masterServers.length==0) return true;
                return backupConn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  (\n"
                    + "    select\n"
                    + "      fb.pkey\n"
                    + "    from\n"
                    + "      master_servers ms,\n"
                    + "      file_backups fb\n"
                    + "    where\n"
                    + "      ms.username=?\n"
                    + "      and ms.server=fb.server\n"
                    + "      and fb.pkey=?\n"
                    + "  ) is not null",
                    username,
                    pkey
                );
            }
            return backupConn.executeBooleanQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  (\n"
                + "    select\n"
                + "      fb.pkey\n"
                + "    from\n"
                + "      usernames un,\n"
                + "      packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "      packages pk2,\n"
                + "      file_backups fb\n"
                + "    where\n"
                + "      un.username=?\n"
                + "      and un.package=pk1.name\n"
                + "      and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "      )\n"
                + "      and bu1.accounting=pk2.accounting\n"
                + "      and pk2.pkey=fb.package\n"
                + "      and fb.pkey=?\n"
                + "  ) is not null",
                username,
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int canAccessFileBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int[] pkeys,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "canAccessFileBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int[],int)", null);
        try {
            // Build query
            String sqlStart;
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser!=null) {
                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, username);
                if(masterServers.length==0) return -1;

                sqlStart=
                    "select\n"
                    + "  fb.pkey\n"
                    + "from\n"
                    + "  master_servers ms,\n"
                    + "  file_backups fb\n"
                    + "where\n"
                    + "  ms.username=?\n"
                    + "  and ms.server=fb.server\n"
                    + "  and fb.pkey in ("
                ;
            } else sqlStart=
                "select\n"
                + "  fb.pkey\n"
                + "from\n"
                + "  usernames un,\n"
                + "  packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  packages pk2,\n"
                + "  file_backups fb\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and un.package=pk1.name\n"
                + "  and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=pk2.accounting\n"
                + "  and pk2.pkey=fb.package\n"
                + "  and fb.pkey in ("
            ;
            
            IntList sortedPKeys=new SortedIntArrayList();
            StringBuilder sql=new StringBuilder();
            sql.append(sqlStart);
            
            for(int c=0;c<batchSize;c++) {
                int pkey=pkeys[c];
                if(!sortedPKeys.contains(pkey)) {
                    sortedPKeys.add(pkey);
                    
                    if(c>0) sql.append(',');
                    sql.append(pkey);
                }
            }
            sql.append(')');
            
            // Execute query
            IntList accessiblePKeys=backupConn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, sql.toString(), username);
            
            // Match results
            int len=accessiblePKeys.size();
            for(int c=0;c<len;c++) {
                sortedPKeys.remove(accessiblePKeys.getInt(c));
            }
            
            // Return results
            return sortedPKeys.size()==0?-1:sortedPKeys.getInt(0);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        String action,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BackupHandler.class, "checkAccessBackupData(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessBackupData(conn, backupConn, source, pkey)) {
                String message=
                    "business_administrator.username="
                    + source.getUsername()
                    + " is not allowed to access backup_data: action='"
                    + action
                    + "', pkey="
                    + pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static void checkAccessFileBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        String action,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "checkAccessFileBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessFileBackup(conn, backupConn, source, pkey)) {
                String message=
                    "business_administrator.username="
                    + source.getUsername()
                    + " is not allowed to access file_backups: action='"
                    + action
                    + ", pkey="
                    + pkey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessFileBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        String action,
        int[] pkeys,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "checkAccessFileBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,String,int[],int)", null);
        try {
            int badPKey=canAccessFileBackups(conn, backupConn, source, pkeys, batchSize);
            if(badPKey!=-1) {
                String message=
                    "business_administrator.username="
                    + source.getUsername()
                    + " is not allowed to access file_backups: action='"
                    + action
                    + ", pkey="
                    + badPKey
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static final int BLOCK_SIZE=100;

    public static Object[] findLatestFileBackupSetAttributeMatches(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int server,
        int batchSize,
        String[] paths,
        short[] devices,
        long[] inodes,
        int[] packages,
        long[] modes,
        int[] uids,
        int[] gids,
        long[] modify_times,
        short[] backup_levels,
        short[] backup_retentions,
        long[] lengths,
        String[] symlink_targets,
        long[] device_ids
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "findLatestFileBackupSetAttributeMatches(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int,int,String[],short[],long[],int[],long[],int[],int[],long[],short[],short[],long[],String[],long[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not find file backups");
            ServerHandler.checkAccessServer(conn, source, "findLatestFileBackupSetAttributeMatches", server);

            final String backupFarm = getBackupFarmForServer(conn, server);

            int[] fileBackups=new int[batchSize];
            Arrays.fill(fileBackups, -1);
            int[] backupDatas=new int[batchSize];
            long[] md5_his=new long[batchSize];
            long[] md5_los=new long[batchSize];
            boolean[] hasDatas=new boolean[batchSize];

            // Buffer reused in following loops
            StringBuilder sql=new StringBuilder();

            // Shortcuts are used to save repetative loops when certain file types do not appear in file list
            boolean hasRegularFile=false;
            boolean hasSymbolicLink=false;
            boolean hasDeviceFile=false;
            boolean hasOtherType=false;

            // Resolve the pkeys in file_paths.  If doesn't exist in file_paths, then cannot exist in file_backups
            int[] file_paths=FilePathHandler.findFilePaths(backupConn, paths, batchSize);

            // Make sure there are no duplicate paths while building a list of path indexes
            // This stores a mapping of paths to indexes in return arrays
            Map<Integer,Integer> pathIndexes=new HashMap<Integer,Integer>(batchSize);
            for(int c=0;c<batchSize;c++) {
                int file_path=file_paths[c];
                if(file_path!=-1) {
                    if(pathIndexes.put(Integer.valueOf(file_path), Integer.valueOf(c))!=null) throw new SQLException("Batch contained a path more than once: "+paths[c]+" (file_path="+file_path+")");
                    long mode=modes[c];
                    if(UnixFile.isRegularFile(mode)) hasRegularFile=true;
                    else if(UnixFile.isSymLink(mode)) hasSymbolicLink=true;
                    else if(UnixFile.isBlockDevice(mode) || UnixFile.isCharacterDevice(mode)) hasDeviceFile=true;
                    else hasOtherType=true;
                }
            }

            short[] blockDevices=new short[BLOCK_SIZE];
            long[] blockInodes=new long[BLOCK_SIZE];
            int[] blockFilePaths=new int[BLOCK_SIZE];
            int[] blockPackages=new int[BLOCK_SIZE];
            long[] blockModes=new long[BLOCK_SIZE];
            int[] blockUIDs=new int[BLOCK_SIZE];
            int[] blockGIDs=new int[BLOCK_SIZE];
            long[] blockModifyTimes=new long[BLOCK_SIZE];
            short[] blockBackupLevels=new short[BLOCK_SIZE];
            short[] blockBackupRetentions=new short[BLOCK_SIZE];
            long[] blockLengths=new long[BLOCK_SIZE];

            // Process the regular files
            if(hasRegularFile) {
                int blockSize=0;
                int lastBlockSize=-1;
                String lastBlockSQL=null;
                for(int c=0;c<=batchSize;c++) {
                    if(c==batchSize || file_paths[c]!=-1) {
                        long mode=c==batchSize?-1:modes[c];
                        if(c==batchSize || UnixFile.isRegularFile(mode)) {
                            if(c!=batchSize) {
                                blockFilePaths[blockSize]=file_paths[c];
                                blockDevices[blockSize]=devices[c];
                                blockInodes[blockSize]=inodes[c];
                                blockPackages[blockSize]=packages[c];
                                blockModes[blockSize]=mode;
                                blockUIDs[blockSize]=uids[c];
                                blockGIDs[blockSize]=gids[c];
                                blockModifyTimes[blockSize]=modify_times[c];
                                blockBackupLevels[blockSize]=backup_levels[c];
                                blockBackupRetentions[blockSize]=backup_retentions[c];
                                blockLengths[blockSize]=lengths[c];

                                blockSize++;
                            }
                            if(blockSize>0 && (blockSize>=BLOCK_SIZE || c==batchSize)) {
                                String sqlString;
                                if(lastBlockSQL==null || blockSize!=lastBlockSize) {
                                    sql.setLength(0);
                                    sql.append(
                                        "select\n"
                                        + "  fbp.file_path,\n"
                                        + "  fbp.pkey,\n"
                                        + "  fbp.backup_data,\n"
                                        + "  bd.md5_hi,\n"
                                        + "  bd.md5_lo,\n"
                                        + "  bd.is_stored\n"
                                        + "from\n"
                                        + "  (\n"
                                        + "    select\n"
                                        + "      file_path,\n"
                                        + "      modify_time,\n"
                                        + "      pkey,\n"
                                        + "      backup_data\n"
                                        + "    from\n"
                                        + "      file_backups\n"
                                        + "    where\n"
                                        + "      (\n"
                                    );
                                    for(int d=0;d<blockSize;d++) {
                                        sql.append("        ");
                                        if(d!=0) sql.append("or ");
                                        sql.append("(file_path=? and server=? and ").append(blockDevices[d]==-1?"device is null":"device=?::smallint").append(" and ").append(blockInodes[d]==-1?"inode is null":"inode=?::int8").append(" and package=? and mode=?::int8 and ").append(blockUIDs[d]==-1?"uid is null":"uid=?").append(" and ").append(blockGIDs[d]==-1?"gid is null":"gid=?").append(" and modify_time=? and backup_level=?::smallint and backup_retention=?::smallint)\n");
                                    }
                                    sql.append(
                                        "      ) and remove_time is null\n"
                                        + "  ) fbp,\n"
                                        + "  backup_data bd,\n"
                                        + "  backup_partitions bp,\n"
                                        + "  servers se\n"
                                        + "where\n"
                                        + "  fbp.backup_data=bd.pkey\n"
                                        + "  and bd.backup_partition=bp.pkey\n"
                                        + "  and bp.enabled\n"
                                        + "  and bp.ao_server=se.pkey\n"
                                        + "  and se.farm=?\n"
                                        + "  and (\n"
                                    );
                                    for(int d=0;d<blockSize;d++) {
                                        sql.append("    ");
                                        if(d!=0) sql.append("or ");
                                        sql.append("(fbp.file_path=? and bd.data_size=?::int8)\n");
                                    }
                                    sql.append("  )");
                                    lastBlockSQL=sqlString=sql.toString();
                                    lastBlockSize=blockSize;
                                } else sqlString=lastBlockSQL;
                                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sqlString);
                                try {
                                    int pos=1;
                                    for(int d=0;d<blockSize;d++) {
                                        pstmt.setInt(pos++, blockFilePaths[d]);
                                        pstmt.setInt(pos++, server);
                                        if(blockDevices[d]!=-1) pstmt.setShort(pos++, blockDevices[d]);
                                        if(blockInodes[d]!=-1) pstmt.setLong(pos++, blockInodes[d]);
                                        pstmt.setInt(pos++, blockPackages[d]);
                                        pstmt.setLong(pos++, blockModes[d]);
                                        if(blockUIDs[d]!=-1) pstmt.setInt(pos++, blockUIDs[d]);
                                        if(blockGIDs[d]!=-1) pstmt.setInt(pos++, blockGIDs[d]);
                                        pstmt.setTimestamp(pos++, new Timestamp(blockModifyTimes[d]));
                                        pstmt.setShort(pos++, blockBackupLevels[d]);
                                        pstmt.setShort(pos++, blockBackupRetentions[d]);
                                    }
                                    pstmt.setString(pos++, backupFarm);
                                    for(int d=0;d<blockSize;d++) {
                                        pstmt.setInt(pos++, blockFilePaths[d]);
                                        pstmt.setLong(pos++, blockLengths[d]);
                                    }

                                    backupConn.incrementQueryCount();
                                    ResultSet results=pstmt.executeQuery();
                                    try {
                                        while(results.next()) {
                                            int file_path=results.getInt(1);
                                            int index=pathIndexes.get(Integer.valueOf(file_path)).intValue();
                                            if(fileBackups[index]==-1) {
                                                fileBackups[index]=results.getInt(2);
                                                backupDatas[index]=results.getInt(3);
                                                md5_his[index]=results.getLong(4);
                                                md5_los[index]=results.getLong(5);
                                                hasDatas[index]=results.getBoolean(6);
                                            }
                                        }
                                    } finally {
                                        results.close();
                                    }
                                } catch(SQLException err) {
                                    System.err.println("Error from query: "+pstmt);
                                    throw err;
                                } finally {
                                    pstmt.close();
                                }
                                blockSize=0;
                            }
                        }
                    }
                }
            }

            // Process the symbolic links
            if(hasSymbolicLink) {
                String[] blockSymlinkTargets=new String[BLOCK_SIZE];

                int blockSize=0;
                int lastBlockSize=-1;
                String lastBlockSQL=null;
                for(int c=0;c<=batchSize;c++) {
                    if(c==batchSize || file_paths[c]!=-1) {
                        long mode=c==batchSize?-1:modes[c];
                        if(c==batchSize || UnixFile.isSymLink(mode)) {
                            if(c!=batchSize) {
                                blockFilePaths[blockSize]=file_paths[c];
                                blockDevices[blockSize]=devices[c];
                                blockInodes[blockSize]=inodes[c];
                                blockPackages[blockSize]=packages[c];
                                blockModes[blockSize]=mode;
                                blockUIDs[blockSize]=uids[c];
                                blockGIDs[blockSize]=gids[c];
                                blockBackupLevels[blockSize]=backup_levels[c];
                                blockBackupRetentions[blockSize]=backup_retentions[c];
                                blockSymlinkTargets[blockSize]=symlink_targets[c];

                                blockSize++;
                            }
                            if(blockSize>0 && (blockSize>=BLOCK_SIZE || c==batchSize)) {
                                String sqlString;
                                if(lastBlockSQL==null || blockSize!=lastBlockSize) {
                                    sql.setLength(0);
                                    sql.append(
                                          "select\n"
                                        + "  file_path,\n"
                                        + "  pkey\n"
                                        + "from\n"
                                        + "  file_backups\n"
                                        + "where\n"
                                        + "  (\n"
                                    );
                                    for(int d=0;d<blockSize;d++) {
                                        sql.append("    ");
                                        if(d!=0) sql.append("or ");
                                        sql.append("(file_path=? and server=? and ").append(blockDevices[d]==-1?"device is null":"device=?::smallint").append(" and ").append(blockInodes[d]==-1?"inode is null":"inode=?::int8").append(" and package=? and mode=?::int8 and ").append(blockUIDs[d]==-1?"uid is null":"uid=?").append(" and ").append(blockGIDs[d]==-1?"gid is null":"gid=?").append(" and backup_level=?::smallint and backup_retention=?::smallint and symlink_target=?)\n");
                                    }
                                    sql.append("  ) and remove_time is null");
                                    lastBlockSQL=sqlString=sql.toString();
                                    lastBlockSize=blockSize;
                                } else sqlString=lastBlockSQL;
                                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sqlString);
                                try {
                                    int pos=1;
                                    for(int d=0;d<blockSize;d++) {
                                        pstmt.setInt(pos++, blockFilePaths[d]);
                                        pstmt.setInt(pos++, server);
                                        if(blockDevices[d]!=-1) pstmt.setShort(pos++, blockDevices[d]);
                                        if(blockInodes[d]!=-1) pstmt.setLong(pos++, blockInodes[d]);
                                        pstmt.setInt(pos++, blockPackages[d]);
                                        pstmt.setLong(pos++, blockModes[d]);
                                        if(blockUIDs[d]!=-1) pstmt.setInt(pos++, blockUIDs[d]);
                                        if(blockGIDs[d]!=-1) pstmt.setInt(pos++, blockGIDs[d]);
                                        pstmt.setShort(pos++, blockBackupLevels[d]);
                                        pstmt.setShort(pos++, blockBackupRetentions[d]);
                                        pstmt.setString(pos++, SQLUtility.encodeString(blockSymlinkTargets[d]));
                                    }

                                    backupConn.incrementQueryCount();
                                    ResultSet results=pstmt.executeQuery();
                                    try {
                                        while(results.next()) {
                                            int file_path=results.getInt(1);
                                            int index=pathIndexes.get(Integer.valueOf(file_path)).intValue();
                                            if(fileBackups[index]==-1) {
                                                fileBackups[index]=results.getInt(2);
                                            }
                                        }
                                    } finally {
                                        results.close();
                                    }
                                } catch(SQLException err) {
                                    System.err.println("Error from query: "+pstmt);
                                    throw err;
                                } finally {
                                    pstmt.close();
                                }
                                blockSize=0;
                            }
                        }
                    }
                }
            }

            // Process the device files
            if(hasDeviceFile) {
                long[] blockDeviceIDs=new long[BLOCK_SIZE];

                int blockSize=0;
                int lastBlockSize=-1;
                String lastBlockSQL=null;
                for(int c=0;c<=batchSize;c++) {
                    if(c==batchSize || file_paths[c]!=-1) {
                        long mode=c==batchSize?-1:modes[c];
                        if(c==batchSize || UnixFile.isBlockDevice(mode) || UnixFile.isCharacterDevice(mode)) {
                            if(c!=batchSize) {
                                blockFilePaths[blockSize]=file_paths[c];
                                blockDevices[blockSize]=devices[c];
                                blockInodes[blockSize]=inodes[c];
                                blockPackages[blockSize]=packages[c];
                                blockModes[blockSize]=mode;
                                blockUIDs[blockSize]=uids[c];
                                blockGIDs[blockSize]=gids[c];
                                blockBackupLevels[blockSize]=backup_levels[c];
                                blockBackupRetentions[blockSize]=backup_retentions[c];
                                blockDeviceIDs[blockSize]=device_ids[c];

                                blockSize++;
                            }
                            if(blockSize>0 && (blockSize>=BLOCK_SIZE || c==batchSize)) {
                                String sqlString;
                                if(lastBlockSQL==null || blockSize!=lastBlockSize) {
                                    sql.setLength(0);
                                    sql.append(
                                          "select\n"
                                        + "  file_path,\n"
                                        + "  pkey\n"
                                        + "from\n"
                                        + "  file_backups\n"
                                        + "where\n"
                                        + "  (\n"
                                    );
                                    for(int d=0;d<blockSize;d++) {
                                        sql.append("    ");
                                        if(d!=0) sql.append("or ");
                                        sql.append("(file_path=? and server=? and ").append(blockDevices[d]==-1?"device is null":"device=?::smallint").append(" and ").append(blockInodes[d]==-1?"inode is null":"inode=?::int8").append(" and package=? and mode=?::int8 and ").append(blockUIDs[d]==-1?"uid is null":"uid=?").append(" and ").append(blockGIDs[d]==-1?"gid is null":"gid=?").append(" and backup_level=?::smallint and backup_retention=?::smallint and device_id=?::int8)\n");
                                    }
                                    sql.append("  ) and remove_time is null");
                                    lastBlockSQL=sqlString=sql.toString();
                                    lastBlockSize=blockSize;
                                } else sqlString=lastBlockSQL;
                                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sqlString);
                                try {
                                    int pos=1;
                                    for(int d=0;d<blockSize;d++) {
                                        pstmt.setInt(pos++, blockFilePaths[d]);
                                        pstmt.setInt(pos++, server);
                                        if(blockDevices[d]!=-1) pstmt.setShort(pos++, blockDevices[d]);
                                        if(blockInodes[d]!=-1) pstmt.setLong(pos++, blockInodes[d]);
                                        pstmt.setInt(pos++, blockPackages[d]);
                                        pstmt.setLong(pos++, blockModes[d]);
                                        if(blockUIDs[d]!=-1) pstmt.setInt(pos++, blockUIDs[d]);
                                        if(blockGIDs[d]!=-1) pstmt.setInt(pos++, blockGIDs[d]);
                                        pstmt.setShort(pos++, blockBackupLevels[d]);
                                        pstmt.setShort(pos++, blockBackupRetentions[d]);
                                        pstmt.setLong(pos++, blockDeviceIDs[d]);
                                    }

                                    backupConn.incrementQueryCount();
                                    ResultSet results=pstmt.executeQuery();
                                    try {
                                        while(results.next()) {
                                            int file_path=results.getInt(1);
                                            int index=pathIndexes.get(Integer.valueOf(file_path)).intValue();
                                            if(fileBackups[index]==-1) {
                                                fileBackups[index]=results.getInt(2);
                                            }
                                        }
                                    } finally {
                                        results.close();
                                    }
                                } catch(SQLException err) {
                                    System.err.println("Error from query: "+pstmt);
                                    throw err;
                                } finally {
                                    pstmt.close();
                                }
                                blockSize=0;
                            }
                        }
                    }
                }
            }

            // Process all the other types
            if(hasOtherType) {
                int blockSize=0;
                int lastBlockSize=-1;
                String lastBlockSQL=null;
                for(int c=0;c<=batchSize;c++) {
                    if(c==batchSize || file_paths[c]!=-1) {
                        long mode=c==batchSize?-1:modes[c];
                        if(
                            c==batchSize
                            || !(
                                UnixFile.isRegularFile(mode)
                                || UnixFile.isSymLink(mode)
                                || UnixFile.isBlockDevice(mode)
                                || UnixFile.isCharacterDevice(mode)
                            )
                        ) {
                            if(c!=batchSize) {
                                blockFilePaths[blockSize]=file_paths[c];
                                blockDevices[blockSize]=devices[c];
                                blockInodes[blockSize]=inodes[c];
                                blockPackages[blockSize]=packages[c];
                                blockModes[blockSize]=mode;
                                blockUIDs[blockSize]=uids[c];
                                blockGIDs[blockSize]=gids[c];
                                blockBackupLevels[blockSize]=backup_levels[c];
                                blockBackupRetentions[blockSize]=backup_retentions[c];

                                blockSize++;
                            }
                            if(blockSize>0 && (blockSize>=BLOCK_SIZE || c==batchSize)) {
                                String sqlString;
                                if(lastBlockSQL==null || blockSize!=lastBlockSize) {
                                    sql.setLength(0);
                                    sql.append(
                                          "select\n"
                                        + "  file_path,\n"
                                        + "  pkey\n"
                                        + "from\n"
                                        + "  file_backups\n"
                                        + "where\n"
                                        + "  (\n"
                                    );
                                    for(int d=0;d<blockSize;d++) {
                                        sql.append("    ");
                                        if(d!=0) sql.append("or ");
                                        sql.append("(file_path=? and server=? and ").append(blockDevices[d]==-1?"device is null":"device=?::smallint").append(" and ").append(blockInodes[d]==-1?"inode is null":"inode=?::int8").append(" and package=? and mode=?::int8 and ").append(blockUIDs[d]==-1?"uid is null":"uid=?").append(" and ").append(blockGIDs[d]==-1?"gid is null":"gid=?").append(" and backup_level=?::smallint and backup_retention=?::smallint)\n");
                                    }
                                    sql.append("  ) and remove_time is null");
                                    lastBlockSQL=sqlString=sql.toString();
                                    lastBlockSize=blockSize;
                                } else sqlString=lastBlockSQL;
                                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sqlString);
                                try {
                                    int pos=1;
                                    for(int d=0;d<blockSize;d++) {
                                        pstmt.setInt(pos++, blockFilePaths[d]);
                                        pstmt.setInt(pos++, server);
                                        if(blockDevices[d]!=-1) pstmt.setShort(pos++, blockDevices[d]);
                                        if(blockInodes[d]!=-1) pstmt.setLong(pos++, blockInodes[d]);
                                        pstmt.setInt(pos++, blockPackages[d]);
                                        pstmt.setLong(pos++, blockModes[d]);
                                        if(blockUIDs[d]!=-1) pstmt.setInt(pos++, blockUIDs[d]);
                                        if(blockGIDs[d]!=-1) pstmt.setInt(pos++, blockGIDs[d]);
                                        pstmt.setShort(pos++, blockBackupLevels[d]);
                                        pstmt.setShort(pos++, blockBackupRetentions[d]);
                                    }

                                    backupConn.incrementQueryCount();
                                    ResultSet results=pstmt.executeQuery();
                                    try {
                                        while(results.next()) {
                                            int file_path=results.getInt(1);
                                            int index=pathIndexes.get(Integer.valueOf(file_path)).intValue();
                                            if(fileBackups[index]==-1) {
                                                fileBackups[index]=results.getInt(2);
                                            }
                                        }
                                    } finally {
                                        results.close();
                                    }
                                } catch(SQLException err) {
                                    System.err.println("Error from query: "+pstmt);
                                    throw err;
                                } finally {
                                    pstmt.close();
                                }
                                blockSize=0;
                            }
                        }
                    }
                }
            }

            Object[] OA={
                fileBackups,
                backupDatas,
                md5_his,
                md5_los,
                hasDatas
            };
            return OA;
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }
    
    public static Object[] findOrAddBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        long length,
        long md5_hi,
        long md5_lo
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "findOrAddBackupData(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,long,long,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not find existing backup_data");
            ServerHandler.checkAccessServer(conn, source, "find_existing_backup_data", server);
            
            int pkey;
            boolean hasData;
            int backupServer;
            int backupPartition;
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                "select\n"
                + "  bd.pkey,\n"
                + "  bd.is_stored,\n"
                + "  bp.ao_server,\n"
                + "  bp.pkey\n"
                + "from\n"
                + "  backup_data bd,\n"
                + "  backup_partitions bp,\n"
                + "  servers bse,\n"
                + "  server_farms sf,\n"
                + "  servers se\n"
                + "where\n"
                + "  bd.md5_hi=?::int8\n"
                + "  and bd.md5_lo=?::int8\n"
                + "  and bd.data_size=?::int8\n"
                + "  and bd.backup_partition=bp.pkey\n"
                + "  and bp.enabled\n"
                + "  and bp.ao_server=bse.pkey\n"
                + "  and bse.farm=sf.backup_farm\n"
                + "  and sf.name=se.farm\n"
                + "  and se.pkey=?\n"
                + "  and (\n"
                + "    sf.allow_same_server_backup\n"
                + "    or se.pkey!=bp.ao_server\n"
                + "  )\n"
                + "limit 1"
            );
            try {
                pstmt.setLong(1, md5_hi);
                pstmt.setLong(2, md5_lo);
                pstmt.setLong(3, length);
                pstmt.setInt(4, server);

                backupConn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
                    if(results.next()) {
                        pkey=results.getInt(1);
                        hasData=results.getBoolean(2);
                        backupServer=results.getInt(3);
                        backupPartition=results.getInt(4);
                    } else {
                        pkey=-1;
                        hasData=false;
                        backupServer=-1;
                        backupPartition=-1;
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
            if(pkey==-1) {
                // Find the available partition
                backupPartition=getBackupPartition(conn, backupConn, server, length);
                backupServer=getAOServerForBackupPartition(backupConn, backupPartition);
                synchronized(backupDataPKeySeqLock) {
                    pkey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('backup_data_pkey_seq')");
                }
                backupConn.executeUpdate(
                    "insert into backup_data values(?,now(),?,?,null,?,?,false)",
                    pkey,
                    backupPartition,
                    length,
                    md5_hi,
                    md5_lo
                );

                // Notify all clients of the update
                invalidateList.addTable(
                    conn,
                    SchemaTable.BACKUP_DATA,
                    InvalidateList.allBusinesses,
                    server,
                    false
                );
            }
            Object[] OA=new Object[] {
                Integer.valueOf(pkey),
                hasData?Boolean.TRUE:Boolean.FALSE,
                Integer.valueOf(backupServer),
                Integer.valueOf(backupPartition)
            };
            return OA;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static Object[] findOrAddBackupDatas(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        int batchSize,
        long[] lengths,
        long[] md5_his,
        long[] md5_los
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "findOrAddBackupDatas(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,int,long[],long[],long[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not find existing backup_datas");
            ServerHandler.checkAccessServer(conn, source, "find_existing_backup_datas", server);

            int[] pkeys=new int[batchSize];
            boolean[] needsPKeys=new boolean[batchSize];
            boolean[] hasDatas=new boolean[batchSize];
            for(int c=0;c<batchSize;c++) {
                pkeys[c]=-1;
                needsPKeys[c]=true;
            }

            StringBuilder sql=new StringBuilder();
            sql.append(
                "select\n"
                + "  bd.pkey,\n"
                + "  bd.data_size,\n"
                + "  bd.md5_hi,\n"
                + "  bd.md5_lo,\n"
                + "  bd.is_stored\n"
                + "from\n"
                + "  backup_data bd,\n"
                + "  backup_partitions bp,\n"
                + "  servers bse,\n"
                + "  server_farms sf,\n"
                + "  servers se\n"
                + "where\n"
                + "  (\n"
            );
            for(int c=0;c<batchSize;c++) {
                sql.append("    ");
                if(c>0) sql.append("or ");
                sql.append("(bd.md5_hi=").append(md5_his[c]).append("::int8 and bd.md5_lo=").append(md5_los[c]).append("::int8 and bd.data_size=").append(lengths[c]).append("::int8)\n");
            }
            sql.append(
                "  )\n"
                + "  and bd.backup_partition=bp.pkey\n"
                + "  and bp.enabled\n"
                + "  and bp.ao_server=bse.pkey\n"
                + "  and bse.farm=sf.backup_farm\n"
                + "  and sf.name=se.farm\n"
                + "  and se.pkey=").append(server).append("\n"
                + "  and (\n"
                + "    sf.allow_same_server_backup\n"
                + "    or se.pkey!=bp.ao_server\n"
                + "  )"
            );
            String sqlString=sql.toString();
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString);
                try {
                    while(results.next()) {
                        long data_size=results.getLong(2);
                        long md5_hi=results.getLong(3);
                        long md5_lo=results.getLong(4);

                        for(int c=0;c<batchSize;c++) {
                            if(
                                needsPKeys[c]
                                && lengths[c]==data_size
                                && md5_his[c]==md5_hi
                                && md5_los[c]==md5_lo
                            ) {
                                pkeys[c]=results.getInt(1);
                                needsPKeys[c]=false;
                                hasDatas[c]=results.getBoolean(5);
                            }
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }

            // Get the backup partition for the sum of the disk space
            boolean needsOnePKey=false;
            long totalLength=0;
            for(int c=0;c<batchSize;c++) {
                if(needsPKeys[c]) {
                    totalLength+=lengths[c];
                    needsOnePKey=true;
                }
            }
            if(needsOnePKey) {
                int backupPartition=getBackupPartition(conn, backupConn, server, totalLength);

                // Get the new pkeys
                synchronized(backupDataPKeySeqLock) {
                    int startVal=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('backup_data_pkey_seq')");
                    int nextVal=startVal;
                    for(int c=0;c<batchSize;c++) {
                        if(needsPKeys[c]) pkeys[c]=nextVal++;
                    }
                    int newVal=nextVal-1;
                    if(newVal>startVal) backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select setval('backup_data_pkey_seq', ?)", newVal);
                }

                // Add the new rows in a batch
                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into backup_data values(?,now(),?,?,null,?,?,false);\n");
                pstmt.clearBatch();
                try {
                    for(int c=0;c<batchSize;c++) {
                        if(needsPKeys[c]) {
                            pstmt.setInt(1, pkeys[c]);
                            pstmt.setInt(2, backupPartition);
                            pstmt.setLong(3, lengths[c]);
                            pstmt.setLong(4, md5_his[c]);
                            pstmt.setLong(5, md5_los[c]);

                            pstmt.addBatch();
                        }
                    }
                    backupConn.incrementUpdateCount();
                    pstmt.executeBatch();
                } catch(SQLException err) {
                    System.err.println("Error from update: "+pstmt.toString());
                    throw err;
                } finally {
                    pstmt.close();
                }

                // Notify all clients of the update
                invalidateList.addTable(
                    conn,
                    SchemaTable.BACKUP_DATA,
                    InvalidateList.allBusinesses,
                    InvalidateList.getServerCollection(conn, server),
                    false
                );
            }
            
            Object[] OA=new Object[] {
                pkeys,
                hasDatas
            };
            return OA;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void flagFileBackupsAsDeleted(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int batchSize,
        int[] pkeys
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "flagFileBackupsAsDeleted(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,int[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if(mu==null) throw new SQLException("User "+mustring+" is not master user and may not flag file backups as deleted");

            // Check access on all
            checkAccessFileBackups(conn, backupConn, source, "flagFileBackupsAsDeleted", pkeys, batchSize);

            // Update the data in a batch
            StringBuilder sql=new StringBuilder();
            sql.append("update file_backups set remove_time=now() where pkey in (");
            for(int c=0;c<batchSize;c++) {
                if(c>0) sql.append(',');
                sql.append(pkeys[c]);
            }
            sql.append(')');
            backupConn.executeUpdate(sql.toString());

            // Add the invalidations
            List<String> bus=getBusinessesForFileBackups(backupConn, pkeys, batchSize);
            IntList ses=getServersForFileBackups(backupConn, pkeys, batchSize);

            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUPS,
                bus,
                InvalidateList.getServerCollection(conn, ses),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void flagBackupDataAsStored(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int backupData,
        boolean isCompressed,
        long compressedSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "flagBackupDataAsStored(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int,boolean,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            // Must be a master user who can access this server
            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if(mu==null) throw new SQLException("User "+mustring+" is not master user and may not flag backup data as stored");
            
            int backupPartition = backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select backup_partition from backup_data where pkey=?", backupData);
            int aoServer = getAOServerForBackupPartition(backupConn, backupPartition);
            ServerHandler.checkAccessServer(conn, source, "flagBackupDataAsStored", aoServer);

            // Update the data in a batch
            backupConn.executeUpdate(
                isCompressed
                ? ("update backup_data set compressed_size="+compressedSize+", is_stored=true where pkey="+backupData)
                : ("update backup_data set is_stored=true where pkey="+backupData)
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getBackupDatasForBackupPartition(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        CompressedDataOutputStream out,
        int backupPartition,
        boolean hasDataOnly
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "getBackupDatasForBackupPartition(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,boolean)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            // Security checks
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to get the list of backup data for one backup partition.");
            ServerHandler.checkAccessServer(conn, source, "getBackupDatasForBackupPartition", getAOServerForBackupPartition(backupConn, backupPartition));

            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).createStatement();
            try {
                // First, make a cursor of all the pkeys sorted
                backupConn.incrementUpdateCount();
                stmt.executeUpdate(sqlString=
                    hasDataOnly
                    ?(
                        "declare get_backup_datas_for_bp cursor for select\n"
                        + "  pkey,\n"
                        + "  coalesce(compressed_size, data_size) as data_size\n"
                        + "from\n"
                        + "  backup_data\n"
                        + "where\n"
                        + "  backup_partition="+backupPartition+"\n"
                        + "  and is_stored\n"
                        + "order by\n"
                        + "  pkey"
                    ):(
                        "declare get_backup_datas_for_bp cursor for select\n"
                        + "  pkey,\n"
                        + "  coalesce(compressed_size, data_size) as data_size\n"
                        + "from\n"
                        + "  backup_data\n"
                        + "where\n"
                        + "  backup_partition="+backupPartition+"\n"
                        + "order by\n"
                        + "  pkey"
                    )
                );
                try {
                    out.writeByte(AOServProtocol.DONE);
                    sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from get_backup_datas_for_bp";
                    int lastpkey=0;
                    while(true) {
                        int batchSize=0;
                        backupConn.incrementQueryCount();
                        ResultSet results=stmt.executeQuery(sqlString);
                        try {
                            while(results.next()) {
                                int pkey=results.getInt(1);
                                out.writeCompressedInt(pkey-lastpkey);
                                lastpkey=pkey;
                                out.writeLong(results.getLong(2));
                                batchSize++;
                            }
                        } finally {
                            results.close();
                        }
                        if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                    }
                    out.writeCompressedInt(-1);
                } finally {
                    // Remove the cursor
                    backupConn.incrementUpdateCount();
                    stmt.executeUpdate("close get_backup_datas_for_bp");
                }
            } catch(SQLException err) {
                System.err.print("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static void getBackupDataPKeys(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean hasDataOnly,
        short minBackupLevel
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "getBackupDataPKeys(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,short)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to get the complete backup data set.");
            com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, username);
            if(masterServers.length!=0) throw new SQLException("Master user must have access to all servers to get the complete backup data set.");

            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).createStatement();
            try {
                // First, make a cursor of all the pkeys sorted
                backupConn.incrementUpdateCount();
                stmt.executeUpdate(sqlString=
                    hasDataOnly
                    ?(
                        minBackupLevel>BackupLevel.DO_NOT_BACKUP
                        ?(
                            "declare get_backup_data_pkeys cursor for select distinct\n"
                            + "  bd.pkey,\n"
                            + "  coalesce(bd.compressed_size, bd.data_size) as data_size\n"
                            + "from\n"
                            + "  (\n"
                            + "    select backup_data from file_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from interbase_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from mysql_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from postgres_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "  ) backups,\n"
                            + "  backup_data bd\n"
                            + "where\n"
                            + "  backups.backup_data=bd.pkey\n"
                            + "  and bd.is_stored\n"
                            + "order by\n"
                            + "  bd.pkey"
                        ):(
                            "declare get_backup_data_pkeys cursor for select\n"
                            + "  pkey,\n"
                            + "  coalesce(compressed_size, data_size) as data_size\n"
                            + "from\n"
                            + "  backup_data\n"
                            + "where\n"
                            + "  is_stored\n"
                            + "order by\n"
                            + "  pkey"
                        )
                    ):(
                        minBackupLevel>BackupLevel.DO_NOT_BACKUP
                        ?(
                            "declare get_backup_data_pkeys cursor for select distinct\n"
                            + "  bd.pkey,\n"
                            + "  coalesce(bd.compressed_size, bd.data_size) as data_size\n"
                            + "from\n"
                            + "  (\n"
                            + "    select backup_data from file_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from interbase_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from mysql_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "    union select backup_data from postgres_backups where backup_level>="+minBackupLevel+"::smallint group by backup_data\n"
                            + "  ) backups,\n"
                            + "  backup_data bd\n"
                            + "where\n"
                            + "  backups.backup_data=bd.pkey\n"
                            + "order by\n"
                            + "  bd.pkey"
                        ):(
                            "declare get_backup_data_pkeys cursor for select\n"
                            + "  pkey,\n"
                            + "  coalesce(compressed_size, data_size) as data_size\n"
                            + "from\n"
                            + "  backup_data\n"
                            + "order by\n"
                            + "  pkey"
                        )
                    )
                );
                try {
                    out.writeByte(AOServProtocol.DONE);
                    sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from get_backup_data_pkeys";
                    int lastpkey=0;
                    while(true) {
                        int batchSize=0;
                        backupConn.incrementQueryCount();
                        ResultSet results=stmt.executeQuery(sqlString);
                        try {
                            while(results.next()) {
                                int pkey=results.getInt(1);
                                out.writeCompressedInt(pkey-lastpkey);
                                lastpkey=pkey;
                                out.writeLong(results.getLong(2));
                                batchSize++;
                            }
                        } finally {
                            results.close();
                        }
                        if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                    }
                    out.writeCompressedInt(-1);
                } finally {
                    // Remove the cursor
                    backupConn.incrementUpdateCount();
                    stmt.executeUpdate("close get_backup_data_pkeys");
                }
            } catch(SQLException err) {
                System.err.print("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static void getFileBackupSetServer(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        CompressedDataOutputStream out,
        int server,
        String path,
        long time
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getFileBackupSetServer(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,String,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to get the complete backup data set.");
            ServerHandler.checkAccessServer(conn, source, "getFileBackupSetServer", server);

            backupConn.executeUpdate("set enable_seqscan to off");
            try {
                if(path==null) {
                    PreparedStatement pstmt;
                    if(time==-1) {
                        pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                            "declare get_file_backup_set_server cursor for select\n"
                            + "  file_path,\n"
                            + "  pkey,\n"
                            + "  mode\n"
                            + "from\n"
                            + "  file_backups\n"
                            + "where\n"
                            + "  server=?\n"
                            + "  and remove_time is null\n"
                            + "order by\n"
                            + "  file_path,\n"
                            + "  create_time desc"
                       );
                    } else {
                        pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(
                            "declare get_file_backup_set_server cursor for select\n"
                            + "  file_path,\n"
                            + "  pkey,\n"
                            + "  mode\n"
                            + "from\n"
                            + "  file_backups\n"
                            + "where\n"
                            + "  server=?\n"
                            + "  and (\n"
                            + "    remove_time is null\n"
                            + "    or remove_time>=?\n"
                            + "  ) and create_time<=?\n"
                            + "order by\n"
                            + "  file_path,\n"
                            + "  create_time desc"
                        );
                    }
                    try {
                        int pos=1;
                        pstmt.setInt(pos++, server);
                        if(time!=-1) {
                            Timestamp T=new Timestamp(time);
                            pstmt.setTimestamp(pos++, T);
                            pstmt.setTimestamp(pos++, T);
                        }
                        backupConn.incrementUpdateCount();
                        pstmt.executeUpdate();
                    } catch(SQLException err) {
                        System.err.println("Error from update: "+pstmt.toString());
                        throw err;
                    } finally {
                        pstmt.close();
                    }

                    String sqlString=null;
                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).createStatement();
                    try {
                        sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from get_file_backup_set_server";

                        out.writeByte(AOServProtocol.DONE);
                        int lastPath=-1;
                        while(true) {
                            int batchSize=0;
                            backupConn.incrementQueryCount();
                            ResultSet results=stmt.executeQuery(sqlString);
                            try {
                                while(results.next()) {
                                    int thisPath=results.getInt(1);
                                    if(thisPath!=lastPath) {
                                        out.writeInt(results.getInt(2));
                                        long mode=results.getLong(3);
                                        out.writeBoolean(UnixFile.isDirectory(mode));
                                        lastPath=thisPath;
                                    }
                                    batchSize++;
                                }
                            } finally {
                                results.close();
                            }
                            if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                        }
                        out.writeInt(-1);
                    } catch(SQLException err) {
                        System.err.println("Error from update: "+sqlString);
                        throw err;
                    } finally {
                        backupConn.incrementUpdateCount();
                        stmt.executeUpdate("close get_file_backup_set_server");
                        stmt.close();
                    }
                } else {
                    // Resolve the starting path
                    int file_path=FilePathHandler.findFilePath(backupConn, path);
                    FileBackup tempFB=new FileBackup();
                    out.writeByte(AOServProtocol.DONE);
                    if(file_path!=-1) getFileBackupSetServer0(backupConn, out, server, time, new int[] {file_path}, 1, 1, new StringBuilder());
                    out.writeInt(-1);
                }
            } finally {
                backupConn.executeUpdate("set enable_seqscan to on");
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    private static void getFileBackupSetServer0(
        BackupDatabaseConnection backupConn,
        CompressedDataOutputStream out,
        int server,
        long time,
        int[] file_paths,
        int blockSize,
        int recursionLevel,
        StringBuilder tempSB
    ) throws IOException, SQLException {
        if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

        if(blockSize>0) {
            // Build the declare statement
            tempSB.setLength(0);
            tempSB.append(
                "select\n"
                + "  file_path,\n"
                + "  pkey,\n"
                + "  mode\n"
                + "from\n"
                + "  file_backups\n"
                + "where\n"
                + "  (\n");
            for(int c=0;c<blockSize;c++) {
                tempSB.append(c>0?"    or (file_path=":"    (file_path=").append(file_paths[c]).append(" and server=").append(server).append(")\n");
            }
            if(time==-1) {
                tempSB.append("  ) and remove_time is null\n");
            } else {
                tempSB.append(
                    "  ) and create_time<=?\n"
                    + "  and (\n"
                    + "    remove_time is null\n"
                    + "    or remove_time>=?\n"
                    + "  )\n"
                );
            }
            tempSB.append(
                "order by\n"
                + "  file_path,\n"
                + "  create_time desc"
            );
            String sql=tempSB.toString();

            // Get all the file_backups
            int[] foundFilePaths=new int[BLOCK_SIZE];
            int foundFilePathCount=0;
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(sql);
            try {
                if(time!=-1) {
                    Timestamp T=new Timestamp(time);
                    pstmt.setTimestamp(1, T);
                    pstmt.setTimestamp(2, T);
                }
                backupConn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
                    int lastFilePath=-1;
                    while(results.next()) {
                        int thisFilePath=results.getInt(1);
                        if(thisFilePath!=lastFilePath) {
                            out.writeInt(results.getInt(2));
                            long mode=results.getLong(3);
                            out.writeBoolean(UnixFile.isDirectory(mode));
                            lastFilePath=thisFilePath;
                            foundFilePaths[foundFilePathCount++]=thisFilePath;
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
            
            if(foundFilePathCount>0) {
                // Find the children file_paths of these file_backups
                tempSB.setLength(0);
                tempSB.append(
                    "declare get_file_backup_set_server_").append(recursionLevel).append(" cursor for select\n"
                    + "  pkey\n"
                    + "from\n"
                    + "  file_paths\n"
                    + "where\n"
                    + "  parent in (");
                for(int c=0;c<foundFilePathCount;c++) {
                    if(c>0) tempSB.append(',');
                    tempSB.append(foundFilePaths[c]);
                }
                tempSB.append(')');
                
                String sqlString=null;
                Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).createStatement();
                try {
                    // Create the cursor
                    backupConn.incrementUpdateCount();
                    stmt.executeUpdate(sqlString=tempSB.toString());
                    
                    try {
                        tempSB.setLength(0);
                        tempSB.append("fetch "+BLOCK_SIZE+" from get_file_backup_set_server_").append(recursionLevel);
                        sqlString=tempSB.toString();
                        
                        while(true) {
                            foundFilePathCount=0;
                            backupConn.incrementQueryCount();
                            ResultSet results=stmt.executeQuery(sqlString);
                            try {
                                while(results.next()) {
                                    foundFilePaths[foundFilePathCount++]=results.getInt(1);
                                }
                            } finally {
                                results.close();
                            }
                            // Recursively get more stuff
                            getFileBackupSetServer0(backupConn, out, server, time, foundFilePaths, foundFilePathCount, recursionLevel+1, tempSB);
                            if(foundFilePathCount<BLOCK_SIZE) break;
                        }
                    } finally {
                        // Close the cursor
                        tempSB.setLength(0);
                        tempSB.append("close get_file_backup_set_server_").append(recursionLevel);
                        backupConn.incrementUpdateCount();
                        stmt.executeUpdate(sqlString=tempSB.toString());
                    }
                } catch(SQLException err) {
                    System.err.println("Error from query: "+sqlString);
                    throw err;
                } finally {
                    stmt.close();
                }
            }
        }
    }

    /**
     * Gets a backup data set
     */
    public static void getBackupDataBytes(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        CompressedDataOutputStream out,
        int pkey,
        long skipBytes,
        GetBackupDataReporter reporter
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, BackupHandler.class, "getBackupDataBytes(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,long,GetBackupDataReporter)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            checkAccessBackupData(conn, backupConn, source, "getBackupDataBytes", pkey);
            int aoServer;
            String partitionPath;
            boolean isCompressed;
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                "select\n"
                + "  bp.ao_server,\n"
                + "  bp.path,\n"
                + "  bd.compressed_size is not null\n"
                + "from\n"
                + "  backup_data bd,\n"
                + "  backup_partitions bp\n"
                + "where\n"
                + "  bd.pkey=?\n"
                + "  and bd.backup_partition=bp.pkey"
            );
            try{
                pstmt.setInt(1, pkey);
                backupConn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
                    if(results.next()) {
                        aoServer=results.getInt(1);
                        partitionPath=results.getString(2);
                        isCompressed=results.getBoolean(3);
                    } else throw new SQLException("No rows returned.");
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
	    String pathPrefix=BackupData.getPathPrefix(partitionPath, pkey);
	    try {
		DaemonHandler.getDaemonConnector(
						 conn,
						 aoServer
						 ).getBackupData(
								 pathPrefix,
								 out,
								 skipBytes,
								 reporter
								 );
	    } catch(IOException err) {
		System.err.println("Error trying to getBackupData(\""+pathPrefix+"\", out, "+skipBytes+", reporter) on "+ServerHandler.getHostnameForServer(conn, aoServer));
	    }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public static long getBackupPartitionTotalSize(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupPartitionTotalSize(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForBackupPartition(backupConn, pkey);
            ServerHandler.checkAccessServer(conn, source, "getBackupPartitionTotalSize", aoServer);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String device=getDeviceForBackupPartition(backupConn, pkey);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceTotalSize(device);
                } catch(IOException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "device="+device,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                } catch(SQLException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "device="+device,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                }
            } else return -1;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static long getBackupPartitionUsedSize(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupPartitionUsedSize(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForBackupPartition(backupConn, pkey);
            ServerHandler.checkAccessServer(conn, source, "getBackupPartitionUsedSize", aoServer);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String device=getDeviceForBackupPartition(backupConn, pkey);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceUsedSize(device);
                } catch(IOException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "device="+device,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                } catch(SQLException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "device="+device,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                }
            } else return -1;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    static class BackupPartitionCacheEntry {
        int pkey;
        int server;
        String serverFarm;
        String device;
        long size;
        long used;
        long desired_free_space;
        int fill_order;
    }

    /**
     * The amount of time between data refreshes.
     */
    private static final long MAX_BACKUP_PARTITION_CACHE_AGE=30L*60*1000;

    private static final Object backupPartitionsLock=new Object();
    private static List<BackupPartitionCacheEntry> backupPartitions;
    private static long lastBackupPartitionsRefreshed;

    private static final Object getBackupPartitionsLock=new Object();

    /**
     * Finds the backup partition that matches all of the following criteria:
     *
     * 1) Is enabled
     * 2) Is not in a failover state
     * 3) Is not the provided server, unless self backups are allowed
     * 4) Is in the backup farm for the requested server
     * 5) Has dataSize or greater bytes available before reaching desired_free_space
     * 6) Has the lowest fill_order of any of the partitions matching 1) through 5) above
     * 7) Would have the lowest percentage of disk space used with these bytes added
     *
     * The data used for these results is cached and automatically updated every MAX_BACKUP_PARTITION_CACHE_AGE.  The
     * cached data is invalidated whenever the <code>backup_partitions</code> table is invalidated.
     *
     * @see #MAX_BACKUP_PARTITION_CACHE_AGE
     */
    private static int getBackupPartition(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        int server,
        long dataSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupPartition(MasterDatabaseConnection,BackupDatabaseConnection,int,long)", null);
        try {
            synchronized(getBackupPartitionsLock) {
                long localLastBackupPartitionsRefreshed;
                List<BackupPartitionCacheEntry> localBackupPartitions;
                synchronized(backupPartitionsLock) {
                    localLastBackupPartitionsRefreshed=lastBackupPartitionsRefreshed;
                    localBackupPartitions=backupPartitions;
                }

                // Make sure the data is ready and fresh
                long cacheAge=System.currentTimeMillis()-localLastBackupPartitionsRefreshed;
                if(cacheAge<0 || cacheAge>MAX_BACKUP_PARTITION_CACHE_AGE || localBackupPartitions==null) {
                    List<BackupPartitionCacheEntry> V=new ArrayList<BackupPartitionCacheEntry>();
                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                    String sql="select\n"
                            + "  bp.pkey,\n"
                            + "  se.pkey,\n"
                            + "  se.farm,\n"
                            + "  bp.device,\n"
                            + "  bp.desired_free_space,\n"
                            + "  bp.fill_order\n"
                            + "from\n"
                            + "  backup_partitions bp,\n"
                            + "  servers se,\n"
                            + "  ao_servers ao\n"
                            + "where\n"
                            + "  bp.enabled\n"
                            + "  and bp.ao_server=se.pkey\n"
                            + "  and bp.ao_server=ao.server\n"
                            + "  and ao.failover_server is null"
                    ;
                    try {
                        backupConn.incrementQueryCount();
                        ResultSet results=stmt.executeQuery(sql);
                        try {
                            while(results.next()) {
                                BackupPartitionCacheEntry entry=new BackupPartitionCacheEntry();
                                entry.pkey=results.getInt(1);
                                entry.server=results.getInt(2);
                                entry.serverFarm=results.getString(3);
                                entry.device=results.getString(4);
                                entry.desired_free_space=results.getLong(5);
                                entry.fill_order=results.getInt(6);
                                V.add(entry);
                            }
                        } finally {
                            results.close();
                        }
                    } catch(SQLException err) {
                        System.err.println("Error from query: "+sql);
                        throw err;
                    } finally {
                        stmt.close();
                    }
                    for(int c=0;c<V.size();c++) {
                        BackupPartitionCacheEntry entry=V.get(c);
                        try {
                            if(DaemonHandler.isDaemonAvailable(entry.server)) {
                                AOServDaemonConnector daemon=DaemonHandler.getDaemonConnector(conn, entry.server);
                                entry.size=daemon.getDiskDeviceTotalSize(entry.device);
                                entry.used=daemon.getDiskDeviceUsedSize(entry.device);
                            }
                        } catch(IOException err) {
                            DaemonHandler.flagDaemonAsDown(entry.server);
                            System.err.println("IOException accessing "+entry.device+" on "+entry.server);
                            MasterServer.reportError(
                                err,
                                new Object[] {
                                    "server="+server,
                                    "dataSize="+dataSize,
                                    "entry.device="+entry.device,
                                    "entry.server="+entry.server
                                }
                            );
                        } catch(SQLException err) {
                            DaemonHandler.flagDaemonAsDown(entry.server);
                            MasterServer.reportError(
                                err,
                                new Object[] {
                                    "server="+server,
                                    "dataSize="+dataSize,
                                    "entry.device="+entry.device,
                                    "entry.server="+entry.server
                                }
                            );
                        }
                    }
                    synchronized(backupPartitionsLock) {
                        localBackupPartitions=backupPartitions=V;
                        lastBackupPartitionsRefreshed=System.currentTimeMillis();
                    }
                }
                String backupFarm=ServerHandler.getBackupFarmForServer(conn, server);
                boolean allowSameServer=ServerHandler.getAllowBackupSameServerForServer(conn, server);
                BackupPartitionCacheEntry lowestPercentEntry=null;
                int lowestPercent=Integer.MAX_VALUE;
                for(int c=0;c<localBackupPartitions.size();c++) {
                    BackupPartitionCacheEntry entry=localBackupPartitions.get(c);
                    if(
                        (allowSameServer || server!=entry.server)
                        && entry.serverFarm.equals(backupFarm)
                        && (entry.size-(entry.used+dataSize))>entry.desired_free_space
                    ) {
                        int newPercentFree=(int)((entry.used+dataSize)*100/(entry.size-entry.desired_free_space));
                        if(
                            lowestPercentEntry==null
                            || entry.fill_order<lowestPercentEntry.fill_order
                            || (
                                entry.fill_order==lowestPercentEntry.fill_order
                                && newPercentFree<lowestPercent
                            )
                        ) {
                            lowestPercentEntry=entry;
                            lowestPercent=newPercentFree;
                        }
                    }
                }
                if(lowestPercentEntry==null) throw new SQLException("Unable to find backup partition for "+dataSize+(dataSize==1?" byte":" bytes")+" from server "+server);
                // Assume 4096 byte allocations
                long diskSize=dataSize&0x7ffffffffffff000L;
                if(diskSize!=dataSize) diskSize+=4096;
                lowestPercentEntry.used+=diskSize;
                return lowestPercentEntry.pkey;
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getBackupDatasPKeys(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int batchSize,
        int[] pkeys
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupDatasPKeys(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,int[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            // Build the query
            StringBuilder SB=new StringBuilder();
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            if(masterUser!=null) {
                if(masterServers.length==0) {
                    // No filter
                    SB.append(
                        "select\n"
                        + "  *\n"
                        + "from\n"
                        + "  backup_data\n"
                        + "where\n"
                        + "  pkey in (");
                    for(int c=0;c<batchSize;c++) {
                        if(c>0) SB.append(',');
                        SB.append(pkeys[c]);
                    }
                    SB.append(')');
                } else {
                    // Filter by server
                    SB.append(
                        "select\n"
                        + "  bd.*\n"
                        + "from\n"
                        + "  backup_data bd\n"
                        + "where\n"
                        + "  bd.pkey in (");
                    for(int c=0;c<batchSize;c++) {
                        if(c>0) SB.append(',');
                        SB.append(pkeys[c]);
                    }
                    SB.append(
                        ")\n"
                        + "  and (\n"
                        + "    (select fb.pkey from file_backups fb where fb.backup_data=bd.pkey and fb.server in (");
                    for(int c=0;c<masterServers.length;c++) {
                        if(c>0) SB.append(',');
                        SB.append(masterServers[c].getServerPKey());
                    }
                    SB.append(") limit 1) is not null\n"
                        + "    or (select ib.pkey from interbase_backups ib where ib.backup_data=bd.pkey and ib.ao_server in (");
                    for(int c=0;c<masterServers.length;c++) {
                        if(c>0) SB.append(',');
                        SB.append(masterServers[c].getServerPKey());
                    }
                    SB.append(") limit 1) is not null\n"
                        + "    or (select mb.pkey from mysql_backups mb where mb.backup_data=bd.pkey and mb.ao_server in (");
                    for(int c=0;c<masterServers.length;c++) {
                        if(c>0) SB.append(',');
                        SB.append(masterServers[c].getServerPKey());
                    }
                    SB.append(") limit 1) is not null\n"
                        + "    or (select pb.pkey from postgres_backups pb, postgres_servers ps where pb.backup_data=bd.pkey and pb.postgres_server=ps.pkey and ps.ao_server in (");
                    for(int c=0;c<masterServers.length;c++) {
                        if(c>0) SB.append(',');
                        SB.append(masterServers[c].getServerPKey());
                    }
                    SB.append(") limit 1) is not null\n"
                        + "  )");
                }
            } else {
                // Filter by package
                IntList packages=PackageHandler.getIntPackages(conn, source);
                int numPackages=packages.size();
                SB.append(
                    "select\n"
                    + "  bd.*\n"
                    + "from\n"
                    + "  backup_data bd\n"
                    + "where\n"
                    + "  bd.pkey in (");
                for(int c=0;c<batchSize;c++) {
                    if(c>0) SB.append(',');
                    SB.append(pkeys[c]);
                }
                SB.append(
                    ")\n"
                    + "  and (\n"
                    + "    (select fb.pkey from file_backups fb where fb.backup_data=bd.pkey and fb.package in (");
                for(int c=0;c<numPackages;c++) {
                    if(c>0) SB.append(',');
                    SB.append(packages.getInt(c));
                }
                SB.append(") limit 1) is not null\n"
                    + "    or (select ib.pkey from interbase_backups ib where ib.backup_data=bd.pkey and ib.package in (");
                for(int c=0;c<numPackages;c++) {
                    if(c>0) SB.append(',');
                    SB.append(packages.getInt(c));
                }
                SB.append(") limit 1) is not null\n"
                    + "    or (select mb.pkey from mysql_backups mb where mb.backup_data=bd.pkey and mb.package in (");
                for(int c=0;c<numPackages;c++) {
                    if(c>0) SB.append(',');
                    SB.append(packages.getInt(c));
                }
                SB.append(") limit 1) is not null\n"
                    + "    or (select pb.pkey from postgres_backups pb where pb.backup_data=bd.pkey and pb.package in (");
                for(int c=0;c<numPackages;c++) {
                    if(c>0) SB.append(',');
                    SB.append(packages.getInt(c));
                }
                SB.append(") limit 1) is not null\n"
                    + "  )");
            }
            
            // Execute the query while aligning the results
            BackupData[] bds=new BackupData[batchSize];
            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        BackupData bd=new BackupData();
                        bd.init(results);
                        int pkey=bd.getPKey();
                        for(int c=0;c<batchSize;c++) {
                            if(pkeys[c]==pkey) bds[c]=bd;
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
            
            // Write the results
            String version=source.getProtocolVersion();
            for(int c=0;c<batchSize;c++) {
                BackupData bd=bds[c];
                if(bd!=null) {
                    out.writeByte(AOServProtocol.NEXT);
                    bd.write(out, version);
                } else out.writeByte(AOServProtocol.DONE);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForFileBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBusinessForFileBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeStringQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                true,
                "select\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  file_backups fb,\n"
                + "  packages pk\n"
                + "where\n"
                + "  fb.pkey=?\n"
                + "  and fb.package=pk.pkey",
                pkey
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static List<String> getBusinessesForFileBackups(BackupDatabaseConnection backupConn, int[] pkeys, int batchSize) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBusinessesForFileBackups(BackupDatabaseConnection,int[],int)", null);
        try {
            StringBuilder sql=new StringBuilder();
            sql.append(
                "select distinct\n"
                + "  pk.accounting\n"
                + "from\n"
                + "  file_backups fb,\n"
                + "  packages pk\n"
                + "where\n"
                + "  fb.pkey in (");
            for(int c=0;c<batchSize;c++) {
                if(c>0) sql.append(',');
                sql.append(pkeys[c]);
            }
            sql.append(
                ")\n"
                + "  and fb.package=pk.pkey"
            );
            return backupConn.executeStringListQuery(
                Connection.TRANSACTION_READ_COMMITTED,
                true,
                sql.toString()
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all file backups for one server.
     */
    public static void getFileBackupsServer(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int server
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getFileBackupsServer(MasterDatabaseConnection,BackupDatabaseConnection.RequestSource,CompressedDataOutputStream,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            backupConn.executeUpdate("set enable_seqscan to off");
            try {
                if(masterUser!=null) {
                    if(masterServers.length==0) MasterServer.fetchObjects(
                        backupConn,
                        source,
                        out,
                        new FileBackup(),
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  file_backups fb,\n"
                        + "  file_paths fp"
                        + "where\n"
                        + "  fb.server=?\n"
                        + "  and fb.file_path=fp.pkey",
                        server
                    ); else MasterServer.fetchObjects(
                        backupConn,
                        source,
                        out,
                        new FileBackup(),
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  master_servers ms,\n"
                        + "  file_backups fb,\n"
                        + "  file_paths fp\n"
                        + "where\n"
                        + "  ms.username=?\n"
                        + "  and ms.server=?\n"
                        + "  and ms.server=fb.server\n"
                        + "  and fb.file_path=fp.pkey",
                        username,
                        server
                    );
                } else MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  usernames un,\n"
                    + "  packages pk1,\n"
                    + TableHandler.BU1_PARENTS_JOIN
                    + "  packages pk2,\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  un.username=?\n"
                    + "  and un.package=pk1.name\n"
                    + "  and (\n"
                    + TableHandler.PK1_BU1_PARENTS_WHERE
                    + "  )\n"
                    + "  and bu1.accounting=pk2.accounting\n"
                    + "  and pk2.pkey=fb.package\n"
                    + "  and fb.server=?\n"
                    + "  and fb.file_path=fp.pkey",
                    username,
                    server
                );
            } finally {
                backupConn.executeUpdate("set enable_seqscan to on");
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getFileBackupsPKeys(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int batchSize,
        int[] pkeys
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getFileBackupsPKeys(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,int[])", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            if(batchSize>0) {
                // Check access for the files
                checkAccessFileBackups(conn, backupConn, source, "getFileBackupsPKeys", pkeys, batchSize);

                // Get all of the files in one query
                StringBuilder SB=new StringBuilder();
                SB.append(
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  fb.pkey in (");
                for(int c=0;c<batchSize;c++) {
                    if(c>0) SB.append(',');
                    SB.append(pkeys[c]);
                }
                SB.append(")\n"
                        + "  and fb.file_path=fp.pkey");
                String sqlString=null;
                Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                try {
                    FileBackup[] fbs=new FileBackup[batchSize];
                    backupConn.incrementQueryCount();
                    ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                    try {
                        while(results.next()) {
                            FileBackup fb=new FileBackup();
                            fb.init(results);
                            int pkey=fb.getPKey();
                            for(int c=0;c<batchSize;c++) {
                                if(pkeys[c]==pkey) fbs[c]=fb;
                            }
                        }
                    } finally {
                        results.close();
                    }

                    // Write the results
                    String version=source.getProtocolVersion();
                    for(int c=0;c<batchSize;c++) {
                        FileBackup fb=fbs[c];
                        if(fb!=null) {
                            out.writeByte(AOServProtocol.NEXT);
                            fb.write(out, version);
                        } else out.writeByte(AOServProtocol.DONE);
                    }
                } catch(SQLException err) {
                    System.err.println("Error from query: "+sqlString);
                    throw err;
                } finally {
                    stmt.close();
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all file backups for one server and parent path
     */
    public static void getFileBackupChildren(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int server,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getFileBackupChildren(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,String)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);

            if(path.length()==0) {
                // For null path, show all of the root paths
                int[] roots=FilePathHandler.getRootNodes(backupConn);
                StringBuilder sql=new StringBuilder();
                if(masterUser!=null) {
                    if(masterServers.length==0) {
                        sql.append(
                            "select\n"
                            + "  fb.pkey,\n"
                            + "  fb.server,\n"
                            + "  fp.path,\n"
                            + "  fb.device,\n"
                            + "  fb.inode,\n"
                            + "  fb.package,\n"
                            + "  fb.mode,\n"
                            + "  fb.uid,\n"
                            + "  fb.gid,\n"
                            + "  fb.backup_data,\n"
                            + "  fb.create_time,\n"
                            + "  fb.modify_time,\n"
                            + "  fb.remove_time,\n"
                            + "  fb.backup_level,\n"
                            + "  fb.backup_retention,\n"
                            + "  fb.symlink_target,\n"
                            + "  fb.device_id\n"
                            + "from\n"
                            + "  file_backups fb,\n"
                            + "  file_paths fp\n"
                            + "where\n"
                            + "  fb.file_path in (");
                        for(int c=0;c<roots.length;c++) {
                            if(c>0) sql.append(',');
                            sql.append(roots[c]);
                        }
                        sql.append(")\n"
                            + "  and fb.server=").append(server).append("\n"
                            + "  and fb.file_path=fp.pkey");
                    } else {
                        sql.append(
                            "select\n"
                            + "  fb.pkey,\n"
                            + "  fb.server,\n"
                            + "  fp.path,\n"
                            + "  fb.device,\n"
                            + "  fb.inode,\n"
                            + "  fb.package,\n"
                            + "  fb.mode,\n"
                            + "  fb.uid,\n"
                            + "  fb.gid,\n"
                            + "  fb.backup_data,\n"
                            + "  fb.create_time,\n"
                            + "  fb.modify_time,\n"
                            + "  fb.remove_time,\n"
                            + "  fb.backup_level,\n"
                            + "  fb.backup_retention,\n"
                            + "  fb.symlink_target,\n"
                            + "  fb.device_id\n"
                            + "from\n"
                            + "  file_backups fb,\n"
                            + "  master_servers ms,\n"
                            + "  file_paths fp\n"
                            + "where\n"
                            + "  and fb.file_path in (");
                        for(int c=0;c<roots.length;c++) {
                            if(c>0) sql.append(',');
                            sql.append(roots[c]);
                        }
                        sql.append(")\n"
                            + "  and fb.server=ms.server\n"
                            + "  and ms.server=").append(server).append("\n"
                            + "  and ms.username='").append(SQLUtility.escapeSQL(username)).append("'\n"
                            + "  and fb.file_path=fp.pkey");
                    }
                } else {
                    sql.append(
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + TableHandler.BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  file_backups fb,\n"
                        + "  file_paths fp\n"
                        + "where\n"
                        + "  un.username='").append(SQLUtility.escapeSQL(username)).append("'\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + TableHandler.PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.pkey=fb.package\n"
                        + "  and fb.server=").append(server).append("\n"
                        + "  and fb.file_path in (");
                    for(int c=0;c<roots.length;c++) {
                        if(c>0) sql.append(',');
                        sql.append(roots[c]);
                    }
                    sql.append(")\n"
                            + "  and fb.file_path=fp.pkey");
                }

                MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    sql.toString()
                );
            } else {
                int file_path=FilePathHandler.findFilePath(backupConn, path);
                if(file_path!=-1) {
                    if(masterUser!=null) {
                        if(masterServers.length==0) MasterServer.fetchObjects(
                            backupConn,
                            source,
                            out,
                            new FileBackup(),
                            "select\n"
                            + "  fb.pkey,\n"
                            + "  fb.server,\n"
                            + "  fp.path,\n"
                            + "  fb.device,\n"
                            + "  fb.inode,\n"
                            + "  fb.package,\n"
                            + "  fb.mode,\n"
                            + "  fb.uid,\n"
                            + "  fb.gid,\n"
                            + "  fb.backup_data,\n"
                            + "  fb.create_time,\n"
                            + "  fb.modify_time,\n"
                            + "  fb.remove_time,\n"
                            + "  fb.backup_level,\n"
                            + "  fb.backup_retention,\n"
                            + "  fb.symlink_target,\n"
                            + "  fb.device_id\n"
                            + "from\n"
                            + "  file_paths fp,\n"
                            + "  file_backups fb\n"
                            + "where\n"
                            + "  fp.parent=?\n"
                            + "  and fp.pkey!=fp.parent\n"
                            + "  and fp.pkey=fb.file_path\n"
                            + "  and fb.server=?",
                            file_path,
                            server
                        ); else MasterServer.fetchObjects(
                            backupConn,
                            source,
                            out,
                            new FileBackup(),
                            "select\n"
                            + "  fb.pkey,\n"
                            + "  fb.server,\n"
                            + "  fp.path,\n"
                            + "  fb.device,\n"
                            + "  fb.inode,\n"
                            + "  fb.package,\n"
                            + "  fb.mode,\n"
                            + "  fb.uid,\n"
                            + "  fb.gid,\n"
                            + "  fb.backup_data,\n"
                            + "  fb.create_time,\n"
                            + "  fb.modify_time,\n"
                            + "  fb.remove_time,\n"
                            + "  fb.backup_level,\n"
                            + "  fb.backup_retention,\n"
                            + "  fb.symlink_target,\n"
                            + "  fb.device_id\n"
                            + "from\n"
                            + "  file_paths fp,\n"
                            + "  file_backups fb,\n"
                            + "  master_servers ms\n"
                            + "where\n"
                            + "  fp.parent=?\n"
                            + "  and fp.pkey!=fp.parent\n"
                            + "  and fp.pkey=fb.file_path\n"
                            + "  and fb.server=ms.server\n"
                            + "  and ms.server=?\n"
                            + "  and ms.username=?",
                            file_path,
                            server,
                            username
                        );
                    } else MasterServer.fetchObjects(
                        backupConn,
                        source,
                        out,
                        new FileBackup(),
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  usernames un,\n"
                        + "  packages pk1,\n"
                        + TableHandler.BU1_PARENTS_JOIN
                        + "  packages pk2,\n"
                        + "  file_backups fb,\n"
                        + "  file_paths fp\n"
                        + "where\n"
                        + "  un.username=?\n"
                        + "  and un.package=pk1.name\n"
                        + "  and (\n"
                        + TableHandler.PK1_BU1_PARENTS_WHERE
                        + "  )\n"
                        + "  and bu1.accounting=pk2.accounting\n"
                        + "  and pk2.pkey=fb.package\n"
                        + "  and fb.server=?\n"
                        + "  and fb.file_path=fp.pkey\n"
                        + "  and fp.pkey!=fp.parent\n"
                        + "  and fp.parent=?",
                        username,
                        server,
                        file_path
                    );
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Seaches for files by md5
     */
    public static void findFileBackupsByMD5(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        long md5_hi,
        long md5_lo,
        int server
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "findFindBackupsByMD5(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,long,long,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            
            if(masterUser!=null) {
                if(masterServers.length==0) MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  backup_data bd,\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  bd.md5_hi="+md5_hi+"::int8\n"
                    + "  and bd.md5_lo="+md5_lo+"::int8\n"
                    + "  and bd.pkey=fb.backup_data\n"
                    + (server==-1?"":("  and fb.server="+server+"\n"))
                    + "  and fb.file_path=fp.pkey"
                ); else MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  master_servers ms,\n"
                    + "  file_backups fb,\n"
                    + "  backup_data bd,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  ms.username=?\n"
                    + (server==-1?"":("  and ms.server="+server+"\n"))
                    + "  and ms.server=fb.server\n"
                    + "  and fb.backup_data=bd.pkey\n"
                    + "  and bd.md5_hi="+md5_hi+"::int8\n"
                    + "  and bd.md5_lo="+md5_lo+"::int8\n"
                    + "  and fb.file_path=fp.pkey",
                    username
                );
            } else MasterServer.fetchObjects(
                backupConn,
                source,
                out,
                new FileBackup(),
                "select\n"
                + "  fb.pkey,\n"
                + "  fb.server,\n"
                + "  fp.path,\n"
                + "  fb.device,\n"
                + "  fb.inode,\n"
                + "  fb.package,\n"
                + "  fb.mode,\n"
                + "  fb.uid,\n"
                + "  fb.gid,\n"
                + "  fb.backup_data,\n"
                + "  fb.create_time,\n"
                + "  fb.modify_time,\n"
                + "  fb.remove_time,\n"
                + "  fb.backup_level,\n"
                + "  fb.backup_retention,\n"
                + "  fb.symlink_target,\n"
                + "  fb.device_id\n"
                + "from\n"
                + "  usernames un,\n"
                + "  packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  packages pk2,\n"
                + "  file_backups fb,\n"
                + "  backup_data bd,\n"
                + "  file_paths fp\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and un.package=pk1.name\n"
                + "  and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=pk2.accounting\n"
                + "  and pk2.pkey=fb.package\n"
                + (server==-1?"":("  and fb.server="+server+"\n"))
                + "  and fb.backup_data=bd.pkey\n"
                + "  and bd.md5_hi="+md5_hi+"::int8\n"
                + "  and bd.md5_lo="+md5_lo+"::int8\n"
                + "  and fb.file_path=fp.pkey",
                username
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Seaches for hard links.
     */
    public static void findHardLinks(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int server,
        short device,
        long inode
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "findHardLinks(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,short,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            
            if(masterUser!=null) {
                if(masterServers.length==0) MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  fb.server=?\n"
                    + "  and fb.device=?::smallint\n"
                    + "  and fb.inode=?::int8\n"
                    + "  and fb.file_path=fp.pkey",
                    server,
                    device,
                    inode
                ); else MasterServer.fetchObjects(
                    backupConn,
                    source,
                    out,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  master_servers ms,\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  ms.username=?\n"
                    + "  and ms.server=?\n"
                    + "  and ms.server=fb.server\n"
                    + "  and fb.device=?::smallint\n"
                    + "  and fb.inode=?::int8\n"
                    + "  and fb.file_path=fp.pkey",
                    username,
                    server,
                    device,
                    inode
                );
            } else MasterServer.fetchObjects(
                backupConn,
                source,
                out,
                new FileBackup(),
                "select\n"
                + "  fb.pkey,\n"
                + "  fb.server,\n"
                + "  fp.path,\n"
                + "  fb.device,\n"
                + "  fb.inode,\n"
                + "  fb.package,\n"
                + "  fb.mode,\n"
                + "  fb.uid,\n"
                + "  fb.gid,\n"
                + "  fb.backup_data,\n"
                + "  fb.create_time,\n"
                + "  fb.modify_time,\n"
                + "  fb.remove_time,\n"
                + "  fb.backup_level,\n"
                + "  fb.backup_retention,\n"
                + "  fb.symlink_target,\n"
                + "  fb.device_id\n"
                + "from\n"
                + "  usernames un,\n"
                + "  packages pk1,\n"
                + TableHandler.BU1_PARENTS_JOIN
                + "  packages pk2,\n"
                + "  file_backups fb,\n"
                + "  file_paths fp\n"
                + "where\n"
                + "  un.username=?\n"
                + "  and un.package=pk1.name\n"
                + "  and (\n"
                + TableHandler.PK1_BU1_PARENTS_WHERE
                + "  )\n"
                + "  and bu1.accounting=pk2.accounting\n"
                + "  and pk2.pkey=fb.package\n"
                + "  and fb.server=?\n"
                + "  and fb.device=?::smallint\n"
                + "  and fb.inode=?::int8\n"
                + "  and fb.file_path=fp.pkey",
                username,
                server,
                device,
                inode
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all file backups for one server and path
     */
    public static void getFileBackupVersions(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        CompressedDataOutputStream out,
        int server,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getFileBackupVersions(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int,String)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            com.aoindustries.aoserv.client.MasterServer[] masterServers=masterUser==null?null:MasterServer.getMasterServers(conn, username);
            
            int file_path=FilePathHandler.findFilePath(backupConn, path);
            if(file_path!=-1) {
                if(masterUser!=null) {
                    if(masterServers.length==0) MasterServer.writeObjects(
                        backupConn,
                        source,
                        out,
                        false,
                        new FileBackup(),
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  file_backups fb,\n"
                        + "  file_paths fp\n"
                        + "where\n"
                        + "  fb.file_path=?\n"
                        + "  and fb.server=?\n"
                        + "  and fb.file_path=fp.pkey",
                        file_path,
                        server
                    ); else MasterServer.writeObjects(
                        backupConn,
                        source,
                        out,
                        false,
                        new FileBackup(),
                        "select\n"
                        + "  fb.pkey,\n"
                        + "  fb.server,\n"
                        + "  fp.path,\n"
                        + "  fb.device,\n"
                        + "  fb.inode,\n"
                        + "  fb.package,\n"
                        + "  fb.mode,\n"
                        + "  fb.uid,\n"
                        + "  fb.gid,\n"
                        + "  fb.backup_data,\n"
                        + "  fb.create_time,\n"
                        + "  fb.modify_time,\n"
                        + "  fb.remove_time,\n"
                        + "  fb.backup_level,\n"
                        + "  fb.backup_retention,\n"
                        + "  fb.symlink_target,\n"
                        + "  fb.device_id\n"
                        + "from\n"
                        + "  file_backups fb,\n"
                        + "  master_servers ms,\n"
                        + "  file_paths fp\n"
                        + "where\n"
                        + "  fb.file_path=?\n"
                        + "  and fb.server=ms.server\n"
                        + "  and ms.server=?\n"
                        + "  and ms.username=?\n"
                        + "  and fb.file_path=fp.pkey",
                        file_path,
                        server,
                        username
                    );
                } else MasterServer.writeObjects(
                    backupConn,
                    source,
                    out,
                    false,
                    new FileBackup(),
                    "select\n"
                    + "  fb.pkey,\n"
                    + "  fb.server,\n"
                    + "  fp.path,\n"
                    + "  fb.device,\n"
                    + "  fb.inode,\n"
                    + "  fb.package,\n"
                    + "  fb.mode,\n"
                    + "  fb.uid,\n"
                    + "  fb.gid,\n"
                    + "  fb.backup_data,\n"
                    + "  fb.create_time,\n"
                    + "  fb.modify_time,\n"
                    + "  fb.remove_time,\n"
                    + "  fb.backup_level,\n"
                    + "  fb.backup_retention,\n"
                    + "  fb.symlink_target,\n"
                    + "  fb.device_id\n"
                    + "from\n"
                    + "  usernames un,\n"
                    + "  packages pk1,\n"
                    + TableHandler.BU1_PARENTS_JOIN
                    + "  packages pk2,\n"
                    + "  file_backups fb,\n"
                    + "  file_paths fp\n"
                    + "where\n"
                    + "  un.username=?\n"
                    + "  and un.package=pk1.name\n"
                    + "  and (\n"
                    + TableHandler.PK1_BU1_PARENTS_WHERE
                    + "  )\n"
                    + "  and bu1.accounting=pk2.accounting\n"
                    + "  and pk2.pkey=fb.package\n"
                    + "  and fb.server=?\n"
                    + "  and fb.file_path=?\n"
                    + "  and fb.file_path=fp.pkey",
                    username,
                    server,
                    file_path
                );
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getFilenameForBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "getFilenameForBackupData(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            checkAccessBackupData(conn, backupConn, source, "getFilenameForBackupData", pkey);

            String path=SQLUtility.decodeString(
                backupConn.executeStringQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  coalesce(\n"
                    + "    (select db_name || '.sql' from postgres_backups where backup_data=? limit 1),\n"
                    + "    (select db_name || '.sql' from mysql_backups where backup_data=? limit 1),\n"
                    + "    (select db_name from interbase_backups where backup_data=? limit 1),\n"
                    + "    (select fp.filename from file_backups fb, file_paths fp where fb.backup_data=? and fb.file_path=fp.pkey limit 1)\n"
                    + "  )",
                    pkey,
                    pkey,
                    pkey,
                    pkey
                )
            );
            if(path!=null) {
                int lpos=path.lastIndexOf('/');
                if(lpos!=-1) path=path.substring(lpos+1);
            }
            return path;
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static void getLatestFileBackupSet(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        CompressedDataOutputStream out,
        int server
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "getLatestFileBackupSet(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,CompressedDataOutputStream,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to get the complete backup set for a server.");
            ServerHandler.checkAccessServer(conn, source, "getLatestFileBackupSet", server);

            backupConn.executeUpdate("set enable_seqscan to off");
            try {
                backupConn.executeUpdate(
                    "declare get_latest_file_backup_set cursor for select\n"
                    + "  pkey\n"
                    + "from\n"
                    + "  file_backups\n"
                    + "where\n"
                    + "  server="+server+"\n"
                    + "  and remove_time is null\n"
                    + "order by\n"
                    + "  pkey"
                );
                try {
                    String sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from get_latest_file_backup_set";
                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                    try {
                        out.writeByte(AOServProtocol.DONE);
                        while(true) {
                            int batchSize=0;
                            backupConn.incrementQueryCount();
                            ResultSet results=stmt.executeQuery(sqlString);
                            try {
                                while(results.next()) {
                                    out.writeCompressedInt(results.getInt(1));
                                    batchSize++;
                                }
                            } finally {
                                results.close();
                            }
                            if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                        }
                        out.writeCompressedInt(-1);
                    } catch(SQLException err) {
                        System.err.print("Error from query: "+sqlString);
                        throw err;
                    } finally {
                        stmt.close();
                    }
                } finally {
                    backupConn.executeUpdate("close get_latest_file_backup_set");
                }
            } finally {
                backupConn.executeUpdate("set enable_seqscan to on");
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static String getDeviceForBackupPartition(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getDeviceForBackupPartition(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select device from backup_partitions where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getServerForFileBackup(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getServerForFileBackup(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select server from file_backups where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBackupFarmForServer(MasterDatabaseConnection conn, int server) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupFarmForServer(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select sf.backup_farm from servers se, server_farms sf where se.pkey=? and se.farm=sf.name", server);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static IntList getServersForFileBackups(BackupDatabaseConnection backupConn, int[] pkeys, int batchSize) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getServersForFileBackups(BackupDatabaseConnection,int[],int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            StringBuilder sql=new StringBuilder();
            sql.append("select distinct server from file_backups where pkey in (");
            for(int c=0;c<batchSize;c++) {
                if(c>0) sql.append(',');
                sql.append(pkeys[c]);
            }
            sql.append(')');
            return backupConn.executeIntListQuery(Connection.TRANSACTION_READ_COMMITTED, true, sql.toString());
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, BackupHandler.class, "invalidateTable(int)", null);
        try {
            if(tableID==SchemaTable.BACKUP_PARTITIONS) {
                synchronized(backupPartitionsLock) {
                    backupPartitions=null;
                }
                synchronized(backupPartitionServers) {
                    backupPartitionServers.clear();
                }
            } else if(tableID==SchemaTable.SERVERS) {
                synchronized(backupPartitionsLock) {
                    backupPartitions=null;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static boolean isBackupDataFilled(BackupDatabaseConnection backupConn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "isBackupDataFilled(BackupDatabaseConnection,int)", null);
        try {
            return backupConn.executeBooleanQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select is_stored from backup_data where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeExpiredFileBackups(
        MasterDatabaseConnection fetchConn,
        BackupDatabaseConnection fetchBackupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int server
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "removeExpiredFileBackups(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(fetchConn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not a master user and may not remove expired file backups");
            ServerHandler.checkAccessServer(fetchConn, source, "removeExpiredFileBackups", server);
            String serverHostname=ServerHandler.getHostnameForServer(fetchConn, server);

            // Get the list of pkeys that should be removed
            fetchBackupConn.executeUpdate("set enable_seqscan to off");
            try {
                fetchBackupConn.executeUpdate(
                    "declare expired_file_backups cursor for select\n"
                    + "  fb.pkey,\n"
                    + "  fb.file_path,\n"
                    + "  fb.package,\n"
                    + "  pk.accounting\n"
                    + "from\n"
                    + "  file_backups fb,\n"
                    + "  packages pk\n"
                    + "where\n"
                    + "  fb.server="+server+"\n"
                    + "  and fb.remove_time is not null\n"
                    + "  and now()>=(fb.remove_time+fb.backup_retention)\n"
                    + "  and fb.package=pk.pkey"
                );
                try {
                    int[] batchPKeys=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                    int[] batchFilePaths=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                    int[] batchPackages=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                    List<String> batchAccountings=new SortedArrayList<String>();
                    StringBuilder sql=new StringBuilder();

                    String sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from expired_file_backups";
                    while(true) {
                        int batchSize=0;
                        Statement stmt=fetchBackupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                        try {
                            ResultSet results=stmt.executeQuery(sqlString);
                            try {
                                batchAccountings.clear();
                                while(results.next()) {
                                    batchPKeys[batchSize]=results.getInt(1);
                                    batchFilePaths[batchSize]=results.getInt(2);
                                    batchPackages[batchSize]=results.getInt(3);
                                    String accounting=results.getString(4);
                                    if(!batchAccountings.contains(accounting)) batchAccountings.add(accounting);
                                    batchSize++;
                                }
                            } finally {
                                results.close();
                            }
                        } catch(SQLException err) {
                            System.err.println("Error from update: "+sqlString);
                            throw err;
                        } finally {
                            stmt.close();
                        }

                        if(batchSize>0) {
                            // Remove the backup database entries
                            sql.setLength(0);
                            sql.append("delete from file_backups where pkey in (");
                            for(int d=0;d<batchSize;d++) {
                                if(d>0) sql.append(',');
                                sql.append(batchPKeys[d]);
                            }
                            sql.append(')');
                            BackupDatabaseConnection updateBackupConn=(BackupDatabaseConnection)BackupDatabase.getDatabase().createDatabaseConnection();
                            try {
                                updateBackupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false, 2);
                                boolean rolledBack=false;
                                try {
                                    updateBackupConn.executeUpdate(sql.toString());

                                    // Notify all clients of the update
                                    invalidateList.addTable(
                                        fetchConn,
                                        SchemaTable.FILE_BACKUPS,
                                        batchAccountings,
                                        InvalidateList.getCollection(serverHostname),
                                        false
                                    );

                                    // Remove any file_backup_roots
                                    removedFileBackups(fetchConn, updateBackupConn, invalidateList, batchFilePaths, server, batchPackages, batchSize);

                                    // Remove the unused file_paths
                                    FilePathHandler.removeUnusedFilePaths(updateBackupConn, batchFilePaths, batchSize);
                                } catch (SQLException err) {
                                    if(updateBackupConn.rollbackAndClose()) {
                                        rolledBack=true;
                                    }
                                    throw err;
                                } catch(IOException err) {
                                    if(updateBackupConn.rollbackAndClose()) {
                                        rolledBack=true;
                                        invalidateList=null;
                                    }
                                    throw err;
                                } finally {
                                    if (!rolledBack && !updateBackupConn.isClosed()) updateBackupConn.commit();
                                }
                            } finally {
                                updateBackupConn.releaseConnection();
                            }
                        }

                        // Go to the next block or end loop
                        if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                    }
                } finally {
                    fetchBackupConn.executeUpdate("close expired_file_backups");
                }
            } finally {
                fetchBackupConn.executeUpdate("set enable_seqscan to on");
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static void removeFileBackupSetting(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "removeFileBackupSetting(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int packageNum=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select package from file_backup_settings where pkey=?", pkey);
            PackageHandler.checkAccessPackage(conn, source, "removeFileBackupSetting", packageNum);

            removeFileBackupSetting(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeFileBackupSetting(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "removeFileBackupSetting(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            int packageNum=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select package from file_backup_settings where pkey=?", pkey);
            int server=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select server from file_backup_settings where pkey=?", pkey);

            conn.executeUpdate("delete from file_backup_settings where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_SETTINGS,
                InvalidateList.getCollection(PackageHandler.getBusinessForPackage(conn, packageNum)),
                InvalidateList.getServerCollection(conn, server),
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a FileBackup from the system.
     */
    public static void removeFileBackup(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "removeFileBackup(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            checkAccessFileBackup(conn, backupConn, source, "removeFileBackup", pkey);

            int packageNum;
            int server;
            int file_path;
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                "select\n"
                + "  package,\n"
                + "  server,\n"
                + "  file_path\n"
                + "from\n"
                + "  file_backups\n"
                + "where\n"
                + "  pkey=?"
            );
            try {
                pstmt.setInt(1, pkey);
                backupConn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
                    if(results.next()) {
                        packageNum=results.getInt(1);
                        server=results.getInt(2);
                        file_path=results.getInt(3);
                    } else throw new SQLException("No row returned.");
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+err.toString());
                throw err;
            } finally {
                pstmt.close();
            }

            // Remove the backup database entry
            backupConn.executeUpdate("delete from file_backups where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUPS,
                InvalidateList.getCollection(PackageHandler.getBusinessForPackage(conn, packageNum)),
                InvalidateList.getServerCollection(conn, server),
                false
            );

            // Add and remove file_backup_roots
            removedFileBackups(conn, backupConn, invalidateList, new int[] {file_path}, server, new int[] {packageNum}, 1);

            // Remove the unused file_path
            FilePathHandler.removeUnusedFilePath(backupConn, file_path);
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    public static void removeUnusedBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.SLOW, BackupHandler.class, "removeUnusedBackupData(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            long dayAgo=System.currentTimeMillis()-(24L*60*60*1000);
            int updated=backupConn.executeUpdate(
                "delete\n"
                + "from\n"
                + "  backup_data\n"
                + "where\n"
                + "  created<?\n"
                + "  and (select fb.pkey from file_backups fb where backup_data.pkey=fb.backup_data limit 1) is null\n"
                + "  and (select ib.pkey from interbase_backups ib where backup_data.pkey=ib.backup_data limit 1) is null\n"
                + "  and (select mb.pkey from mysql_backups mb where backup_data.pkey=mb.backup_data limit 1) is null\n"
                + "  and (select pb.pkey from postgres_backups pb where backup_data.pkey=pb.backup_data limit 1) is null",
                new Timestamp(dayAgo)
            );
            if(updated>0) {
                // Notify all clients of the update
                invalidateList.addTable(
                    conn,
                    SchemaTable.BACKUP_DATA,
                    InvalidateList.allBusinesses,
                    InvalidateList.allServers,
                    false
                );
            }
        } finally {
            Profiler.endProfile(Profiler.SLOW);
        }
    }

    /**
     * Each backup partition is locked on any update operation on the destination server, so locking the same here
     * avoids allocating connections to the daemon that will just end up getting locked on the other end.  The connection
     * pool had filled up in the past, causing other important things, like password checking for WebMail, lock up, too.
     */
    private static final Map<Integer,Object> backupPartitionLocks=new HashMap<Integer,Object>();
    public static Object getBackupPartitionLock(int pkey) {
        Profiler.startProfile(Profiler.FAST, BackupHandler.class, "getBackupPartitionLock(int)", null);
        try {
            Integer I=Integer.valueOf(pkey);
            synchronized(backupPartitionLocks) {
                Object O=backupPartitionLocks.get(I);
                if(O==null) backupPartitionLocks.put(I, O=new Object());
                return O;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * The method must fully read the stream, even if an error occurs
     */
    public static void sendBackupData(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        CompressedDataInputStream in,
        int backupData,
        String filename,
        boolean isCompressed,
        long md5_hi,
        long md5_lo
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, BackupHandler.class, "sendBackupData(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,CompressedDataInputStream,int,String,boolean,long,long)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            boolean streamDone=false;
            byte[] buff=BufferManager.getBytes();
            try {
                String mustring = source.getUsername();
                MasterUser mu = MasterServer.getMasterUser(conn, mustring);
                if (mu==null) throw new SQLException("User "+mustring+" is not a master user and may not upload backup data");
                boolean alreadyStored=isBackupDataFilled(backupConn, backupData);
                if(alreadyStored) System.err.println("Warning, backup_data already stored, reading but ignoring request: "+backupData);

                com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, mustring);
                if(masterServers.length!=0) {
                    // Client should provide the appropriate MD5, as proof that is has access to the backup_data
                    if(
                        md5_hi!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_hi from backup_data where pkey=?", backupData)
                        || md5_lo!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_lo from backup_data where pkey=?", backupData)
                    ) throw new SQLException("sendBackupData attempt with mismatched MD5: backup_data.pkey="+backupData+", attempted md5_hi="+md5_hi+" md5_lo="+md5_lo);
                }

                BackupOutputStream backupOut=alreadyStored?null:new BackupOutputStream(
                    conn,
                    backupConn,
                    invalidateList,
                    backupData,
                    md5_hi,
                    md5_lo,
                    filename,
                    isCompressed
                );

                boolean commit=false;
                try {
                    int code;
                    while((code=in.read())==AOServProtocol.NEXT) {
                        int len=in.readShort();
                        in.readFully(buff, 0, len);
                        if(!alreadyStored) backupOut.write(buff, 0, len);
                    }
                    commit=in.readBoolean();
                    streamDone=true;
                } finally {
                    if(!alreadyStored) {
                        if(commit) {
                            backupOut.flush();
                            backupOut.close();
                        } else backupOut.rollback();
                    }
                }
            } catch(IOException err) {
                if(!streamDone) {
                    try {
                        int code;
                        while((code=in.read())==AOServProtocol.NEXT) {
                            int len=in.readShort();
                            in.readFully(buff, 0, len);
                        }
                        in.readBoolean();
                        streamDone=true;
                    } catch(SocketException err2) {
                        // Connection reset common for abnormal client disconnects
                        String message=err2.getMessage();
                        if(
                            !"Broken pipe".equalsIgnoreCase(message)
                            && !"Connection reset".equalsIgnoreCase(message)
                        ) MasterServer.reportError(err2, null);
                    } catch(IOException err2) {
                        MasterServer.reportError(err2, null);
                    }
                }
                throw err;
            } catch(SQLException err) {
                if(!streamDone) {
                    try {
                        int code;
                        while((code=in.read())==AOServProtocol.NEXT) {
                            int len=in.readShort();
                            in.readFully(buff, 0, len);
                        }
                        in.readBoolean();
                        streamDone=true;
                    } catch(IOException err2) {
                        MasterServer.reportError(err2, null);
                    }
                }
                throw err;
            } finally {
                BufferManager.release(buff);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public static void setFileBackupSettings(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String path,
        int packageNum,
        short backupLevel,
        short backupRetention,
        boolean recurse
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupSetting(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,InvalidateList,int,String,int,short,short,boolean)", null);
        try {
            int server=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select server from file_backup_settings where pkey=?", pkey);

            // Must be able to configure backups on this server
            if(
                !conn.executeBooleanQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    true,
                    "select\n"
                    + "  bs.can_configure_backup\n"
                    + "from\n"
                    + "  usernames un,\n"
                    + "  packages pk,\n"
                    + "  business_servers bs\n"
                    + "where\n"
                    + "  un.username=?\n"
                    + "  and un.package=pk.name\n"
                    + "  and pk.accounting=bs.accounting\n"
                    + "  and bs.server=?",
                    source.getUsername(),
                    server
                )
            ) throw new SQLException("Not allowed allowed to configure backup settings for server #"+server);

            path=path.trim();
            if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
            int slashPos=path.indexOf('/');
            if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
            if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

            // Must be able to access the old and new packages
            int oldPackageNum=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select package from file_backup_settings where pkey=?", pkey);
            PackageHandler.checkAccessPackage(conn, source, "setFileBackupSetting", oldPackageNum);
            PackageHandler.checkAccessPackage(conn, source, "setFileBackupSetting", packageNum);

            conn.executeUpdate(
                "update\n"
                + "  file_backup_settings\n"
                + "set\n"
                + "  path=?,\n"
                + "  package=?,\n"
                + "  backup_level=?,\n"
                + "  backup_retention=?,\n"
                + "  recurse=?\n"
                + "where\n"
                + "  pkey=?",
                path,
                packageNum,
                backupLevel,
                backupRetention,
                recurse,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_SETTINGS,
                PackageHandler.getBusinessForPackage(conn, oldPackageNum),
                server,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.FILE_BACKUP_SETTINGS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                server,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setLastBackupTime(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int server,
        long time
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "setLastBackupTime(MasterDatabaseConnection,RequestSource,InvalidateList,int,long)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to set the last backup time.");
            ServerHandler.checkAccessServer(conn, source, "setLastBackupTime", server);

            conn.executeUpdate(
                "update servers set last_backup_time=? where pkey=?",
                new Timestamp(time),
                server
            );
            invalidateList.addTable(
                conn,
                SchemaTable.SERVERS,
                ServerHandler.getBusinessesForServer(conn, server),
                server,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Keeps track of file_backup_roots.
     */
    private static void fileBackupsAdded(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int[] file_paths,
        int server,
        int[] packages,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "fileBackupsAdded(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,int[],int,int[],int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            Map<Integer,Integer> filePathIndexes=new HashMap<Integer,Integer>(batchSize);
            for(int c=0;c<batchSize;c++) {
                if(filePathIndexes.put(file_paths[c], c)!=null) throw new SQLException("file_path found more than once in call to fileBackupsAdded: "+file_paths[c]);
            }

            StringBuilder SB=new StringBuilder();
            int[] parents=FilePathHandler.getParents(backupConn, file_paths, batchSize, filePathIndexes, SB);

            SB.setLength(0);
            // Remove the children that are of the same package
            SB.append(
                "select\n"
                + "  pkey,\n"
                + "  package\n"
                + "from\n"
                + "  file_backup_roots\n"
                + "where\n");
            for(int c=0;c<batchSize;c++) {
                SB.append(c>0?"  or (":"  (").append("parent=").append(file_paths[c]).append(" and server=").append(server).append(" and package=").append(packages[c]).append(")\n");
            }
            IntList deletePKeys=new IntArrayList();
            IntList deletePackages=new IntArrayList();
            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        deletePKeys.add(results.getInt(1));
                        deletePackages.add(results.getInt(2));
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
            int size=deletePKeys.size();
            if(size>0) {
                SB.setLength(0);
                SB.append("delete from file_backup_roots where pkey in (");
                for(int c=0;c<size;c++) {
                    if(c>0) SB.append(',');
                    SB.append(deletePKeys.getInt(c));
                }
                SB.append(')');
                backupConn.executeUpdate(SB.toString());

                for(int c=0;c<size;c++) {
                    invalidateList.addTable(
                        conn,
                        SchemaTable.FILE_BACKUP_ROOTS,
                        PackageHandler.getBusinessForPackage(conn, deletePackages.getInt(c)),
                        server,
                        false
                    );
                }
            }

            // Keep track of which ones still need added after filtering
            boolean[] needAddeds=new boolean[batchSize];

            // Find those that already exist
            SB.setLength(0);
            SB.append(
                "select\n"
                + "  file_path,\n"
                + "  package\n"
                + "from\n"
                + "  file_backup_roots\n"
                + "where\n"
                + "  server=").append(server).append("\n"
                + "  and file_path in (");
            boolean didOne=false;
            for(int c=0;c<batchSize;c++) {
                // Do not add if its parent is in the same batch, unless it is the root node
                int file_path=file_paths[c];
                boolean needAdded;
                int parent=parents[c];
                if(file_path==parent) needAdded=true;
                else {
                    Integer O=filePathIndexes.get(parent);
                    if(O!=null) {
                        int parentIndex=O.intValue();
                        if(packages[parentIndex]==packages[c]) needAdded=false;
                        else needAdded=true;
                    } else needAdded=true;
                }
                if(needAddeds[c]=needAdded) {
                    if(didOne) SB.append(',');
                    else didOne=true;
                    SB.append(file_path);
                }
            }
            SB.append(')');
            sqlString=null;
            stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        int file_path=results.getInt(1);
                        int packageNum=results.getInt(2);
                        int index=filePathIndexes.get(file_path).intValue();
                        if(packageNum==packages[index]) needAddeds[index]=false;
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
            
            // Add these if not already present and its parent is not of the same package and its parent is not in this batch
            SB.setLength(0);
            SB.append(
                "select\n"
                + "  file_path,\n"
                + "  package\n"
                + "from\n"
                + "  file_backups\n"
                + "where\n"
                + "  file_path in (");
            didOne=false;
            for(int c=0;c<batchSize;c++) {
                if(needAddeds[c]) {
                    if(didOne) SB.append(',');
                    else didOne=true;
                    SB.append(parents[c]);
                }
            }
            // Return if already done
            if(!didOne) return;
            SB.append(")\n"
                    + "  and server=").append(server);
            sqlString=null;
            stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        int parent=results.getInt(1);
                        int packageNum=results.getInt(2);
                        for(int c=0;c<batchSize;c++) {
                            if(needAddeds[c] && parents[c]==parent && packages[c]==packageNum) needAddeds[c]=false;
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }

            for(int c=0;c<batchSize;c++) {
                if(needAddeds[c]) {
                    int packageNum=packages[c];
                    backupConn.executeUpdate("insert into file_backup_roots (parent, file_path, server, package) values("+parents[c]+", "+file_paths[c]+", "+server+", "+packageNum+")");
                    invalidateList.addTable(
                        conn,
                        SchemaTable.FILE_BACKUP_ROOTS,
                        PackageHandler.getBusinessForPackage(conn, packageNum),
                        server,
                        false
                    );
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Keeps track of file_backup_roots.
     */
    private static void removedFileBackups(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int[] file_paths,
        int server,
        int[] packages,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "removingFileBackups(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,int[],int,int[],int)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            // Find those that don't have any remaining with same file_path, server, and package
            StringBuilder SB=new StringBuilder();
            SB.append(
                "select\n"
                + "  file_path,\n"
                + "  package\n"
                + "from\n"
                + "  file_backups\n"
                + "where\n"
                + "  file_path in (");
            for(int c=0;c<batchSize;c++) {
                if(c>0) SB.append(',');
                SB.append(file_paths[c]);
            }
            SB.append(
                ")\n"
                + "  and server=").append(server);
            boolean[] completelyRemoveds=new boolean[batchSize];
            Arrays.fill(completelyRemoveds, true);
            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        int file_path=results.getInt(1);
                        int packageNum=results.getInt(2);
                        for(int c=0;c<batchSize;c++) {
                            if(file_path==file_paths[c] && packageNum==packages[c]) completelyRemoveds[c]=false;
                        }
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }

            // Remove those that are no longer roots
            for(int c=0;c<batchSize;c++) {
                if(completelyRemoveds[c]) {
                    backupConn.executeUpdate("delete from file_backup_roots where server="+server+" and file_path="+file_paths[c]+" and package="+packages[c]);
                    invalidateList.addTable(
                        conn,
                        SchemaTable.FILE_BACKUP_ROOTS,
                        PackageHandler.getBusinessForPackage(conn, packages[c]),
                        server,
                        false
                    );
                }
            }

            // Find the new roots that are children of the old roots
            SB.setLength(0);
            SB.append(
                "select distinct\n"
                + "  fp.parent,\n"
                + "  fb.file_path,\n"
                + "  fb.package\n"
                + "from\n"
                + "  file_paths fp,\n"
                + "  file_backups fb\n"
                + "where\n"
                + "  fp.parent in (");
            boolean didOne=false;
            for(int c=0;c<batchSize;c++) {
                if(completelyRemoveds[c]) {
                    if(didOne) SB.append(',');
                    else didOne=true;
                    SB.append(file_paths[c]);
                }
            }
            if(didOne) {
                SB.append(
                    ")\n"
                    + "  and fp.pkey=fb.file_path\n"
                    + "  and fb.server=").append(server);

                IntList newRootParents=new IntArrayList();
                IntList newRootFilePaths=new IntArrayList();
                IntList newRootPackages=new IntArrayList();
                sqlString=null;
                stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                try {
                    backupConn.incrementQueryCount();
                    ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                    try {
                        while(results.next()) {
                            int parent=results.getInt(1);
                            int file_path=results.getInt(2);
                            int packageNum=results.getInt(3);
                            for(int c=0;c<batchSize;c++) {
                                if(
                                    completelyRemoveds[c]
                                    && parent==file_paths[c]
                                    && packageNum==packages[c]
                                ) {
                                    newRootParents.add(parent);
                                    newRootFilePaths.add(file_path);
                                    newRootPackages.add(packageNum);
                                }
                            }
                        }
                    } finally {
                        results.close();
                    }
                } catch(SQLException err) {
                    System.err.println("Error from query: "+sqlString);
                    throw err;
                } finally {
                    stmt.close();
                }

                // Add the new roots
                for(int c=0;c<newRootParents.size();c++) {
                    backupConn.executeUpdate(
                        "insert into file_backup_roots (\n"
                        + "  parent,\n"
                        + "  file_path,\n"
                        + "  server,\n"
                        + "  package\n"
                        + ") values (\n"
                        + "  "+newRootParents.getInt(c)+",\n"
                        + "  "+newRootFilePaths.getInt(c)+",\n"
                        + "  "+server+",\n"
                        + "  "+newRootPackages.getInt(c)+"\n"
                        + ")"
                    );
                    invalidateList.addTable(
                        conn,
                        SchemaTable.FILE_BACKUP_ROOTS,
                        PackageHandler.getBusinessForPackage(conn, newRootPackages.getInt(c)),
                        server,
                        false
                    );
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Used for temporary data conversion only.
     */
    /*
    public static void main(String[] args) {
        // Not really used
        InvalidateList invalidateList=new InvalidateList();
        try {
            MasterDatabaseConnection readConn=new Connection[1];
            Connection read=MasterDatabase.getDatabase().getConnection(false, readConn);
            try {
                MasterDatabaseConnection writeConn=new Connection[1];
                Connection write=MasterDatabase.getDatabase().getConnection(false, writeConn);
                try {
                    conn.executeUpdate(readConn, "declare get_file_backups cursor for select distinct file_path, server, package from file_backups order by package, server, file_path");
                    try {
                        Statement stmt=read.createStatement();
                        try {
                            int[] file_paths=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                            int[] servers=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                            int[] packages=new int[TableHandler.RESULT_SET_BATCH_SIZE];

                            int[] serverFilePaths=new int[TableHandler.RESULT_SET_BATCH_SIZE];
                            int[] serverPackages=new int[TableHandler.RESULT_SET_BATCH_SIZE];

                            int totalRows=0;
                            while(true) {
                                int batchSize=0;
                                ResultSet results=stmt.executeQuery("fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from get_file_backups");
                                try {
                                    while(results.next()) {
                                        file_paths[batchSize]=results.getInt(1);
                                        servers[batchSize]=results.getInt(2);
                                        packages[batchSize]=results.getInt(3);
                                        batchSize++;
                                    }
                                } finally {
                                    results.close();
                                }
                                if(batchSize>0) {
                                    totalRows+=batchSize;
                                    System.out.print("Read "+batchSize+" rows for a total of "+totalRows+" rows.  Building. . .");
                                    System.out.flush();

                                    // Add grouped by server
                                    int lastServer=-1;
                                    int currentServerCount=0;
                                    for(int c=0;c<=batchSize;c++) {
                                        if(lastServer==-1 || c>=batchSize || servers[c]!=lastServer) {
                                            if(currentServerCount>0) {
                                                fileBackupsAdded(writeConn, invalidateList, serverFilePaths, lastServer, serverPackages, currentServerCount);
                                            }
                                            if(c<batchSize) {
                                                lastServer=servers[c];
                                                currentServerCount=0;
                                            }
                                        }
                                        if(c<batchSize) {
                                            serverFilePaths[currentServerCount]=file_paths[c];
                                            serverPackages[currentServerCount]=packages[c];
                                            currentServerCount++;
                                        }
                                    }

                                    // Commit the changes
                                    write.commit();
                                    System.out.println("  Done");
                                    System.out.flush();
                                }

                                if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
                            }
                        } finally {
                            stmt.close();
                        }
                    } finally {
                        conn.executeUpdate(readConn, "close get_file_backups");
                    }
                } catch(IOException err) {
                    if(!write.isClosed()) write.rollback();
                    throw err;
                } catch(SQLException err) {
                    if(!write.isClosed()) write.rollback();
                    throw err;
                } finally {
                    MasterDatabase.getDatabase().releaseConnection(writeConn);
                }
                // Commit the changes
                read.commit();
            } catch(IOException err) {
                if(!read.isClosed()) read.rollback();
                throw err;
            } catch(SQLException err) {
                if(!read.isClosed()) read.rollback();
                throw err;
            } finally {
                MasterDatabase.getDatabase().releaseConnection(readConn);
            }
        } catch(IOException err) {
            ErrorPrinter.printStackTraces(err);
        } catch(SQLException err) {
            ErrorPrinter.printStackTraces(err);
        }
    }
     */

    public static void requestSendBackupDataToDaemon(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        RequestSource source,
        int backupData,
        long md5_hi,
        long md5_lo,
        CompressedDataOutputStream out
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "requestSendBackupDataToDaemon(MasterDatabaseConnection,BackupDatabaseConnection,RequestSource,int,long,long,CompressedDataOutputStream)", null);
        try {
            if(!OLD_BACKUP_ENABLED) throw new RuntimeException("Old backup disabled");

            // Must be a master user
            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not a master user and may not upload backup data");

            com.aoindustries.aoserv.client.MasterServer[] masterServers=MasterServer.getMasterServers(conn, mustring);
            if(masterServers.length!=0) {
                // Client should provide the appropriate MD5, as proof that is has access to the backup_data
                if(
                    md5_hi!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_hi from backup_data where pkey=?", backupData)
                    || md5_lo!=backupConn.executeLongQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select md5_lo from backup_data where pkey=?", backupData)
                ) throw new SQLException("requestSendBackupDataToDaemon attempt with mismatched MD5: backup_data.pkey="+backupData+", attempted md5_hi="+md5_hi+" md5_lo="+md5_lo);
            }
            boolean alreadyStored=isBackupDataFilled(backupConn, backupData);
            if(alreadyStored) {
                out.writeByte(AOServProtocol.DONE);
            } else {
                int backupPartition = backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select backup_partition from backup_data where pkey=?", backupData);
                int aoServer = getAOServerForBackupPartition(backupConn, backupPartition);
                long accessKey = DaemonHandler.grantDaemonAccess(
                    conn,
                    aoServer,
                    AOServDaemonProtocol.STORE_BACKUP_DATA_DIRECT_ACCESS,
                    Integer.toString(backupPartition),
                    Integer.toString(backupData),
                    MD5.getMD5String(md5_hi, md5_lo)
                );
                out.writeByte(AOServProtocol.NEXT);
                out.writeCompressedInt(aoServer);
                out.writeUTF(DaemonHandler.getDaemonConnectorIP(conn, aoServer));
                out.writeCompressedInt(DaemonHandler.getDaemonConnectorPort(conn, aoServer));
                out.writeUTF(DaemonHandler.getDaemonConnectorProtocol(conn, aoServer));
                out.writeLong(accessKey);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
