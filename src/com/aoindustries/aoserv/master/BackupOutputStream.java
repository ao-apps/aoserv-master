package com.aoindustries.aoserv.master;

/*
 * Copyright 2002-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;

/**
 * A <code>BackupOutputStream</code> stores the data on the provided backup
 * server in the provided backup file.  In the event of an error, the process may
 * be canceled by calling <code>rollback()</code>.  During the close of the stream,
 * all connections to the daemon are released and the <code>backup_data</code> table
 * is updated to indicate the file is stored and provided the compressed size.
 * If <code>rollback()</code> is called before <code>close()</code>, then the
 * <code>backup_data</code> table is not updated.
 *
 * @author  AO Industries, Inc.
 */
public final class BackupOutputStream extends OutputStream {
    
    final private MasterDatabaseConnection conn;
    final private BackupDatabaseConnection backupConn;
    final private InvalidateList invalidateList;
    final private int backupDataPKey;
    final private String filename;
    final private boolean isCompressed;
    private AOServDaemonConnector connector;
    private AOServDaemonConnection daemonConnection;
    private CompressedDataInputStream daemonIn;
    private CompressedDataOutputStream daemonOut;
    private long compressedSize=0;

    public BackupOutputStream(
        MasterDatabaseConnection conn,
        BackupDatabaseConnection backupConn,
        InvalidateList invalidateList,
        int backupDataPKey,
        long expectedMD5Hi,
        long expectedMD5Lo,
        String filename,
        boolean isCompressed
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupOutputStream.class, "<init>(MasterDatabaseConnection,BackupDatabaseConnection,InvalidateList,int,long,long,String,boolean)", null);
        try {
            this.conn=conn;
            this.backupConn=backupConn;
            this.invalidateList=invalidateList;
            this.backupDataPKey=backupDataPKey;
            this.filename=filename;
            int aoServer;
            int backupPartition;
            PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
                "select\n"
                + "  bp.pkey,\n"
                + "  bp.ao_server\n"
                + "from\n"
                + "  backup_data bd,\n"
                + "  backup_partitions bp\n"
                + "where\n"
                + "  bd.pkey=?\n"
                + "  and bd.backup_partition=bp.pkey"
            );
            try {
                pstmt.setInt(1, backupDataPKey);
                backupConn.incrementQueryCount();
                ResultSet results=pstmt.executeQuery();
                try {
                    if(results.next()) {
                        backupPartition=results.getInt(1);
                        aoServer=results.getInt(2);
                    } else throw new SQLException("No row returned.");
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }

            synchronized(BackupHandler.getBackupPartitionLock(backupPartition)) {
                this.connector=DaemonHandler.getDaemonConnector(conn, aoServer);
                boolean initDone=false;
                try {
                    this.daemonConnection=connector.getConnection(2);
                    this.daemonIn=daemonConnection.getInputStream();
                    this.daemonOut=daemonConnection.getOutputStream();
                    this.isCompressed=isCompressed;
                    daemonOut.writeCompressedInt(AOServDaemonProtocol.STORE_BACKUP_DATA);
                    daemonOut.writeCompressedInt(backupPartition);
                    StringBuilder SB=new StringBuilder();
                    SB.append(BackupData.getRelativePathPrefix(backupDataPKey)).append(filename);
                    if(isCompressed) SB.append(".gz");
                    daemonOut.writeUTF(SB.toString());
                    daemonOut.writeBoolean(isCompressed);
                    daemonOut.writeLong(expectedMD5Hi);
                    daemonOut.writeLong(expectedMD5Lo);
                    initDone=true;
                } catch(IOException err) {
                    daemonOut.close();
                    throw err;
                } finally {
                    if(!initDone) {
                        connector.releaseConnection(daemonConnection);
                        connector=null;
                        daemonOut=null;
                    }
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public void close() throws IOException {
        Profiler.startProfile(Profiler.FAST, BackupOutputStream.class, "close()", null);
        try {
	    synchronized(this) {
		close0(true);
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    private void close0(boolean commit) throws IOException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupOutputStream.class, "close0(boolean)", null);
        try {
            if(daemonOut!=null) {
                try {
                    daemonOut.writeByte(AOServDaemonProtocol.DONE);
                    daemonOut.writeBoolean(commit);
                    daemonOut.flush();
                    int code=daemonIn.read();
                    if(code!=AOServDaemonProtocol.DONE) {
                        if(code==AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(daemonIn.readUTF());
                        if(code==AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(daemonIn.readUTF());
                        throw new IOException("Unknown daemon response code: "+code);
                    }
                    if(isCompressed) {
                        backupConn.executeUpdate(
                            "update backup_data set compressed_size=?, is_stored=true where pkey=?",
                            compressedSize,
                            backupDataPKey
                        );
                    } else {
                        backupConn.executeUpdate(
                            "update backup_data set is_stored=true where pkey=?",
                            backupDataPKey
                        );
                    }
                    invalidateList.addTable(conn, SchemaTable.TableID.BACKUP_DATA, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                } catch(IOException err) {
                    daemonConnection.close();
                    throw err;
                } catch(SQLException err) {
                    daemonConnection.close();
                    throw new IOException(err.toString());
                } finally {
                    connector.releaseConnection(daemonConnection);
                    connector=null;
                    daemonConnection=null;
                    daemonIn=null;
                    daemonOut=null;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public void flush() throws IOException {
        Profiler.startProfile(Profiler.FAST, BackupOutputStream.class, "flush()", null);
        try {
	    synchronized(this) {
		try {
		    daemonOut.flush();
		} catch(IOException err) {
		    rollback();
		    throw err;
		}
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public void write(byte[] buff, int off, int len) throws IOException {
        Profiler.startProfile(Profiler.IO, BackupOutputStream.class, "write(byte[],int,int)", null);
        try {
	    synchronized(this) {
		try {
		    while(len>0) {
			int blockLen=len;
			if(blockLen>BufferManager.BUFFER_SIZE) blockLen=BufferManager.BUFFER_SIZE;
			daemonOut.write(AOServDaemonProtocol.NEXT);
			daemonOut.writeShort(blockLen);
			daemonOut.write(buff, off, blockLen);
			off+=blockLen;
			len-=blockLen;
			if(isCompressed) compressedSize+=blockLen;
		    }
		} catch(IOException err) {
		    rollback();
		    throw err;
		}
	    }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }
    
    public void write(int b) throws IOException {
        Profiler.startProfile(Profiler.IO, BackupOutputStream.class, "write(int)", null);
        try {
	    synchronized(this) {
		try {
		    daemonOut.write(AOServDaemonProtocol.NEXT);
		    daemonOut.writeShort(1);
		    daemonOut.write(b);
		    if(isCompressed) compressedSize++;
		} catch(IOException err) {
		    rollback();
		    throw err;
		}
	    }
	} finally {
	    Profiler.endProfile(Profiler.IO);
	}
    }
    
    public void rollback() {
        Profiler.startProfile(Profiler.UNKNOWN, BackupOutputStream.class, "rollback()", null);
        try {
	    synchronized(this) {
		close0(false);
	    }
        } catch(IOException err) {
            MasterServer.reportError(err, null);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
