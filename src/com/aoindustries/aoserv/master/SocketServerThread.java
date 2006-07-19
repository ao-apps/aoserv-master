package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.MasterProcess;
import com.aoindustries.aoserv.daemon.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>AOServServerThread</code> handles a connection once it is accepted.
 *
 * @author  AO Industries, Inc.
 */
final public class SocketServerThread extends Thread implements RequestSource {

    /**
     * The <code>TCPServer</code> that created this <code>SocketServerThread</code>.
     */
    private final TCPServer server;
    
    /**
     * The <code>Socket</code> that is connected.
     */
    private final Socket socket;
    
    /**
     * The <code>CompressedCompressedDataInputStream</code> that is being read from.
     */
    private final CompressedDataInputStream in;
    
    /**
     * The <code>CompressedDataOutputStream</code> that is being written to.
     */
    private final CompressedDataOutputStream out;
    
    /**
     * The version of the protocol the client is running.
     */
    private final String protocolVersion;

    /**
     * The server if this is a connection from a daemon.
     */
    
    /**
     * The master process.
     */
    private final MasterProcess process;

    private boolean isClosed=true;

    /**
     * Creates a new, running <code>AOServServerThread</code>.
     */
    public SocketServerThread(TCPServer server, Socket socket) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, SocketServerThread.class, "<init>(TCPServer,Socket)", null);
        try {
            this.server = server;
            this.socket = socket;
            this.in=new CompressedDataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out=new CompressedDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            String host=socket.getInetAddress().getHostAddress();
            process=MasterProcessManager.createProcess(
                host,
                server.getProtocol(),
                server.isSecure()
            );
            boolean done=false;
            try {
                this.protocolVersion=in.readUTF();
                process.setAOServProtocol(protocolVersion);
                if(in.readBoolean()) {
                    String daemonServerHostname=in.readUTF();
                    MasterDatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
                    try {
                        process.setDeamonServer(ServerHandler.getPKeyForServer(conn, daemonServerHostname));
                    } catch(IOException err) {
                        conn.rollbackAndClose();
                        throw err;
                    } catch(SQLException err) {
                        conn.rollbackAndClose();
                        throw err;
                    } finally {
                        conn.releaseConnection();
                    }
                } else process.setDeamonServer(-1);
                process.setEffectiveUser(in.readUTF());
                process.setAuthenticatedUser(in.readUTF());
                setName(
                    "SocketServerThread"
                    + "?"
                    + host
                    + ":"
                    + socket.getPort()
                    + "->"
                    + socket.getLocalAddress().getHostAddress()
                    + ":"
                    + socket.getLocalPort()
                    + " as "
                    + process.getEffectiveUser()
                    + " ("
                    + process.getAuthenticatedUser()
                    + ")"
                );
                done=true;
            } finally {
                if(!done) {
                    MasterProcessManager.removeProcess(process);
                }
            }
            isClosed=false;
            start();
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private final List<InvalidateCacheEntry> invalidateLists=new ArrayList<InvalidateCacheEntry>();

    /**
     * Invalidates the listed tables.  Also, if this connector represents a daemon,
     * this invalidate is registered with ServerHandler for invalidation synchronization.
     */
    public void cachesInvalidated(IntList tableList) throws IOException {
        Profiler.startProfile(Profiler.FAST, SocketServerThread.class, "cachesInvalidated(IntList)", null);
        try {
	    synchronized(this) {
		if(tableList!=null && tableList.size()>0) {
		    // Register with ServerHandler for invalidation synchronization
		    int daemonServer=getDaemonServer();
                    IntList copy=new IntArrayList(tableList);
		    InvalidateCacheEntry ice=new InvalidateCacheEntry(
                        copy,
                        daemonServer,
                        daemonServer==-1?null:ServerHandler.addInvalidateSyncEntry(daemonServer, this)
                    );
		    invalidateLists.add(ice);
		    notify();
		}
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public int getDaemonServer() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, SocketServerThread.class, "getDaemonServer()", null);
        try {
            return process.getDaemonServer();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public InvalidateCacheEntry getNextInvalidatedTables() {
        Profiler.startProfile(Profiler.FAST, SocketServerThread.class, "getNextInvalidatedTables()", null);
        try {
	    synchronized(this) {
		if(invalidateLists.size()==0) return null;
		return invalidateLists.remove(0);
	    }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    final public long getConnectorID() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, SocketServerThread.class, "getConnectorID()", null);
        try {
            return process.getConnectorID();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    final public String getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Logs a security message to <code>System.err</code>.
     * Also sends email messages to <code>aoserv.server.
     */
    public String getSecurityMessageHeader() {
        Profiler.startProfile(Profiler.UNKNOWN, SocketServerThread.class, "getSecurityMessageHeader()", null);
        try {
            return "IP="+socket.getInetAddress().getHostAddress()+" EffUsr="+process.getEffectiveUser()+" AuthUsr="+process.getAuthenticatedUser();
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public String getUsername() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, SocketServerThread.class, "getUsername()", null);
        try {
            return process.getEffectiveUser();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public boolean isSecure() throws UnknownHostException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, SocketServerThread.class, "isSecure()", null);
        try {
            return server.isSecure();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void run() {
        Profiler.startProfile(Profiler.IO, SocketServerThread.class, "run()", null);
        try {
            try {
                try {
                    String password=in.readUTF();
                    long existingID=in.readLong();

                    if(
                        !protocolVersion.equals(AOServProtocol.VERSION_1_12)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_11)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_10)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_9)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_8)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_7)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_6)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_5)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_4)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_3)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_2)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_1)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_130)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_129)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_128)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_127)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_126)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_125)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_124)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_123)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_122)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_121)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_120)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_119)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_118)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_117)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_116)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_115)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_114)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_113)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_112)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_111)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_110)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_109)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_108)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_107)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_106)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_105)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_104)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_103)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_102)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_101)
                        && !protocolVersion.equals(AOServProtocol.VERSION_1_0_A_100)
                    ) {
                        out.writeBoolean(false);
                        out.writeUTF(
                        "Client ("+socket.getInetAddress().getHostAddress()+":"+socket.getPort()+") requesting AOServ Protocol version "
                        +protocolVersion
                        +", server ("+socket.getLocalAddress().getHostAddress()+":"+socket.getLocalPort()+") supporting versions "
                        +StringUtility.buildList(AOServProtocol.getVersions())
                        +".  Please upgrade the client code to match the server."
                        );
                        out.flush();
                    } {
                        String message;
                        MasterDatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
                        try {
                            try {
                                message=MasterServer.authenticate(conn, socket.getInetAddress().getHostAddress(), process.getEffectiveUser(), process.getAuthenticatedUser(), password);
                            } catch(SQLException err) {
                                conn.rollbackAndClose();
                                throw err;
                            } finally {
                                conn.releaseConnection();
                            }

                            if(message!=null) {
                                //MasterServer.reportSecurityMessage(this, message, process.getEffectiveUser().length()>0 && password.length()>0);
                                out.writeBoolean(false);
                                out.writeUTF(message);
                                out.flush();
                            } else {
                                // Only master users may provide a daemon_server
                                boolean isOK=true;
                                int daemonServer=process.getDaemonServer();
                                if(daemonServer!=-1) {
                                    try {
                                        if(MasterServer.getMasterUser(conn, process.getEffectiveUser())==null) {
                                            conn.releaseConnection();
                                            out.writeBoolean(false);
                                            out.writeUTF("Only master users may register a daemon server.");
                                            out.flush();
                                            isOK=false;
                                        } else {
                                            com.aoindustries.aoserv.client.MasterServer[] servers=MasterServer.getMasterServers(conn, process.getEffectiveUser());
                                            conn.releaseConnection();
                                            if(servers.length!=0) {
                                                isOK=false;
                                                for(int c=0;c<servers.length;c++) {
                                                    if(servers[c].getServerPKey()==daemonServer) {
                                                        isOK=true;
                                                        break;
                                                    }
                                                }
                                                if(!isOK) {
                                                    out.writeBoolean(false);
                                                    out.writeUTF("Master user ("+process.getEffectiveUser()+") not allowed to access server: "+daemonServer);
                                                    out.flush();
                                                }
                                            }
                                        }
                                    } catch(SQLException err) {
                                        conn.rollbackAndClose();
                                        throw err;
                                    } finally {
                                        conn.releaseConnection();
                                    }
                                }
                                if(isOK) {
                                    out.writeBoolean(true);
                                    if(existingID==-1) {
                                        process.setConnectorID(MasterServer.getNextConnectorID());
                                        out.writeLong(process.getConnectorID());
                                    } else process.setConnectorID(existingID);
                                    out.flush();

                                    try {
                                        MasterServer.updateAOServProtocolLastUsed(conn, protocolVersion);
                                    } catch(SQLException err) {
                                        conn.rollbackAndClose();
                                        throw err;
                                    } finally {
                                        conn.releaseConnection();
                                    }

                                    while(server.handleRequest(this, in, out, process));
                                }
                            }
                        } catch(SQLException err) {
                            conn.rollbackAndClose();
                            throw err;
                        } finally {
                            conn.releaseConnection();
                        }
                    }
                } catch(EOFException err) {
                    // Normal when disconnecting
                } catch(SocketException err) {
                    // Connection reset common for abnormal client disconnects
                    String message=err.getMessage();
                    if(
                        !"Broken pipe".equalsIgnoreCase(message)
                        && !"Connection reset".equalsIgnoreCase(message)
                    ) MasterServer.reportError(err, null);
                } catch(IOException err) {
                    // Broken pipe common for abnormal client disconnects
                    String message=err.getMessage();
                    if(
                        !"Broken pipe".equalsIgnoreCase(message)
                    ) MasterServer.reportError(err, null);
                } catch(SQLException err) {
                    MasterServer.reportError(err, null);
                } catch(ThreadDeath TD) {
                    throw TD;
                } catch (Throwable T) {
                    MasterServer.reportError(T, null);
                } finally {
                    // Close the socket
                    try {
                        isClosed=true;
                        socket.close();
                    } catch (IOException err) {
                        MasterServer.reportError(err, null);
                    }
                }
            } finally {
                MasterProcessManager.removeProcess(process);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }
    
    public boolean isClosed() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, SocketServerThread.class, "isClosed()", null);
        try {
            return isClosed;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
}
