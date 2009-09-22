package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.MasterProcess;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.IntArrayList;
import com.aoindustries.util.IntList;
import com.aoindustries.util.StringUtility;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

/**
 * The <code>AOServServerThread</code> handles a connection once it is accepted.
 *
 * @author  AO Industries, Inc.
 */
final public class SocketServerThread extends Thread implements RequestSource {

    private static final Logger logger = LogFactory.getLogger(SocketServerThread.class);

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
    private AOServProtocol.Version protocolVersion;

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
        isClosed=false;
        start();
    }

    private final LinkedList<InvalidateCacheEntry> invalidateLists=new LinkedList<InvalidateCacheEntry>();

    /**
     * Invalidates the listed tables.  Also, if this connector represents a daemon,
     * this invalidate is registered with ServerHandler for invalidation synchronization.
     *
     * IDEA: Could reduce signals under high load by combining entries that are not synchronous.
     *       Could even combine synchronous ones as long as all sync entries were acknowledged in the proper order.
     */
    public void cachesInvalidated(IntList tableList) throws IOException {
        if(tableList!=null && tableList.size()>0) {
            synchronized(this) { // Must use "this" lock because wait is performed on this object externally
                // Register with ServerHandler for invalidation synchronization
                int daemonServer=getDaemonServer();
                IntList copy=new IntArrayList(tableList);
                InvalidateCacheEntry ice=new InvalidateCacheEntry(
                    copy,
                    daemonServer,
                    daemonServer==-1?null:ServerHandler.addInvalidateSyncEntry(daemonServer, this)
                );
                invalidateLists.addLast(ice);
                notify();
            }
        }
    }

    public int getDaemonServer() {
        return process.getDaemonServer();
    }

    public InvalidateCacheEntry getNextInvalidatedTables() {
        synchronized(this) {
            if(invalidateLists.isEmpty()) return null;
            return invalidateLists.removeFirst();
        }
    }

    final public long getConnectorID() {
        return process.getConnectorID();
    }

    final public AOServProtocol.Version getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Logs a security message to <code>System.err</code>.
     * Also sends email messages to <code>aoserv.server.
     */
    public String getSecurityMessageHeader() {
        return "IP="+socket.getInetAddress().getHostAddress()+" EffUsr="+process.getEffectiveUser()+" AuthUsr="+process.getAuthenticatedUser();
    }

    public String getUsername() {
        return process.getEffectiveUser();
    }

    public boolean isSecure() throws UnknownHostException {
        return server.isSecure();
    }

    @Override
    public void run() {
        try {
            try {
                this.protocolVersion=AOServProtocol.Version.getVersion(in.readUTF());
                process.setAOServProtocol(protocolVersion.getVersion());
                if(in.readBoolean()) {
                    String daemonServerHostname=in.readUTF();
                    DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
                    try {
                        process.setDeamonServer(ServerHandler.getServerForAOServerHostname(conn, daemonServerHostname));
                    } catch(RuntimeException err) {
                        conn.rollback();
                        throw err;
                    } catch(IOException err) {
                        conn.rollback();
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
                String host=socket.getInetAddress().getHostAddress();
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
                String password=in.readUTF();
                long existingID=in.readLong();

                switch(protocolVersion) {
                    case VERSION_1_58 :
                    case VERSION_1_57 :
                    case VERSION_1_56 :
                    case VERSION_1_55 :
                    case VERSION_1_54 :
                    case VERSION_1_53 :
                    case VERSION_1_52 :
                    case VERSION_1_51 :
                    case VERSION_1_50 :
                    case VERSION_1_49 :
                    case VERSION_1_48 :
                    case VERSION_1_47 :
                    case VERSION_1_46 :
                    case VERSION_1_45 :
                    case VERSION_1_44 :
                    case VERSION_1_43 :
                    case VERSION_1_42 :
                    case VERSION_1_41 :
                    case VERSION_1_40 :
                    case VERSION_1_39 :
                    case VERSION_1_38 :
                    case VERSION_1_37 :
                    case VERSION_1_36 :
                    case VERSION_1_35 :
                    case VERSION_1_34 :
                    case VERSION_1_33 :
                    case VERSION_1_32 :
                    case VERSION_1_31 :
                    case VERSION_1_30 :
                    case VERSION_1_29 :
                    case VERSION_1_28 :
                    case VERSION_1_27 :
                    case VERSION_1_26 :
                    case VERSION_1_25 :
                    case VERSION_1_24 :
                    case VERSION_1_23 :
                    case VERSION_1_22 :
                    case VERSION_1_21 :
                    case VERSION_1_20 :
                    case VERSION_1_19 :
                    case VERSION_1_18 :
                    case VERSION_1_17 :
                    case VERSION_1_16 :
                    case VERSION_1_15 :
                    case VERSION_1_14 :
                    case VERSION_1_13 :
                    case VERSION_1_12 :
                    case VERSION_1_11 :
                    case VERSION_1_10 :
                    case VERSION_1_9 :
                    case VERSION_1_8 :
                    case VERSION_1_7 :
                    case VERSION_1_6 :
                    case VERSION_1_5 :
                    case VERSION_1_4 :
                    case VERSION_1_3 :
                    case VERSION_1_2 :
                    case VERSION_1_1 :
                    case VERSION_1_0_A_130 :
                    case VERSION_1_0_A_129 :
                    case VERSION_1_0_A_128 :
                    case VERSION_1_0_A_127 :
                    case VERSION_1_0_A_126 :
                    case VERSION_1_0_A_125 :
                    case VERSION_1_0_A_124 :
                    case VERSION_1_0_A_123 :
                    case VERSION_1_0_A_122 :
                    case VERSION_1_0_A_121 :
                    case VERSION_1_0_A_120 :
                    case VERSION_1_0_A_119 :
                    case VERSION_1_0_A_118 :
                    case VERSION_1_0_A_117 :
                    case VERSION_1_0_A_116 :
                    case VERSION_1_0_A_115 :
                    case VERSION_1_0_A_114 :
                    case VERSION_1_0_A_113 :
                    case VERSION_1_0_A_112 :
                    case VERSION_1_0_A_111 :
                    case VERSION_1_0_A_110 :
                    case VERSION_1_0_A_109 :
                    case VERSION_1_0_A_108 :
                    case VERSION_1_0_A_107 :
                    case VERSION_1_0_A_106 :
                    case VERSION_1_0_A_105 :
                    case VERSION_1_0_A_104 :
                    case VERSION_1_0_A_103 :
                    case VERSION_1_0_A_102 :
                    case VERSION_1_0_A_101 :
                    case VERSION_1_0_A_100 :
                    {
                        String message;
                        DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
                        try {
                            try {
                                message=MasterServer.authenticate(conn, socket.getInetAddress().getHostAddress(), process.getEffectiveUser(), process.getAuthenticatedUser(), password);
                            } catch(RuntimeException err) {
                                conn.rollback();
                                throw err;
                            } catch(IOException err) {
                                conn.rollback();
                                throw err;
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
                                    } catch(RuntimeException err) {
                                        conn.rollback();
                                        throw err;
                                    } catch(IOException err) {
                                        conn.rollback();
                                        throw err;
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
                                    } catch(RuntimeException err) {
                                        conn.rollback();
                                        throw err;
                                    } catch(IOException err) {
                                        conn.rollback();
                                        throw err;
                                    } catch(SQLException err) {
                                        conn.rollbackAndClose();
                                        throw err;
                                    } finally {
                                        conn.releaseConnection();
                                    }

                                    while(server.handleRequest(this, in, out, process)) {
                                        // Do nothing in loop
                                    }
                                }
                            }
                        } catch(RuntimeException err) {
                            conn.rollback();
                            throw err;
                        } catch(IOException err) {
                            conn.rollback();
                            throw err;
                        } catch(SQLException err) {
                            conn.rollbackAndClose();
                            throw err;
                        } finally {
                            conn.releaseConnection();
                        }
                        break;
                    }
                    default :
                    {
                        out.writeBoolean(false);
                        out.writeUTF(
                            "Client ("+socket.getInetAddress().getHostAddress()+":"+socket.getPort()+") requesting AOServ Protocol version "
                            +protocolVersion
                            +", server ("+socket.getLocalAddress().getHostAddress()+":"+socket.getLocalPort()+") supporting versions "
                            +StringUtility.buildList(AOServProtocol.Version.values())
                            +".  Please upgrade the client code to match the server."
                        );
                        out.flush();
                    }
                }
            } catch(EOFException err) {
                // Normal when disconnecting
            } catch(SSLHandshakeException err) {
                String message = err.getMessage();
                if(
                    !"Remote host closed connection during handshake".equalsIgnoreCase(message)
                ) logger.log(Level.SEVERE, null, err);
            } catch(SSLException err) {
                String message = err.getMessage();
                if(
                    !"Connection has been shutdown: javax.net.ssl.SSLException: java.net.SocketException: Connection reset".equalsIgnoreCase(message)
                ) logger.log(Level.SEVERE, null, err);
            } catch(SocketException err) {
                // Connection reset common for abnormal client disconnects
                String message=err.getMessage();
                if(
                    !"Broken pipe".equalsIgnoreCase(message)
                    && !"Connection reset".equalsIgnoreCase(message)
                    && !"Connection timed out".equalsIgnoreCase(message)
                ) logger.log(Level.SEVERE, null, err);
            } catch(IOException err) {
                // Broken pipe common for abnormal client disconnects
                String message=err.getMessage();
                if(
                    !"Broken pipe".equalsIgnoreCase(message)
                ) logger.log(Level.SEVERE, null, err);
            } catch(SQLException err) {
                logger.log(Level.SEVERE, null, err);
            } catch(ThreadDeath TD) {
                throw TD;
            } catch (Throwable T) {
                logger.log(Level.SEVERE, null, T);
            } finally {
                // Close the socket
                try {
                    isClosed=true;
                    socket.close();
                } catch (IOException err) {
                    logger.log(Level.SEVERE, null, err);
                }
            }
        } finally {
            MasterProcessManager.removeProcess(process);
        }
    }
    
    public boolean isClosed() {
        return isClosed;
    }
}
