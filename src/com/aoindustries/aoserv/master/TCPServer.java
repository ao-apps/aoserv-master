package com.aoindustries.aoserv.master;

/*
 * Copyright 2000-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.AOPool;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>TCPServer</code> accepts connections from an <code>SimpleAOClient</code>.
 * Once the connection is accepted and authenticated, the server carries out all actions requested
 * by the client while providing the necessary security checks and data filters.
 * <p>
 * This server is completely threaded to handle multiple, simultaneous clients.
 * </p>
 * @author  AO Industries, Inc.
 */
public class TCPServer extends MasterServer implements Runnable {

    /**
     * The protocol of this server.
     */
    static final String PROTOCOL="tcp";
    
    /**
     * The thead that is listening.
     */
    protected Thread thread;

    /**
     * Creates a new, running <code>AOServServer</code>.
     */
    public TCPServer(String serverBind, int serverPort) {
        super(serverBind, serverPort);
        Profiler.startProfile(Profiler.FAST, TCPServer.class, "<init>(String,int)", null);
        try {
            (thread=new Thread(this, getClass().getName()+"?address="+serverBind+"&port="+serverPort)).start();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
    
    public String getProtocol() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, TCPServer.class, "getProtocol()", null);
        try {
            return PROTOCOL;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Determines if communication on this server is secure.
     */
    public boolean isSecure() throws UnknownHostException {
        Profiler.startProfile(Profiler.FAST, TCPServer.class, "isSecure()", null);
        try {
            byte[] address=InetAddress.getByName(getBindAddress()).getAddress();
            if(
                address[0]==(byte)127
                || address[0]==(byte)10
                || (
                    address[0]==(byte)192
                    && address[1]==(byte)168
                )
            ) return true;
            // Allow same class C network
            byte[] localAddress=InetAddress.getByName(serverBind).getAddress();
            return
                address[0]==localAddress[0]
                && address[1]==localAddress[1]
                && address[2]==localAddress[2]
            ;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public void run() {
        Profiler.startProfile(Profiler.UNKNOWN, TCPServer.class, "run()", null);
        try {
            while (true) {
                try {
                    InetAddress address=InetAddress.getByName(serverBind);
                    synchronized(System.out) {
                        System.out.println("Accepting TCP connections on "+address.getHostAddress()+':'+serverPort);
                    }
                    ServerSocket SS = new ServerSocket(serverPort, 50, address);
                    try {
                        while (true) {
                            Socket socket=SS.accept();
                            incConnectionCount();
                            try {
                                socket.setKeepAlive(true);
                                socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
                                //socket.setTcpNoDelay(true);
                                new SocketServerThread(this, socket);
                            } catch(ThreadDeath TD) {
                                throw TD;
                            } catch(Throwable T) {
                                reportError(
                                    T,
                                    new Object[] {
                                        "serverPort="+serverPort,
                                        "address="+address
                                    }
                                );
                            }
                        }
                    } finally {
                        SS.close();
                    }
                } catch (ThreadDeath TD) {
                    throw TD;
                } catch (Throwable T) {
                    reportError(
                        T,
                        new Object[] {
                            "serverPort="+serverPort,
                            "serverBind="+serverBind
                        }
                    );
                }
                try {
                    thread.sleep(15000);
                } catch (InterruptedException err) {
                    reportWarning(err, null);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}