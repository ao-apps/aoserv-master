package com.aoindustries.aoserv.master;

/*
 * Copyright 2000-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.AOPool;
import java.io.*;
import java.net.*;
import java.security.*;
import javax.net.ssl.*;

/**
 * The <code>AOServServer</code> accepts connections from an <code>SimpleAOClient</code>.
 * Once the connection is accepted and authenticated, the server carries out all actions requested
 * by the client while providing the necessary security checks and data filters.
 * <p>
 * This server is completely threaded to handle multiple, simultaneous clients.
 * </p>
 * @author  AO Industries, Inc.
 */
public class SSLServer extends TCPServer {

    /**
     * Flags that the SSL factory has already been loaded.
     */
    public static final boolean[] sslProviderLoaded=new boolean[1];

    /**
     * The protocol of this server.
     */
    static final String PROTOCOL="ssl";
    
    /**
     * Creates a new, running <code>AOServServer</code>.
     */
    public SSLServer(String serverBind, int serverPort) {
        super(serverBind, serverPort);
    }

    @Override
    public String getProtocol() {
        return PROTOCOL;
    }

    /**
     * Determines if communication on this server is secure.
     */
    @Override
    public boolean isSecure() throws UnknownHostException {
        return true;
    }

    @Override
    public void run() {
        synchronized(SSLServer.class) {
            try {
                if(!sslProviderLoaded[0]) {
                    System.setProperty(
                        "javax.net.ssl.keyStorePassword",
                        MasterConfiguration.getSSLKeystorePassword()
                    );
                    System.setProperty(
                        "javax.net.ssl.keyStore",
                        MasterConfiguration.getSSLKeystorePath()
                    );
                    Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
                    sslProviderLoaded[0]=true;
                }
            } catch(IOException err) {
                reportError(err, null);
                return;
            }
        }

        SSLServerSocketFactory factory = (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();
        while (true) {
            try {
                InetAddress address=InetAddress.getByName(serverBind);
                synchronized(System.out) {
                    System.out.println("Accepting SSL connections on "+address.getHostAddress()+':'+serverPort);
                }
                SSLServerSocket SS=(SSLServerSocket)factory.createServerSocket(serverPort, 50, address);

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
                reportError(T, null);
            }
            try {
                Thread.sleep(15000);
            } catch (InterruptedException err) {
                reportWarning(err, null);
            }
        }
    }
}