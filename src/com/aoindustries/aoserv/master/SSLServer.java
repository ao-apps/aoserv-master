/*
 * Copyright 2000-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.io.AOPool;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

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

    private static final Logger logger = LogFactory.getLogger(ServerHandler.class);

    /**
     * The protocol of this server.
     */
    static final String PROTOCOL_SSL="ssl";
    
    /**
     * Creates a new, running <code>AOServServer</code>.
     */
    SSLServer(String serverBind, int serverPort) {
        super(serverBind, serverPort);
    }

    @Override
    public String getProtocol() {
        return PROTOCOL_SSL;
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
        try {
            System.setProperty(
                "javax.net.ssl.keyStorePassword",
                MasterConfiguration.getSSLKeystorePassword()
            );
            System.setProperty(
                "javax.net.ssl.keyStore",
                MasterConfiguration.getSSLKeystorePath()
            );
        } catch(IOException err) {
            logger.log(Level.SEVERE, null, err);
            return;
        }

        SSLServerSocketFactory factory = (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();
        while (true) {
            try {
                InetAddress address=InetAddress.getByName(serverBind);
                synchronized(System.out) {
                    System.out.println("Accepting SSL connections on "+address.getHostAddress()+':'+serverPort);
                }
                try (SSLServerSocket SS = (SSLServerSocket)factory.createServerSocket(serverPort, 50, address)) {
                    while (true) {
                        Socket socket=SS.accept();
                        incConnectionCount();
                        try {
                            socket.setKeepAlive(true);
                            socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
                            //socket.setTcpNoDelay(true);
                            new SocketServerThread(this, socket).start();
                        } catch(ThreadDeath TD) {
                            throw TD;
                        } catch(Throwable T) {
                            logger.log(Level.SEVERE, "serverPort="+serverPort+", address="+address, T);
                        }
                    }
                }
            } catch (ThreadDeath TD) {
                throw TD;
            } catch (Throwable T) {
                logger.log(Level.SEVERE, null, T);
            }
            try {
                Thread.sleep(15000);
            } catch (InterruptedException err) {
                logger.log(Level.WARNING, null, err);
				// Restore the interrupted status
				Thread.currentThread().interrupt();
            }
        }
    }
}