/*
 * Copyright 2000-2013, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.io.AOPool;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger logger = LogFactory.getLogger(TCPServer.class);

    /**
     * The protocol of this server.
     */
    static final String PROTOCOL_TCP = "tcp";
    
    /**
     * The thread that is listening.
     */
    protected Thread thread;

    /**
     * Creates a new, running <code>AOServServer</code>.
     */
    TCPServer(String serverBind, int serverPort) {
        super(serverBind, serverPort);
    }

	void start() {
		if(thread != null) throw new IllegalStateException();
        (thread = new Thread(this, getClass().getName() + "?address=" + serverBind + "&port=" + serverPort)).start();
	}

	@Override
    public String getProtocol() {
        return PROTOCOL_TCP;
    }

    /**
     * Determines if communication on this server is secure.
     */
    public boolean isSecure() throws UnknownHostException {
        byte[] address = InetAddress.getByName(getBindAddress()).getAddress();
        if(
            address[0] == (byte)127
            || address[0] == (byte)10
            || (
                address[0] == (byte)192
                && address[1] == (byte)168
            )
        ) return true;
        // Allow same class C network
        byte[] localAddress = InetAddress.getByName(serverBind).getAddress();
        return
            address[0] == localAddress[0]
            && address[1] == localAddress[1]
            && address[2] == localAddress[2]
        ;
    }

	@Override
    public void run() {
        while (true) {
            try {
                InetAddress address=InetAddress.getByName(serverBind);
                synchronized(System.out) {
                    System.out.println("Accepting TCP connections on " + address.getHostAddress() + ':' + serverPort);
                }
                try (ServerSocket SS = new ServerSocket(serverPort, 50, address)) {
                    while (true) {
                        Socket socket = SS.accept();
                        incConnectionCount();
                        try {
                            socket.setKeepAlive(true);
                            socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
                            //socket.setTcpNoDelay(true);
                            new SocketServerThread(this, socket).start();
                        } catch(ThreadDeath TD) {
                            throw TD;
                        } catch(Throwable T) {
                            logger.log(Level.SEVERE, "serverPort=" + serverPort + ". address=" + address, T);
                        }
                    }
                }
            } catch (ThreadDeath TD) {
                throw TD;
            } catch (Throwable T) {
                logger.log(Level.SEVERE, "serverPort=" + serverPort + ", serverBind=" + serverBind, T);
            }
            try {
                Thread.sleep(15000);
            } catch (InterruptedException err) {
                logger.log(Level.WARNING, null, err);
            }
        }
    }
}