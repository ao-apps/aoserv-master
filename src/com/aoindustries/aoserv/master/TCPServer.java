package com.aoindustries.aoserv.master;

/*
 * Copyright 2000-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.AOPool;
import java.net.*;

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
        (thread=new Thread(this, getClass().getName()+"?address="+serverBind+"&port="+serverPort)).start();
    }
    
    public String getProtocol() {
        return PROTOCOL;
    }

    /**
     * Determines if communication on this server is secure.
     */
    public boolean isSecure() throws UnknownHostException {
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
    }

    public void run() {
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
                Thread.sleep(15000);
            } catch (InterruptedException err) {
                reportWarning(err, null);
            }
        }
    }
}