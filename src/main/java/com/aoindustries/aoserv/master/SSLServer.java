/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2000-2013, 2018, 2019, 2020, 2021  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.hodgepodge.io.AOPool;
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

	private static final Logger logger = Logger.getLogger(NetHostHandler.class.getName());

	/**
	 * The protocol of this server.
	 */
	static final String PROTOCOL_SSL = "ssl";

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
		while (!Thread.currentThread().isInterrupted()) {
			try {
				InetAddress address = InetAddress.getByName(serverBind);
				synchronized(System.out) {
					System.out.println("Accepting SSL connections on " + address.getHostAddress() + ':' + serverPort);
				}
				try (SSLServerSocket SS = (SSLServerSocket)factory.createServerSocket(serverPort, 50, address)) {
					while (!Thread.currentThread().isInterrupted()) {
						Socket socket = SS.accept();
						incConnectionCount();
						try {
							socket.setKeepAlive(true);
							socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
							//socket.setTcpNoDelay(true);
							new SocketServerThread(this, socket).start();
						} catch(ThreadDeath td) {
							throw td;
						} catch(Throwable t) {
							logger.log(Level.SEVERE, "serverPort=" + serverPort + ", address=" + address, t);
						}
					}
				}
			} catch (ThreadDeath td) {
				throw td;
			} catch (Throwable t) {
				logger.log(Level.SEVERE, null, t);
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
