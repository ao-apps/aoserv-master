/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2000-2013, 2018, 2020, 2021, 2022  AO Industries, Inc.
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

	private static final Logger logger = Logger.getLogger(TCPServer.class.getName());

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
		while (!Thread.currentThread().isInterrupted()) {
			try {
				InetAddress address=InetAddress.getByName(serverBind);
				synchronized(System.out) {
					System.out.println("Accepting TCP connections on " + address.getHostAddress() + ':' + serverPort);
				}
				try (ServerSocket SS = new ServerSocket(serverPort, 50, address)) {
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
							logger.log(Level.SEVERE, "serverPort=" + serverPort + ". address=" + address, t);
						}
					}
				}
			} catch (ThreadDeath td) {
				throw td;
			} catch (Throwable t) {
				logger.log(Level.SEVERE, "serverPort=" + serverPort + ", serverBind=" + serverBind, t);
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
