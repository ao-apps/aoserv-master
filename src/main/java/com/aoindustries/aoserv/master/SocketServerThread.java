/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.master.master.Process;
import com.aoindustries.aoserv.master.master.Process_Manager;
import com.aoindustries.collections.IntArrayList;
import com.aoindustries.collections.IntList;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.io.stream.StreamableInput;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.lang.Strings;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.InetAddress;
import com.aoindustries.security.Identifier;
import com.aoindustries.validation.ValidationException;
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

	private static final Logger logger = Logger.getLogger(SocketServerThread.class.getName());

	/**
	 * The <code>{@link TCPServer}</code> that created this <code>SocketServerThread</code>.
	 */
	private final TCPServer server;

	/**
	 * The <code>{@link Socket}</code> that is connected.
	 */
	private final Socket socket;

	/**
	 * The <code>{@link StreamableInput}</code> that is being read from.
	 */
	private final StreamableInput in;

	/**
	 * The <code>{@link StreamableOutput}</code> that is being written to.
	 */
	private final StreamableOutput out;

	/**
	 * The version of the protocol the client is running.
	 */
	private AoservProtocol.Version protocolVersion;

	/**
	 * The server if this is a connection from a daemon.
	 */

	/**
	 * The master process.
	 */
	private final Process process;

	private boolean isClosed = true;

	/**
	 * Creates a new, running <code>AOServServerThread</code>.
	 */
	public SocketServerThread(TCPServer server, Socket socket) throws IOException, SQLException {
		try {
			this.server = server;
			this.socket = socket;
			this.in = new StreamableInput(new BufferedInputStream(socket.getInputStream()));
			this.out = new StreamableOutput(new BufferedOutputStream(socket.getOutputStream()));
			InetAddress host = InetAddress.valueOf(socket.getInetAddress().getHostAddress());
			process = Process_Manager.createProcess(
				host,
				server.getProtocol(),
				server.isSecure()
			);
			isClosed = false;
		} catch(ValidationException e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}
	}

	private final LinkedList<InvalidateCacheEntry> invalidateLists=new LinkedList<>();

	/**
	 * Invalidates the listed tables.  Also, if this connector represents a daemon,
	 * this invalidate is registered with ServerHandler for invalidation synchronization.
	 *
	 * IDEA: Could reduce signals under high load by combining entries that are not synchronous.
	 *       Could even combine synchronous ones as long as all sync entries were acknowledged in the proper order.
	 */
	@Override
	public void cachesInvalidated(IntList tableList) throws IOException {
		if(tableList!=null && tableList.size()>0) {
			synchronized(this) { // Must use "this" lock because wait is performed on this object externally
				// Register with ServerHandler for invalidation synchronization
				int daemonServer=getDaemonServer();
				IntList copy=new IntArrayList(tableList);
				InvalidateCacheEntry ice=new InvalidateCacheEntry(
					copy,
					daemonServer,
					daemonServer==-1?null:NetHostHandler.addInvalidateSyncEntry(daemonServer, this)
				);
				invalidateLists.addLast(ice);
				notify();
			}
		}
	}

	@Override
	public int getDaemonServer() {
		return process.getDaemonServer();
	}

	@Override
	public InvalidateCacheEntry getNextInvalidatedTables() {
		synchronized(this) {
			if(invalidateLists.isEmpty()) return null;
			return invalidateLists.removeFirst();
		}
	}

	@Override
	public Identifier getConnectorId() {
		return process.getConnectorId();
	}

	@Override
	public AoservProtocol.Version getProtocolVersion() {
		return protocolVersion;
	}

	/**
	 * Logs a security message to <code>System.err</code>.
	 * Also sends email messages to <code>aoserv.server</code>.
	 */
	@Override
	public String getSecurityMessageHeader() {
		return "IP="+socket.getInetAddress().getHostAddress()+" EffUsr="+process.getEffectiveAdministrator_username()+" AuthUsr="+process.getAuthenticatedAdministrator_username();
	}

	@Override
	public com.aoindustries.aoserv.client.account.User.Name getCurrentAdministrator() {
		return process.getEffectiveAdministrator_username();
	}

	@Override
	public boolean isSecure() throws UnknownHostException {
		return server.isSecure();
	}

	@Override
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	public void run() {
		try {
			try {
				this.protocolVersion=AoservProtocol.Version.getVersion(in.readUTF());
				process.setAOServProtocol(protocolVersion.getVersion());
				if(in.readBoolean()) {
					DomainName daemonServer;
					try {
						daemonServer = DomainName.valueOf(in.readUTF());
					} catch(ValidationException e) {
						out.writeBoolean(false);
						out.writeUTF(e.getLocalizedMessage());
						out.flush();
						return;
					}
					process.setDeamonServer(
						NetHostHandler.getHostForLinuxServerHostname(
							MasterDatabase.getDatabase(),
							daemonServer
						)
					);
				} else {
					process.setDeamonServer(-1);
				}
				try {
					process.setEffectiveUser(com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF()));
					process.setAuthenticatedUser(com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF()));
				} catch(ValidationException e) {
					out.writeBoolean(false);
					out.writeUTF(e.getLocalizedMessage());
					out.flush();
					return;
				}
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
					+ process.getEffectiveAdministrator_username()
					+ " ("
					+ process.getAuthenticatedAdministrator_username()
					+ ")"
				);
				String password = in.readUTF();
				Identifier existingId;
				if(protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
					long existingIdLong = in.readLong();
					existingId = existingIdLong == -1 ? null : new Identifier(0, existingIdLong);
				} else {
					existingId = in.readNullIdentifier();
				}

				switch(protocolVersion) {
					case VERSION_1_83_2 :
					case VERSION_1_83_1 :
					case VERSION_1_83_0 :
					case VERSION_1_82_1 :
					case VERSION_1_82_0 :
					case VERSION_1_81_22 :
					case VERSION_1_81_21 :
					case VERSION_1_81_20 :
					case VERSION_1_81_19 :
					case VERSION_1_81_18 :
					case VERSION_1_81_17 :
					case VERSION_1_81_16 :
					case VERSION_1_81_15 :
					case VERSION_1_81_14 :
					case VERSION_1_81_13 :
					case VERSION_1_81_12 :
					case VERSION_1_81_11 :
					case VERSION_1_81_10 :
					case VERSION_1_81_9 :
					case VERSION_1_81_8 :
					case VERSION_1_81_7 :
					case VERSION_1_81_6 :
					case VERSION_1_81_5 :
					case VERSION_1_81_4 :
					case VERSION_1_81_3 :
					case VERSION_1_81_2 :
					case VERSION_1_81_1 :
					case VERSION_1_81_0 :
					case VERSION_1_80_2 :
					case VERSION_1_80_1 :
					case VERSION_1_80_0 :
					case VERSION_1_80 :
					case VERSION_1_79 :
					case VERSION_1_78 :
					case VERSION_1_77 :
					case VERSION_1_76 :
					case VERSION_1_75 :
					case VERSION_1_74 :
					case VERSION_1_73 :
					case VERSION_1_72 :
					case VERSION_1_71 :
					case VERSION_1_70 :
					case VERSION_1_69 :
					case VERSION_1_68 :
					case VERSION_1_67 :
					case VERSION_1_66 :
					case VERSION_1_65 :
					case VERSION_1_64 :
					case VERSION_1_63 :
					case VERSION_1_62 :
					case VERSION_1_61 :
					case VERSION_1_60 :
					case VERSION_1_59 :
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
						DatabaseAccess db = MasterDatabase.getDatabase();
						String message = MasterServer.authenticate(
							db,
							socket.getInetAddress().getHostAddress(),
							process.getEffectiveAdministrator_username(),
							process.getAuthenticatedAdministrator_username(),
							password
						);

						if(message!=null) {
							//UserHost.reportSecurityMessage(this, message, process.getEffectiveUser().length()>0 && password.length()>0);
							out.writeBoolean(false);
							out.writeUTF(message);
							out.flush();
						} else {
							// Only master users may provide a daemon_server
							boolean isOK=true;
							int daemonServer=process.getDaemonServer();
							if(daemonServer!=-1) {
								if(MasterServer.getUser(db, process.getEffectiveAdministrator_username())==null) {
									out.writeBoolean(false);
									out.writeUTF("Only master users may register a daemon server.");
									out.flush();
									isOK=false;
								} else {
									UserHost[] servers=MasterServer.getUserHosts(db, process.getEffectiveAdministrator_username());
									if(servers.length!=0) {
										isOK=false;
										for (UserHost server1 : servers) {
											if (server1.getServerPKey() == daemonServer) {
												isOK=true;
												break;
											}
										}
										if(!isOK) {
											out.writeBoolean(false);
											out.writeUTF("Master user ("+process.getEffectiveAdministrator_username()+") not allowed to access server: "+daemonServer);
											out.flush();
										}
									}
								}
							}
							if(isOK) {
								out.writeBoolean(true);
								if(existingId == null) {
									Identifier connectorId = MasterServer.getNextConnectorId(protocolVersion);
									process.setConnectorId(connectorId);
									if(protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
										assert connectorId.getHi() == 0;
										assert connectorId.getLo() != -1;
										out.writeLong(connectorId.getLo());
									} else {
										out.writeIdentifier(connectorId);
									}
								} else {
									process.setConnectorId(existingId);
								}
								// Command sequence starts at a random value
								final long startSeq;
								if(protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
									startSeq = MasterServer.getSecureRandom().nextLong();
									out.writeLong(startSeq);
								} else {
									startSeq = 0;
								}
								out.flush();

								MasterServer.updateAOServProtocolLastUsed(db, protocolVersion);

								long seq = startSeq;
								while(server.handleRequest(this, seq++, in, out, process)) {
									// Do nothing in loop
								}
							}
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
							+Strings.join(AoservProtocol.Version.values(), ", ")
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
					message == null
					|| (
						!message.equals("Remote host closed connection during handshake")
						&& !message.equals("no cipher suites in common")
					)
				) {
					logger.log(Level.SEVERE, null, err);
				} else {
					logger.log(Level.FINE, null, err);
				}
			} catch(SSLException err) {
				String message = err.getMessage();
				if(
					message == null
					|| (
						!message.equals("Connection has been shutdown: javax.net.ssl.SSLException: java.net.SocketException: Connection reset")
						&& !message.equals("Unrecognized SSL message, plaintext connection?")
					)
				) {
					logger.log(Level.SEVERE, null, err);
				} else {
					logger.log(Level.FINE, null, err);
				}
			} catch(SocketException err) {
				String message = err.getMessage();
				if(
					message == null
					|| (
						// Connection reset common for abnormal client disconnects
						!message.startsWith("Broken pipe")
						&& !message.equals("Connection reset")
						&& !message.startsWith("Connection timed out")
					)
				) {
					logger.log(Level.SEVERE, null, err);
				} else {
					logger.log(Level.FINE, null, err);
				}
			} catch(IOException err) {
				String message = err.getMessage();
				if(
					message == null
					// Broken pipe common for abnormal client disconnects
					|| !message.startsWith("Broken pipe")
				) {
					logger.log(Level.SEVERE, null, err);
				} else {
					logger.log(Level.FINE, null, err);
				}
			} catch(ThreadDeath td) {
				throw td;
			} catch(Throwable t) {
				logger.log(Level.SEVERE, null, t);
			} finally {
				// Close the socket
				try {
					isClosed = true;
					socket.close();
				} catch (IOException err) {
					logger.log(Level.SEVERE, null, err);
				}
			}
		} finally {
			Process_Manager.removeProcess(process);
		}
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}
}
