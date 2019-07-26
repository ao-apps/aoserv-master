/*
 * Copyright 2000-2013, 2014, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServWritable;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.account.Profile;
import com.aoindustries.aoserv.client.aosh.Command;
import com.aoindustries.aoserv.client.billing.Currency;
import com.aoindustries.aoserv.client.billing.MoneyUtil;
import com.aoindustries.aoserv.client.billing.Transaction;
import com.aoindustries.aoserv.client.billing.TransactionSearchCriteria;
import com.aoindustries.aoserv.client.dns.Record;
import com.aoindustries.aoserv.client.dns.ZoneTable;
import com.aoindustries.aoserv.client.email.InboxAttributes;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.linux.Server;
import com.aoindustries.aoserv.client.linux.User.Gecos;
import com.aoindustries.aoserv.client.master.Process;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.mysql.Database;
import com.aoindustries.aoserv.client.mysql.Table_Name;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.aoserv.client.net.reputation.Set.AddReputation;
import com.aoindustries.aoserv.client.net.reputation.Set.ConfidenceType;
import com.aoindustries.aoserv.client.net.reputation.Set.ReputationType;
import com.aoindustries.aoserv.client.pki.Certificate;
import com.aoindustries.aoserv.client.pki.HashedPassword;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.schema.Type;
import com.aoindustries.aoserv.client.ticket.Language;
import com.aoindustries.aoserv.client.web.Location;
import com.aoindustries.aoserv.client.web.tomcat.Context;
import com.aoindustries.aoserv.master.billing.WhoisHistoryService;
import com.aoindustries.aoserv.master.dns.DnsService;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.dbc.NoRowException;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.Email;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.net.Protocol;
import com.aoindustries.security.SmallIdentifier;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.IntArrayList;
import com.aoindustries.util.IntList;
import com.aoindustries.util.PolymorphicMultimap;
import com.aoindustries.util.SortedArrayList;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.Tuple2;
import com.aoindustries.util.i18n.Money;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>AOServServer</code> accepts connections from an <code>SimpleAOClient</code>.
 * Once the connection is accepted and authenticated, the server carries out all actions requested
 * by the client while providing the necessary security checks and data filters.
 * <p>
 * This server is completely threaded to handle multiple, simultaneous clients.
 * </p>
 * @author  AO Industries, Inc.
 */
public abstract class MasterServer {

	private static final Logger logger = LogFactory.getLogger(MasterServer.class);

	private static final int SERVICE_RETRY_INTERVAL = 60 * 1000; // One minute

	/**
	 * An unbounded executor for master-wide tasks.
	 */
	public final static ExecutorService executorService = Executors.newCachedThreadPool();

	/**
	 * The database values are read the first time this data is needed.
	 */
	private static final Object masterUsersLock = new Object();
	private static Map<com.aoindustries.aoserv.client.account.User.Name,User> masterUsers;
	private static final Object masterHostsLock = new Object();
	private static Map<com.aoindustries.aoserv.client.account.User.Name,List<HostAddress>> masterHosts;
	private static final Object masterServersLock = new Object();
	private static Map<com.aoindustries.aoserv.client.account.User.Name,UserHost[]> masterServers;

	/**
	 * The time the system started up
	 */
	private static final long START_TIME = System.currentTimeMillis();

	/**
	 * The central list of all objects that are notified of
	 * cache updates.
	 */
	private static final List<RequestSource> cacheListeners=new ArrayList<>();

	/**
	 * The address that this server will bind to.
	 */
	protected final String serverBind;

	/**
	 * The port that this server will listen on.
	 */
	protected final int serverPort;

	private static final AtomicInteger concurrency = new AtomicInteger();

	private static final AtomicInteger maxConcurrency = new AtomicInteger();

	private static final AtomicLong requestCount = new AtomicLong();

	private static final AtomicLong totalTime = new AtomicLong();

	/**
	 * Creates a new, running <code>AOServServer</code>.
	 */
	protected MasterServer(String serverBind, int serverPort) {
		this.serverBind = serverBind;
		this.serverPort = serverPort;
	}

	private static void addCacheListener(RequestSource source) {
		synchronized(cacheListeners) {
			cacheListeners.add(source);
		}
	}

	/*
	private static void appendParam(String S, StringBuilder SB) {
		if(S==null) SB.append("null");
		else {
			int len=S.length();
			// Figure out to use quotes or not
			boolean useQuotes=false;
			for(int c=0;c<len;c++) {
				char ch=S.charAt(c);
				if(ch<=' ' || ch=='\'') {
					useQuotes=true;
					break;
				}
			}
			if(useQuotes) SB.append('\'');
			for(int c=0;c<len;c++) {
				char ch=S.charAt(c);
				if(ch=='\'') SB.append('\\');
				SB.append(ch);
			}
			if(useQuotes) SB.append('\'');
		}
	}*/

	/**
	 * Gets the interface address this server is listening on.
	 */
	final public String getBindAddress() {
		return serverBind;
	}

	// TODO: Make be Identifier for full 128-bit
	public static SmallIdentifier getNextConnectorID() {
		while(true) {
			SmallIdentifier id = new SmallIdentifier();
			// Avoid the small chance of conflicting with -1 send to indicate that no id yet assigned
			if(id.getValue() != -1) {
				boolean found = false;
				synchronized(cacheListeners) {
					// TODO: Keep a map of cacheListeners to avoid this sequential scan?
					for(RequestSource source : cacheListeners) {
						if(source.getConnectorID().equals(id)) {
							found = true;
							break;
						}
					}
				}
				if(!found) return id;
			}
		}
	}

	/**
	 * Gets the interface port this server is listening on.
	 */
	final public int getPort() {
		return serverPort;
	}

	abstract public String getProtocol();

	private static final Random random = new SecureRandom();
	public static Random getRandom() {
		return random;
	}

	public static int getRequestConcurrency() {
		return concurrency.get();
	}

	private static final Object connectionsLock=new Object();
	private static long connections=0;
	protected static void incConnectionCount() {
		synchronized(connectionsLock) {
			connections++;
		}
	}
	public static long getRequestConnections() {
		synchronized(connectionsLock) {
			return connections;
		}
	}

	public static int getRequestMaxConcurrency() {
		return maxConcurrency.get();
	}

	public static long getRequestTotalTime() {
		return totalTime.get();
	}

	public static long getRequestTransactions() {
		return requestCount.get();
	}

	public static long getStartTime() {
		return START_TIME;
	}

	/** Used to avoid cloning of array for each access. */
	private static final AoservProtocol.CommandID[] commandIDs = AoservProtocol.CommandID.values();

	/** Copy used to avoid copying for each access. */
	private static final Table.TableID[] tableIDs = Table.TableID.values();

	// TODO: Make this an interface to leverage lambdas
	static abstract class Response {

		abstract void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException;

		static final Response DONE = Response.of(AoservProtocol.DONE);

		static Response of(int resp1) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
				}
			};
		}

		static Response of(int resp1, int resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeCompressedInt(resp2);
				}
			};
		}

		static Response of(int resp1, long resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLong(resp2);
				}
			};
		}

		static Response of(int resp1, boolean resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeBoolean(resp2);
				}
			};
		}

		static Response of(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
				}
			};
		}

		static Response of(int resp1, String resp2, String resp3) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
					out.writeUTF(resp3);
				}
			};
		}

		static Response of(int resp1, Account.Name resp2) {
			return of(resp1, resp2.toString());
		}

		static Response of(int resp1, Database.Name resp2) {
			return of(resp1, resp2.toString());
		}

		static Response of(int resp1, com.aoindustries.aoserv.client.postgresql.Database.Name resp2) {
			return of(resp1, resp2.toString());
		}

		static Response of(int resp1, long resp2, String resp3) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLong(resp2);
					out.writeUTF(resp3);
				}
			};
		}

		static Response of(int resp1, long[] resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					for(int c=0;c<resp2.length;c++) out.writeLong(resp2[c]);
				}
			};
		}

		static Response of(int resp1, InboxAttributes resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeBoolean(resp2!=null);
					if(resp2!=null) resp2.write(out, protocolVersion);
				}
			};
		}

		static Response of(int resp1, String resp2, String resp3, String resp4) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
					out.writeUTF(resp3);
					out.writeUTF(resp4);
				}
			};
		}

		static Response ofNullLongString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeNullLongUTF(resp2);
				}
			};
		}

		static Response ofLongString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLongUTF(resp2);
				}
			};
		}

		static Response of(
			int resp1,
			String resp2,
			HostAddress resp3,
			int resp4,
			long resp5
		) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
					out.writeUTF(resp3.toString());
					out.writeCompressedInt(resp4);
					out.writeLong(resp5);
				}
			};
		}

		static Response ofNullString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AoservProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeNullUTF(resp2);
				}
			};
		}
	}

	/**
	 * Handles a single request and then returns.  Exceptions during command processing
	 * are caught, logged, and possibly sends a message to the client.  Other exceptions
	 * before or after the command will be thrown from here.
	 *
	 * @return  <code>true</code> if another request could be made on this stream, or
	 *          <code>false</code> if this connection should be closed.
	 */
	final boolean handleRequest(
		RequestSource source,
		long seq,
		CompressedDataInputStream in,
		CompressedDataOutputStream out,
		Process process
	) throws IOException, SQLException {
		// Time is not added for the cache invalidation connection
		boolean addTime=true;

		// The return value
		boolean keepOpen = true;

		process.commandCompleted();

		// Verify client sends matching sequence
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
			long clientSeq = in.readLong();
			if(clientSeq != seq) throw new IOException("Sequence mismatch: " + clientSeq + " != " + seq);
		}
		// Send command sequence
		if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) >= 0) {
			out.writeLong(seq); // out is buffered, so no I/O created by writing this early
		}
		// Continue with task
		int taskCodeOrdinal = in.readCompressedInt();
		process.commandRunning();
		{
			int conc=concurrency.incrementAndGet();
			while(true) {
				int maxConc = maxConcurrency.get();
				if(maxConc>=conc) break;
				if(maxConcurrency.compareAndSet(maxConc, conc)) break;
			}
		}
		requestCount.incrementAndGet();
		long requestStartTime=System.currentTimeMillis();
		try {
			if(taskCodeOrdinal==-1) {
				// EOF
				process.setCommand("quit");
				addTime=false;
				concurrency.decrementAndGet();
				return false;
			} else {
				final boolean done;
				AoservProtocol.CommandID taskCode=commandIDs[taskCodeOrdinal];
				switch(taskCode) {
					case LISTEN_CACHES :
						process.setCommand("listen_caches");
						addTime=false;
						concurrency.decrementAndGet();
						// This method normally never leaves for this command
						try {
							addCacheListener(source);
							final DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
							try {
								final AoservProtocol.Version protocolVersion = source.getProtocolVersion();
								final com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
								boolean didInitialInvalidateAll = false;
								while(!AccountHandler.isAdministratorDisabled(conn, currentAdministrator)) {
									InvalidateCacheEntry ice;
									if(!didInitialInvalidateAll) {
										// Invalidate all once immediately now that listener added, just in case signals were lost during a network outage and reconnect
										IntList clientInvalidateList = new IntArrayList();
										for(Table.TableID tableID : Table.TableID.values()) {
											int clientTableID = TableHandler.convertToClientTableID(conn, source, tableID);
											if(clientTableID != -1) clientInvalidateList.add(clientTableID);
										}
										conn.releaseConnection();
										ice = new InvalidateCacheEntry(clientInvalidateList, -1, null);
									} else {
										conn.releaseConnection();
										process.commandSleeping();
										long endTime=System.currentTimeMillis()+60000;
										synchronized(source) {
											while((ice=source.getNextInvalidatedTables())==null) {
												long delay=endTime-System.currentTimeMillis();
												if(delay<=0 || delay>60000) break;
												try {
													source.wait(delay);
												} catch(InterruptedException err) {
													logger.log(Level.WARNING, null, err);
													break;
												}
											}
										}
									}
									if(ice!=null) {
										if(didInitialInvalidateAll) process.commandRunning();
										IntList clientTableIDs=ice.getInvalidateList();
										int size=clientTableIDs.size();
										if(protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_47)>=0) out.writeBoolean(ice.getCacheSyncID()!=null);
										out.writeCompressedInt(size);
										for(int c=0;c<size;c++) out.writeCompressedInt(clientTableIDs.getInt(c));
									} else {
										if(protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_47)>=0) out.writeBoolean(true);
										out.writeCompressedInt(-1);
									}
									out.flush();

									if(ice!=null) {
										int host = ice.getHost();
										Long id=ice.getCacheSyncID();
										if(
											id!=null
											|| protocolVersion.compareTo(AoservProtocol.Version.VERSION_1_47)<0 // Before version 1.47 was always synchronous
										) {
											if(!in.readBoolean()) throw new IOException("Unexpected invalidate sync response.");
										}
										if(host!=-1 && id!=null) NetHostHandler.removeInvalidateSyncEntry(host, id);
									} else {
										if(!in.readBoolean()) throw new IOException("Unexpected invalidate sync response.");
									}
									didInitialInvalidateAll = true;
								}
							} finally {
								conn.releaseConnection();
							}
						} finally {
							removeCacheListener(source);
						}
						return false;
					case PING :
						process.setCommand(Command.PING);
						out.writeByte(AoservProtocol.DONE);
						done=true;
						break;
					case QUIT :
						process.setCommand("quit");
						addTime=false;
						concurrency.decrementAndGet();
						return false;
					case TEST_CONNECTION :
						process.setCommand("test_connection");
						out.writeByte(AoservProtocol.DONE);
						done=true;
						break;
					default :
						done=false;
				}
				if(!done) {
					// These commands automatically have the try/catch and the database connection releasing
					// And a finally block to reset thread priority
					boolean logIOException = true;
					boolean logSQLException = true;
					Thread currentThread=Thread.currentThread();
					try {
						InvalidateList invalidateList=new InvalidateList();
						IntArrayList clientInvalidateList=null;

						final Response resp;
						final boolean sendInvalidateList;

						final DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
						try {
							boolean connRolledBack=false;
							try {
								// Stop processing if the account is disabled
								if(AccountHandler.isAdministratorDisabled(conn, source.getCurrentAdministrator())) throw new IOException("Administrator disabled: "+source.getCurrentAdministrator());

								switch(taskCode) {
									case INVALIDATE_TABLE :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
											int host;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
												DomainName hostname = DomainName.valueOf(in.readNullUTF());
												host = hostname==null ? -1 : NetHostHandler.getHostForLinuxServerHostname(conn, hostname);
											} else {
												host = in.readCompressedInt();
											}
											process.setCommand(
												Command.INVALIDATE,
												TableHandler.getTableName(
													conn,
													tableID
												),
												host==-1 ? null : host
											);
											TableHandler.invalidate(
												conn,
												source,
												invalidateList,
												tableID,
												host
											);
											resp = Response.DONE;
											sendInvalidateList=true;
										}
										break;
									case ADD : {
										int clientTableID = in.readCompressedInt();
										Table.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
										if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
										switch(tableID) {
											case BUSINESS_ADMINISTRATORS :
												{
													com.aoindustries.aoserv.client.account.User.Name user = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													String name=in.readUTF().trim();
													String title=in.readNullUTF();
													long birthdayLong=in.readLong();
													Date birthday = birthdayLong==-1 ? null : new Date(birthdayLong);
													boolean isPrivate=in.readBoolean();
													String workPhone=in.readUTF().trim();
													String homePhone=in.readNullUTF();
													String cellPhone=in.readNullUTF();
													String fax=in.readNullUTF();
													String email=in.readUTF().trim();
													String address1=in.readNullUTF();
													String address2=in.readNullUTF();
													String city=in.readNullUTF();
													String state=in.readNullUTF();
													String country=in.readNullUTF();
													String zip=in.readNullUTF();
													boolean enableEmailSupport=
														source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_44)>=0
														? in.readBoolean()
														: false
													;
													process.setCommand(
														Command.ADD_BUSINESS_ADMINISTRATOR,
														user,
														name,
														title,
														birthday,
														isPrivate,
														workPhone,
														homePhone,
														cellPhone,
														fax,
														email,
														address1,
														address2,
														city,
														state,
														country,
														zip,
														enableEmailSupport
													);
													AccountHandler.addAdministrator(
														conn,
														source,
														invalidateList,
														user,
														name,
														title,
														birthday,
														isPrivate,
														workPhone,
														homePhone,
														cellPhone,
														fax,
														email,
														address1,
														address2,
														city,
														state,
														country,
														zip,
														enableEmailSupport
													);
													resp = Response.DONE;
												}
												break;
											case BUSINESS_PROFILES :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String name=in.readUTF().trim();
													boolean isPrivate=in.readBoolean();
													String phone=in.readUTF().trim();
													String fax=in.readNullUTF();
													String address1=in.readUTF().trim();
													String address2=in.readNullUTF();
													String city=in.readUTF().trim();
													String state=in.readNullUTF();
													String country=in.readUTF();
													String zip=in.readNullUTF();
													boolean sendInvoice=in.readBoolean();
													String billingContact=in.readUTF().trim();
													Set<Email> billingEmail;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_22) >= 0) {
														int size = in.readCompressedInt();
														billingEmail = new LinkedHashSet<>(size*4/3+1);
														for(int i = 0; i < size; i++) {
															billingEmail.add(Email.valueOf(in.readUTF()));
														}
													} else {
														billingEmail = Profile.splitEmails(in.readUTF().trim());
													}
													Profile.EmailFormat billingEmailFormat;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_20) >= 0) {
														billingEmailFormat = in.readEnum(Profile.EmailFormat.class);
													} else {
														billingEmailFormat = Profile.EmailFormat.HTML;
													}
													String technicalContact=in.readUTF().trim();
													Set<Email> technicalEmail;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_22) >= 0) {
														int size = in.readCompressedInt();
														technicalEmail = new LinkedHashSet<>(size*4/3+1);
														for(int i = 0; i < size; i++) {
															technicalEmail.add(Email.valueOf(in.readUTF()));
														}
													} else {
														technicalEmail = Profile.splitEmails(in.readUTF().trim());
													}
													Profile.EmailFormat technicalEmailFormat;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_20) >= 0) {
														technicalEmailFormat = in.readEnum(Profile.EmailFormat.class);
													} else {
														technicalEmailFormat = Profile.EmailFormat.HTML;
													}
													process.setCommand(
														Command.ADD_BUSINESS_PROFILE,
														account,
														name,
														isPrivate,
														phone,
														fax,
														address1,
														address2,
														city,
														state,
														country,
														zip,
														sendInvoice,
														billingContact,
														billingEmail,
														billingEmailFormat,
														technicalContact,
														technicalEmail,
														technicalEmailFormat
													);
													resp = Response.of(
														AoservProtocol.DONE,
														AccountHandler.addProfile(
															conn,
															source,
															invalidateList,
															account,
															name,
															isPrivate,
															phone,
															fax,
															address1,
															address2,
															city,
															state,
															country,
															zip,
															sendInvoice,
															billingContact,
															billingEmail,
															billingEmailFormat,
															technicalContact,
															technicalEmail,
															technicalEmailFormat
														)
													);
												}
												break;
											case BUSINESS_SERVERS :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													int host = in.readCompressedInt();
													if(
														source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_102)>=0
														&& source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0
													) {
														boolean can_configure_backup=in.readBoolean();
													}
													process.setCommand(
														Command.ADD_BUSINESS_SERVER,
														account,
														host
													);
													resp = Response.of(
														AoservProtocol.DONE,
														AccountHandler.addAccountHost(
															conn,
															source,
															invalidateList,
															account,
															host
														)
													);
												}
												break;
											case BUSINESSES :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String contractVersion=in.readNullUTF();
													int defaultServer;
													DomainName hostname;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
														defaultServer = -1;
														hostname = DomainName.valueOf(in.readUTF());
													} else {
														defaultServer = in.readCompressedInt();
														hostname = null;
													}
													Account.Name parent = Account.Name.valueOf(in.readUTF());
													boolean can_add_backup_servers=
														source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_102)>=0
														?in.readBoolean()
														:false
													;
													boolean can_add_businesses=in.readBoolean();
													boolean can_see_prices=
														source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_103)>=0
														?in.readBoolean()
														:true
													;
													boolean billParent=in.readBoolean();
													// Convert old hostname to net.Host.id
													if(defaultServer==-1) {
														defaultServer = NetHostHandler.getHostForLinuxServerHostname(conn, hostname);
													}
													process.setCommand(
														Command.ADD_BUSINESS,
														account,
														contractVersion,
														defaultServer,
														parent,
														can_add_backup_servers,
														can_add_businesses,
														can_see_prices,
														billParent
													);
													AccountHandler.addAccount(
														conn,
														source,
														invalidateList,
														account,
														contractVersion,
														defaultServer,
														parent,
														can_add_backup_servers,
														can_add_businesses,
														can_see_prices,
														billParent
													);
													resp = Response.DONE;
												}
												break;
											case CREDIT_CARDS :
												{
													// If before version 1.29, do not support add call but read the old values anyway
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_28)<=0) {
														String accounting=in.readUTF();
														byte[] cardNumber=new byte[in.readCompressedInt()]; in.readFully(cardNumber);
														String cardInfo=in.readUTF().trim();
														byte[] expirationMonth=new byte[in.readCompressedInt()]; in.readFully(expirationMonth);
														byte[] expirationYear=new byte[in.readCompressedInt()]; in.readFully(expirationYear);
														byte[] cardholderName=new byte[in.readCompressedInt()]; in.readFully(cardholderName);
														byte[] streetAddress=new byte[in.readCompressedInt()]; in.readFully(streetAddress);
														byte[] city=new byte[in.readCompressedInt()]; in.readFully(city);
														int len=in.readCompressedInt(); byte[] state=len>=0?new byte[len]:null; if(len>=0) in.readFully(state);
														len=in.readCompressedInt(); byte[] zip=len>=0?new byte[len]:null; if(len>=0) in.readFully(zip);
														boolean useMonthly=in.readBoolean();
														String description=in.readNullUTF();
														throw new SQLException("add_credit_card for protocol version "+AoservProtocol.Version.VERSION_1_28+" or older is no longer supported.");
													}
													String processorName = in.readUTF();
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String groupName = in.readNullUTF();
													String cardInfo = in.readUTF().trim();
													Byte expirationMonth;
													Short expirationYear;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
														expirationMonth = in.readByte();
														expirationYear = in.readShort();
													} else {
														expirationMonth = null;
														expirationYear = null;
													}
													String providerUniqueId = in.readUTF();
													String firstName = in.readUTF().trim();
													String lastName = in.readUTF().trim();
													String companyName = in.readNullUTF();
													String email = in.readNullUTF();
													String phone = in.readNullUTF();
													String fax = in.readNullUTF();
													String customerId;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_1) >= 0) {
														customerId = in.readNullUTF();
													} else {
														customerId = null;
													}
													String customerTaxId = in.readNullUTF();
													String streetAddress1 = in.readUTF();
													String streetAddress2 = in.readNullUTF();
													String city = in.readUTF();
													String state = in.readNullUTF();
													String postalCode = in.readNullUTF();
													String countryCode = in.readUTF();
													String principalName = in.readNullUTF();
													String description = in.readNullUTF();
													String encryptedCardNumber;
													int encryptionFrom;
													int encryptionRecipient;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
														encryptedCardNumber = null;
														encryptionFrom = -1;
														encryptionRecipient = -1;
													} else {
														encryptedCardNumber = in.readNullUTF();
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) < 0) {
															String encryptedExpiration = in.readNullUTF();
														}
														encryptionFrom = in.readCompressedInt();
														encryptionRecipient = in.readCompressedInt();
													}

													process.setCommand(
														"add_credit_card",
														processorName,
														account,
														groupName,
														cardInfo,
														expirationMonth==null ? null : AoservProtocol.FILTERED,
														expirationYear==null ? null : AoservProtocol.FILTERED,
														providerUniqueId,
														firstName,
														lastName,
														companyName,
														email,
														phone,
														fax,
														customerId,
														customerTaxId,
														streetAddress1,
														streetAddress2,
														city,
														state,
														postalCode,
														countryCode,
														principalName,
														description,
														encryptedCardNumber==null ? null : AoservProtocol.FILTERED,
														encryptionFrom==-1 ? null : encryptionFrom,
														encryptionRecipient==-1 ? null : encryptionRecipient
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PaymentHandler.addCreditCard(
															conn,
															source,
															invalidateList,
															processorName,
															account,
															groupName,
															cardInfo,
															expirationMonth,
															expirationYear,
															providerUniqueId,
															firstName,
															lastName,
															companyName,
															email,
															phone,
															fax,
															customerId,
															customerTaxId,
															streetAddress1,
															streetAddress2,
															city,
															state,
															postalCode,
															countryCode,
															principalName,
															description,
															encryptedCardNumber,
															encryptionFrom,
															encryptionRecipient
														)
													);
												}
												break;
											case CREDIT_CARD_TRANSACTIONS :
												{
													String processor = in.readUTF();
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String groupName = in.readNullUTF();
													boolean testMode = in.readBoolean();
													int duplicateWindow = in.readCompressedInt();
													String orderNumber = in.readNullUTF();
													java.util.Currency currency = java.util.Currency.getInstance(in.readUTF());
													Money amount;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														amount = new Money(currency, new BigDecimal(in.readUTF()));
													} else {
														amount = new Money(currency, in.readLong(), in.readCompressedInt());
													}
													Money taxAmount;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														String value = in.readNullUTF();
														taxAmount = value == null ? null : new Money(currency, new BigDecimal(value));
													} else {
														if(in.readBoolean()) {
															taxAmount = new Money(currency, in.readLong(), in.readCompressedInt());
														} else {
															taxAmount = null;
														}
													}
													boolean taxExempt = in.readBoolean();
													Money shippingAmount;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														String value = in.readNullUTF();
														shippingAmount = value == null ? null : new Money(currency, new BigDecimal(value));
													} else {
														if(in.readBoolean()) {
															shippingAmount = new Money(currency, in.readLong(), in.readCompressedInt());
														} else {
															shippingAmount = null;
														}
													}
													Money dutyAmount;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														String value = in.readNullUTF();
														dutyAmount = value == null ? null : new Money(currency, new BigDecimal(value));
													} else {
														if(in.readBoolean()) {
															dutyAmount = new Money(currency, in.readLong(), in.readCompressedInt());
														} else {
															dutyAmount = null;
														}
													}
													String shippingFirstName = in.readNullUTF();
													String shippingLastName = in.readNullUTF();
													String shippingCompanyName = in.readNullUTF();
													String shippingStreetAddress1 = in.readNullUTF();
													String shippingStreetAddress2 = in.readNullUTF();
													String shippingCity = in.readNullUTF();
													String shippingState = in.readNullUTF();
													String shippingPostalCode = in.readNullUTF();
													String shippingCountryCode = in.readNullUTF();
													boolean emailCustomer = in.readBoolean();
													String merchantEmail = in.readNullUTF();
													String invoiceNumber = in.readNullUTF();
													String purchaseOrderNumber = in.readNullUTF();
													String description = in.readNullUTF();
													com.aoindustries.aoserv.client.account.User.Name creditCardCreatedBy = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													String creditCardPrincipalName = in.readNullUTF();
													Account.Name creditCardAccounting = Account.Name.valueOf(in.readUTF());
													String creditCardGroupName = in.readNullUTF();
													String creditCardProviderUniqueId = in.readNullUTF();
													String creditCardMaskedCardNumber = in.readUTF();
													Byte creditCard_expirationMonth;
													Short creditCard_expirationYear;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
														creditCard_expirationMonth = in.readNullByte();
														creditCard_expirationYear = in.readNullShort();
													} else {
														creditCard_expirationMonth = null;
														creditCard_expirationYear = null;
													}
													String creditCardFirstName = in.readUTF();
													String creditCardLastName = in.readUTF();
													String creditCardCompanyName = in.readNullUTF();
													String creditCardEmail = in.readNullUTF();
													String creditCardPhone = in.readNullUTF();
													String creditCardFax = in.readNullUTF();
													String creditCardCustomerId;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_1) >= 0) {
														creditCardCustomerId = in.readNullUTF();
													} else {
														creditCardCustomerId = null;
													}
													String creditCardCustomerTaxId = in.readNullUTF();
													String creditCardStreetAddress1 = in.readUTF();
													String creditCardStreetAddress2 = in.readNullUTF();
													String creditCardCity = in.readUTF();
													String creditCardState = in.readNullUTF();
													String creditCardPostalCode = in.readNullUTF();
													String creditCardCountryCode = in.readUTF();
													String creditCardComments = in.readNullUTF();
													long authorizationTime = in.readLong();
													String authorizationPrincipalName = in.readNullUTF();

													process.setCommand(
														"add_credit_card_transaction",
														processor,
														account,
														groupName,
														testMode,
														duplicateWindow,
														orderNumber,
														amount,
														taxAmount,
														taxExempt,
														shippingAmount,
														dutyAmount,
														shippingFirstName,
														shippingLastName,
														shippingCompanyName,
														shippingStreetAddress1,
														shippingStreetAddress2,
														shippingCity,
														shippingState,
														shippingPostalCode,
														shippingCountryCode,
														emailCustomer,
														merchantEmail,
														invoiceNumber,
														purchaseOrderNumber,
														description,
														creditCardCreatedBy,
														creditCardPrincipalName,
														creditCardAccounting,
														creditCardGroupName,
														creditCardProviderUniqueId,
														creditCardMaskedCardNumber,
														creditCard_expirationMonth==null ? null : AoservProtocol.FILTERED,
														creditCard_expirationYear==null ? null : AoservProtocol.FILTERED,
														creditCardFirstName,
														creditCardLastName,
														creditCardCompanyName,
														creditCardEmail,
														creditCardPhone,
														creditCardFax,
														creditCardCustomerId,
														creditCardCustomerTaxId,
														creditCardStreetAddress1,
														creditCardStreetAddress2,
														creditCardCity,
														creditCardState,
														creditCardPostalCode,
														creditCardCountryCode,
														creditCardComments,
														new java.util.Date(authorizationTime),
														authorizationPrincipalName
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PaymentHandler.addPayment(
															conn,
															source,
															invalidateList,
															processor,
															account,
															groupName,
															testMode,
															duplicateWindow,
															orderNumber,
															amount,
															taxAmount,
															taxExempt,
															shippingAmount,
															dutyAmount,
															shippingFirstName,
															shippingLastName,
															shippingCompanyName,
															shippingStreetAddress1,
															shippingStreetAddress2,
															shippingCity,
															shippingState,
															shippingPostalCode,
															shippingCountryCode,
															emailCustomer,
															merchantEmail,
															invoiceNumber,
															purchaseOrderNumber,
															description,
															creditCardCreatedBy,
															creditCardPrincipalName,
															creditCardAccounting,
															creditCardGroupName,
															creditCardProviderUniqueId,
															creditCardMaskedCardNumber,
															creditCard_expirationMonth,
															creditCard_expirationYear,
															creditCardFirstName,
															creditCardLastName,
															creditCardCompanyName,
															creditCardEmail,
															creditCardPhone,
															creditCardFax,
															creditCardCustomerId,
															creditCardCustomerTaxId,
															creditCardStreetAddress1,
															creditCardStreetAddress2,
															creditCardCity,
															creditCardState,
															creditCardPostalCode,
															creditCardCountryCode,
															creditCardComments,
															authorizationTime,
															authorizationPrincipalName
														)
													);
												}
												break;
											case CVS_REPOSITORIES :
												{
													int linuxServer = in.readCompressedInt();
													PosixPath path = PosixPath.valueOf(in.readUTF());
													int lsa = in.readCompressedInt();
													int lsg = in.readCompressedInt();
													long mode = in.readLong();
													process.setCommand(
														Command.ADD_CVS_REPOSITORY,
														linuxServer,
														path,
														lsa,
														lsg,
														Long.toOctalString(mode)
													);
													resp = Response.of(
														AoservProtocol.DONE,
														CvsHandler.addCvsRepository(
															conn,
															source,
															invalidateList,
															linuxServer,
															path,
															lsa,
															lsg,
															mode
														)
													);
												}
												break;
											case DISABLE_LOG :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String disableReason = in.readNullUTF();
													process.setCommand(
														"add_disable_log",
														account,
														disableReason
													);
													resp = Response.of(
														AoservProtocol.DONE,
														AccountHandler.addDisableLog(
															conn,
															source,
															invalidateList,
															account,
															disableReason
														)
													);
												}
												break;
											case DNS_RECORDS :
												{
													String zone        = in.readUTF();
													String domain      = in.readUTF().trim();
													String type        = in.readUTF();
													int priority       = in.readCompressedInt();
													int weight;
													int port;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_72)>=0) {
														weight         = in.readCompressedInt();
														port           = in.readCompressedInt();
													} else {
														weight         = Record.NO_WEIGHT;
														port           = Record.NO_PORT;
													}
													String destination = in.readUTF().trim();
													int ttl;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_127)>=0) {
														ttl            = in.readCompressedInt();
													} else {
														ttl            = Record.NO_TTL;
													}
													process.setCommand(
														Command.ADD_DNS_RECORD,
														zone,
														domain,
														type,
														priority==Record.NO_PRIORITY ? null : priority,
														weight==Record.NO_WEIGHT ? null : weight,
														port==Record.NO_PORT ? null : port,
														destination,
														ttl==Record.NO_TTL ? null : ttl
													);
													resp = Response.of(
														AoservProtocol.DONE,
														MasterServer.getService(DnsService.class).addRecord(
															conn,
															source,
															invalidateList,
															zone,
															domain,
															type,
															priority,
															weight,
															port,
															destination,
															ttl
														)
													);
												}
												break;
											case DNS_ZONES :
												{
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													String zone = in.readUTF().trim();
													InetAddress ip = InetAddress.valueOf(in.readUTF());
													int ttl = in.readCompressedInt();
													process.setCommand(
														Command.ADD_DNS_ZONE,
														packageName,
														zone,
														ip,
														ttl
													);
													MasterServer.getService(DnsService.class).addDNSZone(
														conn,
														source,
														invalidateList,
														packageName,
														zone,
														ip,
														ttl
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_ADDRESSES :
												{
													String address = in.readUTF().trim();
													int domain = in.readCompressedInt();
													process.setCommand(
														Command.ADD_EMAIL_ADDRESS,
														address,
														domain
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addAddress(
															conn,
															source,
															invalidateList,
															address,
															domain
														)
													);
												}
												break;
											case EMAIL_DOMAINS :
												{
													DomainName domain = DomainName.valueOf(in.readUTF());
													int linuxServer = in.readCompressedInt();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_EMAIL_DOMAIN,
														domain,
														linuxServer,
														packageName
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addDomain(
															conn,
															source,
															invalidateList,
															domain,
															linuxServer,
															packageName
														)
													);
												}
												break;
											case EMAIL_FORWARDING :
												{
													int address = in.readCompressedInt();
													Email destination = Email.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_EMAIL_FORWARDING,
														address,
														destination
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addForwarding(
															conn,
															source,
															invalidateList,
															address,
															destination
														)
													);
												}
												break;
											case EMAIL_LIST_ADDRESSES :
												{
													int address=in.readCompressedInt();
													int list=in.readCompressedInt();
													process.setCommand(
														Command.ADD_EMAIL_LIST_ADDRESS,
														address,
														list
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addListAddress(
															conn,
															source,
															invalidateList,
															address,
															list
														)
													);
												}
												break;
											case EMAIL_LISTS :
												{
													PosixPath path = PosixPath.valueOf(in.readUTF());
													int userServer = in.readCompressedInt();
													int groupServer = in.readCompressedInt();
													process.setCommand(
														Command.ADD_EMAIL_LIST,
														path,
														userServer,
														groupServer
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addList(
															conn,
															source,
															invalidateList,
															path,
															userServer,
															groupServer
														)
													);
												}
												break;
											case EMAIL_PIPE_ADDRESSES :
												{
													int address=in.readCompressedInt();
													int pipe=in.readCompressedInt();
													process.setCommand(
														Command.ADD_EMAIL_PIPE_ADDRESS,
														address,
														pipe
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addPipeAddress(
															conn,
															source,
															invalidateList,
															address,
															pipe
														)
													);
												}
												break;
											case EMAIL_PIPES :
												{
													int linuxServer = in.readCompressedInt();
													String command = in.readUTF();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_EMAIL_PIPE,
														linuxServer,
														command,
														packageName
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addPipe(
															conn,
															source,
															invalidateList,
															linuxServer,
															command,
															packageName
														)
													);
												}
												break;
											case EMAIL_SMTP_RELAYS :
												{
													process.setPriority(Thread.NORM_PRIORITY+1);
													currentThread.setPriority(Thread.NORM_PRIORITY+1);

													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													int linuxServer= in.readCompressedInt();
													HostAddress host = HostAddress.valueOf(in.readUTF());
													String type=in.readUTF();
													long duration=in.readLong();
													process.setCommand(
														Command.ADD_EMAIL_SMTP_RELAY,
														packageName,
														linuxServer==-1?null:linuxServer,
														host,
														type,
														duration
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addSmtpRelay(
															conn,
															source,
															invalidateList,
															packageName,
															linuxServer,
															host,
															type,
															duration
														)
													);
												}
												break;
											case FAILOVER_FILE_LOG :
												{
													int fileReplication=in.readCompressedInt();
													long fflStartTime=in.readLong();
													long endTime=in.readLong();
													int scanned=in.readCompressedInt();
													int updated=in.readCompressedInt();
													long bytes=in.readLong();
													boolean isSuccessful=in.readBoolean();
													process.setCommand(
														"add_failover_file_log",
														fileReplication,
														new java.util.Date(fflStartTime),
														new java.util.Date(endTime),
														scanned,
														updated,
														bytes,
														isSuccessful
													);
													resp = Response.of(
														AoservProtocol.DONE,
														FailoverHandler.addFileReplicationLog(
															conn,
															source,
															invalidateList,
															fileReplication,
															fflStartTime,
															endTime,
															scanned,
															updated,
															bytes,
															isSuccessful
														)
													);
												}
												break;
											case FILE_BACKUP_SETTINGS :
												{
													int fileReplication=in.readCompressedInt();
													String path=in.readUTF();
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
														int packageNum=in.readCompressedInt();
													}
													boolean backupEnabled;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
														short backupLevel=in.readShort();
														short backupRetention=in.readShort();
														boolean recurse=in.readBoolean();
														backupEnabled = backupLevel>0;
													} else {
														backupEnabled = in.readBoolean();
													}
													boolean required;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_62)>=0) {
														required = in.readBoolean();
													} else {
														required = false;
													}
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_31)<0) {
														throw new IOException(Command.ADD_FILE_BACKUP_SETTING+" call not supported for AoservProtocol < "+AoservProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
													}
													process.setCommand(
														Command.ADD_FILE_BACKUP_SETTING,
														fileReplication,
														path,
														backupEnabled,
														required
													);
													resp = Response.of(
														AoservProtocol.DONE,
														BackupHandler.addFileReplicationSetting(
															conn,
															source,
															invalidateList,
															fileReplication,
															path,
															backupEnabled,
															required
														)
													);
												}
												break;
											case FTP_GUEST_USERS :
												{
													com.aoindustries.aoserv.client.linux.User.Name linuxUser = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_FTP_GUEST_USER,
														linuxUser
													);
													FTPHandler.addGuestUser(
														conn,
														source,
														invalidateList,
														linuxUser
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SHARED_TOMCATS :
												{
													String name=in.readUTF().trim();
													int linuxServer=in.readCompressedInt();
													int version=in.readCompressedInt();
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													Group.Name group = Group.Name.valueOf(in.readUTF());
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_9) <= 0) {
														boolean isSecure = in.readBoolean();
														boolean isOverflow = in.readBoolean();
														if(isSecure) throw new IOException(Command.ADD_HTTPD_SHARED_TOMCAT + " call no longer supports is_secure=true");
														if(isOverflow) throw new IOException(Command.ADD_HTTPD_SHARED_TOMCAT + " call no longer supports isOverflow=true");
													}
													process.setCommand(
														Command.ADD_HTTPD_SHARED_TOMCAT,
														name,
														linuxServer,
														version,
														user,
														group
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addSharedTomcat(
															conn,
															source,
															invalidateList,
															name,
															linuxServer,
															version,
															user,
															group,
															false
														)
													);
												}
												break;
											case HTTPD_JBOSS_SITES :
												{
													int linuxServer=in.readCompressedInt();
													String siteName=in.readUTF().trim();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													Group.Name group = Group.Name.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache=in.readBoolean();
													int ipAddress=in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													int jBossVersion = in.readCompressedInt();
													PosixPath contentSrc;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_9) <= 0) {
														contentSrc = PosixPath.valueOf(in.readNullUTF());
													} else {
														contentSrc = null;
													}
													int phpVersion;
													boolean enableCgi;
													boolean enableSsi;
													boolean enableHtaccess;
													boolean enableIndexes;
													boolean enableFollowSymlinks;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) < 0) {
														phpVersion = -1;
														enableCgi = true;
														enableSsi = true;
														enableHtaccess = true;
														enableIndexes = true;
														enableFollowSymlinks = false;
													} else if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_6) < 0) {
														phpVersion = in.readCompressedInt();
														enableCgi = in.readBoolean();
														enableSsi = in.readBoolean();
														enableHtaccess = in.readBoolean();
														enableIndexes = in.readBoolean();
														enableFollowSymlinks = in.readBoolean();
													} else {
														phpVersion = -1;
														enableCgi = false;
														enableSsi = false;
														enableHtaccess = false;
														enableIndexes = false;
														enableFollowSymlinks = false;
													}
													process.setCommand(
														Command.ADD_HTTPD_JBOSS_SITE,
														linuxServer,
														siteName,
														packageName,
														user,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														jBossVersion,
														phpVersion,
														enableCgi,
														enableSsi,
														enableHtaccess,
														enableIndexes,
														enableFollowSymlinks
													);
													if(contentSrc != null) throw new IOException(Command.ADD_HTTPD_JBOSS_SITE + " call no longer supports non-null content_source");
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addJbossSite(
															conn,
															source,
															invalidateList,
															linuxServer,
															siteName,
															packageName,
															user,
															group,
															serverAdmin,
															useApache,
															ipAddress,
															primaryHttpHostname,
															altHttpHostnames,
															jBossVersion,
															phpVersion,
															enableCgi,
															enableSsi,
															enableHtaccess,
															enableIndexes,
															enableFollowSymlinks
														)
													);
												}
												break;
											case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
												{
													int httpd_site = in.readCompressedInt();
													String path = in.readUTF();
													boolean isRegularExpression = in.readBoolean();
													String authName = in.readUTF();
													PosixPath authGroupFile;
													{
														String s = in.readUTF();
														authGroupFile = s.isEmpty() ? null : PosixPath.valueOf(s);
													}
													PosixPath authUserFile;
													{
														String s = in.readUTF();
														authUserFile = s.isEmpty() ? null : PosixPath.valueOf(s);
													}
													String require = in.readUTF();
													String handler;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_13) >= 0) {
														String s = in.readUTF();
														handler = s.isEmpty() ? null : s;
													} else {
														handler = null;
													}

													process.setCommand(
														Command.ADD_HTTPD_SITE_AUTHENTICATED_LOCATION,
														httpd_site,
														path,
														isRegularExpression,
														authName,
														authGroupFile,
														authUserFile,
														require,
														handler
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addLocation(
															conn,
															source,
															invalidateList,
															httpd_site,
															path,
															isRegularExpression,
															authName,
															authGroupFile,
															authUserFile,
															require,
															handler
														)
													);
												}
												break;							
											case HTTPD_SITE_URLS :
												{
													int virtualHost = in.readCompressedInt();
													DomainName hostname = DomainName.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_HTTPD_SITE_URL,
														virtualHost,
														hostname
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addVirtualHostName(
															conn,
															source,
															invalidateList,
															virtualHost,
															hostname
														)
													);
												}
												break;
											case HTTPD_TOMCAT_CONTEXTS :
												{
													int tomcatSite = in.readCompressedInt();
													String className = in.readNullUTF();
													boolean cookies = in.readBoolean();
													boolean crossContext = in.readBoolean();
													PosixPath docBase = PosixPath.valueOf(in.readUTF());
													boolean override = in.readBoolean();
													String path = in.readUTF().trim();
													boolean privileged = in.readBoolean();
													boolean reloadable = in.readBoolean();
													boolean useNaming = in.readBoolean();
													String wrapperClass = in.readNullUTF();
													int debug = in.readCompressedInt();
													PosixPath workDir = PosixPath.valueOf(in.readNullUTF());
													boolean serverXmlConfigured;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_2) <= 0) {
														serverXmlConfigured = Context.DEFAULT_SERVER_XML_CONFIGURED;
													} else {
														serverXmlConfigured = in.readBoolean();
													}
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_CONTEXT,
														tomcatSite,
														className,
														cookies,
														crossContext,
														docBase,
														override,
														path,
														privileged,
														reloadable,
														useNaming,
														wrapperClass,
														debug,
														workDir,
														serverXmlConfigured
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addContext(
															conn,
															source,
															invalidateList,
															tomcatSite,
															className,
															cookies,
															crossContext,
															docBase,
															override,
															path,
															privileged,
															reloadable,
															useNaming,
															wrapperClass,
															debug,
															workDir,
															serverXmlConfigured
														)
													);
												}
												break;							
											case HTTPD_TOMCAT_DATA_SOURCES :
												{
													int context=in.readCompressedInt();
													String name=in.readUTF();
													String driverClassName=in.readUTF();
													String url=in.readUTF();
													String username=in.readUTF();
													String password=in.readUTF();
													int maxActive=in.readCompressedInt();
													int maxIdle=in.readCompressedInt();
													int maxWait=in.readCompressedInt();
													String validationQuery=in.readUTF();
													if(validationQuery.length() == 0) validationQuery=null;
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_DATA_SOURCE,
														context,
														name,
														driverClassName,
														url,
														username,
														AoservProtocol.FILTERED,
														maxActive,
														maxIdle,
														maxWait,
														validationQuery
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addContextDataSource(
															conn,
															source,
															invalidateList,
															context,
															name,
															driverClassName,
															url,
															username,
															password,
															maxActive,
															maxIdle,
															maxWait,
															validationQuery
														)
													);
												}
												break;							
											case HTTPD_TOMCAT_PARAMETERS :
												{
													int context=in.readCompressedInt();
													String name=in.readUTF();
													String value=in.readUTF();
													boolean override=in.readBoolean();
													String description=in.readUTF();
													if(description.length() == 0) description=null;
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_PARAMETER,
														context,
														name,
														value,
														override,
														description
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addContextParameter(
															conn,
															source,
															invalidateList,
															context,
															name,
															value,
															override,
															description
														)
													);
												}
												break;							
											case HTTPD_TOMCAT_SITE_JK_MOUNTS :
												{
													int tomcatSite = in.readCompressedInt();
													String path = in.readUTF();
													boolean mount = in.readBoolean();
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_SITE_JK_MOUNT,
														tomcatSite,
														path,
														mount
													);
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addJkMount(
															conn,
															source,
															invalidateList,
															tomcatSite,
															path,
															mount
														)
													);
												}
												break;							
											case HTTPD_TOMCAT_SHARED_SITES :
												{
													int linuxServer = in.readCompressedInt();
													String siteName=in.readUTF().trim();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													Group.Name group = Group.Name.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache = in.readBoolean();
													int ipAddress = in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													String sharedTomcatName;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_9) <= 0) {
														sharedTomcatName = in.readNullUTF();
														int version = in.readCompressedInt();
													} else {
														sharedTomcatName = in.readUTF();
													}
													PosixPath contentSrc;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_9) <= 0) {
														contentSrc = PosixPath.valueOf(in.readNullUTF());
													} else {
														contentSrc = null;
													}
													int phpVersion;
													boolean enableCgi;
													boolean enableSsi;
													boolean enableHtaccess;
													boolean enableIndexes;
													boolean enableFollowSymlinks;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) < 0) {
														phpVersion = -1;
														enableCgi = true;
														enableSsi = true;
														enableHtaccess = true;
														enableIndexes = true;
														enableFollowSymlinks = false;
													} else if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_6) < 0) {
														phpVersion = in.readCompressedInt();
														enableCgi = in.readBoolean();
														enableSsi = in.readBoolean();
														enableHtaccess = in.readBoolean();
														enableIndexes = in.readBoolean();
														enableFollowSymlinks = in.readBoolean();
													} else {
														phpVersion = -1;
														enableCgi = false;
														enableSsi = false;
														enableHtaccess = false;
														enableIndexes = false;
														enableFollowSymlinks = false;
													}
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_SHARED_SITE,
														linuxServer,
														siteName,
														packageName,
														user,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														sharedTomcatName,
														phpVersion,
														enableCgi,
														enableSsi,
														enableHtaccess,
														enableIndexes,
														enableFollowSymlinks
													);
													if(sharedTomcatName == null) throw new IOException(Command.ADD_HTTPD_TOMCAT_SHARED_SITE + " call now requires non-null shared_tomcat_name");
													if(contentSrc != null) throw new IOException(Command.ADD_HTTPD_TOMCAT_SHARED_SITE + " call no longer supports non-null content_source");
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addSharedTomcatSite(
															conn,
															source,
															invalidateList,
															linuxServer,
															siteName,
															packageName,
															user,
															group,
															serverAdmin,
															useApache,
															ipAddress,
															primaryHttpHostname,
															altHttpHostnames,
															sharedTomcatName,
															phpVersion,
															enableCgi,
															enableSsi,
															enableHtaccess,
															enableIndexes,
															enableFollowSymlinks
														)
													);
												}
												break;
											case HTTPD_TOMCAT_STD_SITES :
												{
													int linuxServer = in.readCompressedInt();
													String siteName = in.readUTF().trim();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													Group.Name group = Group.Name.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache = in.readBoolean();
													int ipAddress = in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													int tomcatVersion = in.readCompressedInt();
													PosixPath contentSrc;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_9) <= 0) {
														contentSrc = PosixPath.valueOf(in.readNullUTF());
													} else {
														contentSrc = null;
													}
													int phpVersion;
													boolean enableCgi;
													boolean enableSsi;
													boolean enableHtaccess;
													boolean enableIndexes;
													boolean enableFollowSymlinks;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) < 0) {
														phpVersion = -1;
														enableCgi = true;
														enableSsi = true;
														enableHtaccess = true;
														enableIndexes = true;
														enableFollowSymlinks = false;
													} else if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_6) < 0) {
														phpVersion = in.readCompressedInt();
														enableCgi = in.readBoolean();
														enableSsi = in.readBoolean();
														enableHtaccess = in.readBoolean();
														enableIndexes = in.readBoolean();
														enableFollowSymlinks = in.readBoolean();
													} else {
														phpVersion = -1;
														enableCgi = false;
														enableSsi = false;
														enableHtaccess = false;
														enableIndexes = false;
														enableFollowSymlinks = false;
													}
													process.setCommand(
														Command.ADD_HTTPD_TOMCAT_STD_SITE,
														linuxServer,
														siteName,
														packageName,
														user,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														tomcatVersion,
														phpVersion,
														enableCgi,
														enableSsi,
														enableHtaccess,
														enableIndexes,
														enableFollowSymlinks
													);
													if(contentSrc != null) throw new IOException(Command.ADD_HTTPD_TOMCAT_STD_SITE + " call no longer supports non-null content_source");
													resp = Response.of(
														AoservProtocol.DONE,
														WebHandler.addPrivateTomcatSite(
															conn,
															source,
															invalidateList,
															linuxServer,
															siteName,
															packageName,
															user,
															group,
															serverAdmin,
															useApache,
															ipAddress,
															primaryHttpHostname,
															altHttpHostnames,
															tomcatVersion,
															phpVersion,
															enableCgi,
															enableSsi,
															enableHtaccess,
															enableIndexes,
															enableFollowSymlinks
														)
													);
												}
												break;
											case LINUX_ACC_ADDRESSES :
												{
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_31)<0) {
														int address = in.readCompressedInt();
														String username = in.readUTF().trim();
														throw new IOException(Command.ADD_LINUX_ACC_ADDRESS+" call not supported for AoservProtocol < "+AoservProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
													}
													int address = in.readCompressedInt();
													int userServer = in.readCompressedInt();
													process.setCommand(
														Command.ADD_LINUX_ACC_ADDRESS,
														address,
														userServer
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addInboxAddress(
															conn,
															source,
															invalidateList,
															address,
															userServer
														)
													);
												}
												break;
											case LINUX_ACCOUNTS :
												{
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													Group.Name primary_group = Group.Name.valueOf(in.readUTF());
													Gecos name;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) < 0) {
														name = Gecos.valueOf(in.readUTF());
													} else {
														name = Gecos.valueOf(in.readNullUTF());
													}
													Gecos office_location = Gecos.valueOf(in.readNullUTF());
													Gecos office_phone = Gecos.valueOf(in.readNullUTF());
													Gecos home_phone = Gecos.valueOf(in.readNullUTF());
													String type = in.readUTF().trim();
													PosixPath shell = PosixPath.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_LINUX_ACCOUNT,
														user,
														primary_group,
														name,
														office_location,
														office_phone,
														home_phone,
														type,
														shell
													);
													LinuxAccountHandler.addUser(
														conn,
														source,
														invalidateList,
														user,
														primary_group,
														name,
														office_location,
														office_phone,
														home_phone,
														type,
														shell,
														false
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_GROUP_ACCOUNTS :
												{
													Group.Name group = Group.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_LINUX_GROUP_ACCOUNT,
														group,
														user
													);
													resp = Response.of(
														AoservProtocol.DONE,
														LinuxAccountHandler.addGroupUser(
															conn,
															source,
															invalidateList,
															group,
															user,
															false,
															false
														)
													);
												}
												break;
											case LINUX_GROUPS :
												{
													Group.Name name = Group.Name.valueOf(in.readUTF());
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													String type = in.readUTF().trim();
													process.setCommand(
														Command.ADD_LINUX_GROUP,
														name,
														packageName,
														type
													);
													LinuxAccountHandler.addGroup(
														conn,
														source,
														invalidateList,
														name,
														packageName,
														type,
														false
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_SERVER_ACCOUNTS :
												{
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													int linuxServer = in.readCompressedInt();
													PosixPath home = PosixPath.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_LINUX_SERVER_ACCOUNT,
														user,
														linuxServer,
														home
													);
													resp = Response.of(
														AoservProtocol.DONE,
														LinuxAccountHandler.addUserServer(
															conn,
															source,
															invalidateList,
															user,
															linuxServer,
															home,
															false
														)
													);
												}
												break;
											case LINUX_SERVER_GROUPS :
												{
													Group.Name group = Group.Name.valueOf(in.readUTF());
													int linuxServer = in.readCompressedInt();
													process.setCommand(
														Command.ADD_LINUX_SERVER_GROUP,
														group,
														linuxServer
													);
													resp = Response.of(
														AoservProtocol.DONE,
														LinuxAccountHandler.addGroupServer(
															conn,
															source,
															invalidateList,
															group,
															linuxServer,
															false
														)
													);
												}
												break;
											case MAJORDOMO_LISTS :
												{
													int majordomoServer = in.readCompressedInt();
													String listName = in.readUTF().trim();
													process.setCommand(
														Command.ADD_MAJORDOMO_LIST,
														majordomoServer,
														listName
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addMajordomoList(
															conn,
															source,
															invalidateList,
															majordomoServer,
															listName
														)
													);
												}
												break;
											case MAJORDOMO_SERVERS :
												{
													int emailDomain = in.readCompressedInt();
													int lsa = in.readCompressedInt();
													int lsg = in.readCompressedInt();
													String version = in.readUTF().trim();
													process.setCommand(
														Command.ADD_MAJORDOMO_SERVER,
														emailDomain,
														lsa,
														lsg,
														version
													);
													EmailHandler.addMajordomoServer(
														conn,
														source,
														invalidateList,
														emailDomain,
														lsa,
														lsg,
														version
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_DATABASES :
												{
													Database.Name name = Database.Name.valueOf(in.readUTF());
													int mysqlServer = in.readCompressedInt();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_MYSQL_DATABASE,
														name,
														mysqlServer,
														packageName
													);
													resp = Response.of(
														AoservProtocol.DONE,
														MysqlHandler.addDatabase(
															conn,
															source,
															invalidateList,
															name,
															mysqlServer,
															packageName
														)
													);
												}
												break;
											case MYSQL_DB_USERS :
												{
													int database = in.readCompressedInt();
													int userServer = in.readCompressedInt();
													boolean canSelect=in.readBoolean();
													boolean canInsert=in.readBoolean();
													boolean canUpdate=in.readBoolean();
													boolean canDelete=in.readBoolean();
													boolean canCreate=in.readBoolean();
													boolean canDrop=in.readBoolean();
													boolean canReference;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_2) >= 0) {
														canReference = in.readBoolean();
													} else {
														// Default to copying drop_priv for older clients
														canReference = canDrop;
													}
													boolean canIndex=in.readBoolean();
													boolean canAlter=in.readBoolean();
													boolean canCreateTempTable;
													boolean canLockTables;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_111)>=0) {
														canCreateTempTable=in.readBoolean();
														canLockTables=in.readBoolean();
													} else {
														canCreateTempTable=false;
														canLockTables=false;
													}
													boolean canCreateView;
													boolean canShowView;
													boolean canCreateRoutine;
													boolean canAlterRoutine;
													boolean canExecute;
													boolean canEvent;
													boolean canTrigger;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_4)>=0) {
														canCreateView=in.readBoolean();
														canShowView=in.readBoolean();
														canCreateRoutine=in.readBoolean();
														canAlterRoutine=in.readBoolean();
														canExecute=in.readBoolean();
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_54)>=0) {
															canEvent=in.readBoolean();
															canTrigger=in.readBoolean();
														} else {
															canEvent=false;
															canTrigger=false;
														}
													} else {
														canCreateView=false;
														canShowView=false;
														canCreateRoutine=false;
														canAlterRoutine=false;
														canExecute=false;
														canEvent=false;
														canTrigger=false;
													}
													process.setCommand(
														Command.ADD_MYSQL_DB_USER,
														database,
														userServer,
														canSelect,
														canInsert,
														canUpdate,
														canDelete,
														canCreate,
														canDrop,
														canReference,
														canIndex,
														canAlter,
														canCreateTempTable,
														canLockTables,
														canCreateView,
														canShowView,
														canCreateRoutine,
														canAlterRoutine,
														canExecute,
														canEvent,
														canTrigger
													);
													resp = Response.of(
														AoservProtocol.DONE,
														MysqlHandler.addDatabaseUser(
															conn,
															source,
															invalidateList,
															database,
															userServer,
															canSelect,
															canInsert,
															canUpdate,
															canDelete,
															canCreate,
															canDrop,
															canReference,
															canIndex,
															canAlter,
															canCreateTempTable,
															canLockTables,
															canCreateView,
															canShowView,
															canCreateRoutine,
															canAlterRoutine,
															canExecute,
															canEvent,
															canTrigger
														)
													);
												}
												break;
											case MYSQL_SERVER_USERS :
												{
													com.aoindustries.aoserv.client.mysql.User.Name user = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(in.readUTF());
													int mysqlServer = in.readCompressedInt();
													String host = in.readNullUTF();
													process.setCommand(
														Command.ADD_MYSQL_SERVER_USER,
														user,
														mysqlServer,
														host
													);
													resp = Response.of(
														AoservProtocol.DONE,
														MysqlHandler.addUserServer(
															conn,
															source,
															invalidateList,
															user,
															mysqlServer,
															host
														)
													);
												}
												break;
											case MYSQL_USERS :
												{
													com.aoindustries.aoserv.client.mysql.User.Name user = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_MYSQL_USER,
														user
													);
													MysqlHandler.addUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case NET_BINDS :
												{
													int host = in.readCompressedInt();
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													int ipAddress = in.readCompressedInt();
													Port port;
													{
														int portNum = in.readCompressedInt();
														Protocol protocol;
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) < 0) {
															protocol = Protocol.valueOf(in.readUTF().toUpperCase(Locale.ROOT));
														} else {
															protocol = in.readEnum(Protocol.class);
														}
														port = Port.valueOf(portNum, protocol);
													}
													String appProtocol = in.readUTF().trim();
													boolean monitoringEnabled;
													int numZones;
													Set<FirewallZone.Name> firewalldZones;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_2) <= 0) {
														boolean openFirewall = in.readBoolean();
														if(openFirewall) {
															numZones = 1;
															firewalldZones = Collections.singleton(FirewallZone.PUBLIC);
														} else {
															numZones = 0;
															firewalldZones = Collections.emptySet();
														}
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_103) <= 0) {
															monitoringEnabled = in.readCompressedInt() != -1;
															in.readNullUTF();
															in.readNullUTF();
															in.readNullUTF();
														} else {
															monitoringEnabled = in.readBoolean();
														}
													} else {
														monitoringEnabled = in.readBoolean();
														numZones = in.readCompressedInt();
														firewalldZones = new LinkedHashSet<>(numZones*4/3+1);
														for(int i = 0; i < numZones; i++) {
															FirewallZone.Name name = FirewallZone.Name.valueOf(in.readUTF());
															if(!firewalldZones.add(name)) {
																throw new IOException("Duplicate firewalld name: " + name);
															}
														}
													}
													Object[] command = new Object[7 + numZones];
													command[0] = Command.ADD_NET_BIND;
													command[1] = host;
													command[2] = packageName;
													command[3] = ipAddress;
													command[4] = port;
													command[5] = appProtocol;
													command[6] = monitoringEnabled;
													System.arraycopy(
														firewalldZones.toArray(new FirewallZone.Name[numZones]),
														0,
														command,
														7,
														numZones
													);
													process.setCommand(command);
													resp = Response.of(
														AoservProtocol.DONE,
														NetBindHandler.addBind(
															conn,
															source,
															invalidateList,
															host,
															packageName,
															ipAddress,
															port,
															appProtocol,
															monitoringEnabled,
															firewalldZones
														)
													);
												}
												break;
											case NOTICE_LOG :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String billingContact = in.readUTF().trim();
													String emailAddress = in.readUTF().trim();
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														in.readCompressedInt(); // balance ignored, current balance queried
													}
													String type = in.readUTF().trim();
													int transid = in.readCompressedInt();
													process.setCommand(
														Command.ADD_NOTICE_LOG,
														account,
														billingContact,
														emailAddress,
														type,
														transid
													);
													int id = AccountHandler.addNoticeLog(
														conn,
														source,
														invalidateList,
														account,
														billingContact,
														emailAddress,
														type,
														transid
													);
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														resp = Response.DONE;
													} else {
														resp = Response.of(
															AoservProtocol.DONE,
															id
														);
													}
												}
												break;
											case PACKAGES :
												{
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													Account.Name account = Account.Name.valueOf(in.readUTF());
													int packageDefinition;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_122)<=0) {
														// Try to find a package definition owned by the source accounting with matching rates and limits
														String level=in.readUTF().trim();
														Money rate = new Money(Currency.USD, in.readCompressedInt(), 2);
														int userLimit=in.readCompressedInt();
														int additionalUserRate=in.readCompressedInt();
														int popLimit=in.readCompressedInt();
														int additionalPopRate=in.readCompressedInt();
														Account.Name baAccounting = AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator());
														packageDefinition = PackageHandler.findActivePackageDefinition(
															conn,
															baAccounting,
															rate,
															userLimit,
															popLimit
														);
														if(packageDefinition==-1) {
															throw new SQLException(
																"Unable to find PackageDefinition: accounting="
																+ baAccounting
																+ ", rate="
																+ rate
																+ ", userLimit="
																+ (userLimit==-1?"unlimited":Integer.toString(userLimit))
																+ ", popLimit="
																+ (popLimit==-1?"unlimited":Integer.toString(popLimit))
															);
														}
													} else {
														packageDefinition = in.readCompressedInt();
													}
													process.setCommand(
														Command.ADD_PACKAGE,
														packageName,
														account,
														packageDefinition
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PackageHandler.addPackage(
															conn,
															source,
															invalidateList,
															packageName,
															account,
															packageDefinition
														)
													);
												}
												break;
											case PACKAGE_DEFINITIONS :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													String category = in.readUTF().trim();
													String name = in.readUTF().trim();
													String version = in.readUTF().trim();
													String display = in.readUTF().trim();
													String description = in.readUTF().trim();
													Money setupFee;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														int pennies = in.readCompressedInt();
														setupFee = pennies == -1 || pennies == 0 ? null : new Money(Currency.USD, pennies, 2);
													} else {
														setupFee = MoneyUtil.readNullMoney(in);
													}
													String setupFeeTransactionType = in.readNullUTF();
													Money monthlyRate;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														monthlyRate = new Money(Currency.USD, in.readCompressedInt(), 2);
													} else {
														monthlyRate = MoneyUtil.readMoney(in);
													}
													String monthlyRateTransactionType = in.readUTF();
													process.setCommand(
														"add_package_definition",
														account,
														category,
														name,
														version,
														display,
														description,
														setupFee,
														setupFeeTransactionType,
														monthlyRate,
														monthlyRateTransactionType
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PackageHandler.addPackageDefinition(
															conn,
															source,
															invalidateList,
															account,
															category,
															name,
															version,
															display,
															description,
															setupFee,
															setupFeeTransactionType,
															monthlyRate,
															monthlyRateTransactionType
														)
													);
												}
												break;
											case POSTGRES_DATABASES :
												{
													com.aoindustries.aoserv.client.postgresql.Database.Name name = com.aoindustries.aoserv.client.postgresql.Database.Name.valueOf(in.readUTF());
													int postgresqlServer = in.readCompressedInt();
													int datdba = in.readCompressedInt();
													int encoding = in.readCompressedInt();
													boolean enable_postgis = source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_27)>=0?in.readBoolean():false;
													process.setCommand(
														Command.ADD_POSTGRES_DATABASE,
														name,
														postgresqlServer,
														datdba,
														encoding,
														enable_postgis
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PostgresqlHandler.addDatabase(
															conn,
															source,
															invalidateList,
															name,
															postgresqlServer,
															datdba,
															encoding,
															enable_postgis
														)
													);
												}
												break;
											case POSTGRES_SERVER_USERS :
												{
													com.aoindustries.aoserv.client.postgresql.User.Name user = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(in.readUTF());
													int postgresqlServer = in.readCompressedInt();
													process.setCommand(
														Command.ADD_POSTGRES_SERVER_USER,
														user,
														postgresqlServer
													);
													resp = Response.of(
														AoservProtocol.DONE,
														PostgresqlHandler.addUserServer(
															conn,
															source,
															invalidateList,
															user,
															postgresqlServer
														)
													);
												}
												break;
											case POSTGRES_USERS :
												{
													com.aoindustries.aoserv.client.postgresql.User.Name user = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_POSTGRES_USER,
														user
													);
													PostgresqlHandler.addUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case SIGNUP_REQUESTS :
												{
													Account.Name account = Account.Name.valueOf(in.readUTF());
													InetAddress ip_address = InetAddress.valueOf(in.readUTF());
													int package_definition = in.readCompressedInt();
													String business_name = in.readUTF();
													String business_phone = in.readUTF();
													String business_fax = in.readNullUTF();
													String business_address1 = in.readUTF();
													String business_address2 = in.readNullUTF();
													String business_city = in.readUTF();
													String business_state = in.readNullUTF();
													String business_country = in.readUTF();
													String business_zip = in.readNullUTF();
													String ba_name = in.readUTF();
													String ba_title = in.readNullUTF();
													String ba_work_phone = in.readUTF();
													String ba_cell_phone = in.readNullUTF();
													String ba_home_phone = in.readNullUTF();
													String ba_fax = in.readNullUTF();
													String ba_email = in.readUTF();
													String ba_address1 = in.readNullUTF();
													String ba_address2 = in.readNullUTF();
													String ba_city = in.readNullUTF();
													String ba_state = in.readNullUTF();
													String ba_country = in.readNullUTF();
													String ba_zip = in.readNullUTF();
													com.aoindustries.aoserv.client.account.User.Name administrator_user_name = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													String billing_contact = in.readUTF();
													String billing_email = in.readUTF();
													boolean billing_use_monthly = in.readBoolean();
													boolean billing_pay_one_year = in.readBoolean();
													// Encrypted values
													int from;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) from = 2; // Hard-coded value from AO website key
													else from = in.readCompressedInt();
													int recipient = in.readCompressedInt();
													String ciphertext = in.readUTF();
													// options
													int numOptions = in.readCompressedInt();
													Map<String,String> options = new HashMap<>(numOptions * 4 / 3 + 1);
													for(int c=0;c<numOptions;c++) {
														String name = in.readUTF();
														String value = in.readNullUTF();
														options.put(name, value);
													}
													process.setCommand(
														"add_signup_request",
														account,
														ip_address,
														package_definition,
														business_name,
														business_phone,
														business_fax,
														business_address1,
														business_address2,
														business_city,
														business_state,
														business_country,
														business_zip,
														ba_name,
														ba_title,
														ba_work_phone,
														ba_cell_phone,
														ba_home_phone,
														ba_fax,
														ba_email,
														ba_address1,
														ba_address2,
														ba_city,
														ba_state,
														ba_country,
														ba_zip,
														administrator_user_name,
														billing_contact,
														billing_email,
														billing_use_monthly,
														billing_pay_one_year,
														// Encrypted values
														from,
														recipient,
														ciphertext,
														// options
														numOptions
													);
													resp = Response.of(
														AoservProtocol.DONE,
														SignupHandler.addRequest(
															conn,
															source,
															invalidateList,
															account,
															ip_address,
															package_definition,
															business_name,
															business_phone,
															business_fax,
															business_address1,
															business_address2,
															business_city,
															business_state,
															business_country,
															business_zip,
															ba_name,
															ba_title,
															ba_work_phone,
															ba_cell_phone,
															ba_home_phone,
															ba_fax,
															ba_email,
															ba_address1,
															ba_address2,
															ba_city,
															ba_state,
															ba_country,
															ba_zip,
															administrator_user_name,
															billing_contact,
															billing_email,
															billing_use_monthly,
															billing_pay_one_year,
															// Encrypted values
															from,
															recipient,
															ciphertext,
															// options
															options
														)
													);
												}
												break;
											case SPAM_EMAIL_MESSAGES :
												{
													int smtpRelay = in.readCompressedInt();
													String message = in.readUTF().trim();
													process.setCommand(
														Command.ADD_SPAM_EMAIL_MESSAGE,
														smtpRelay,
														message
													);
													resp = Response.of(
														AoservProtocol.DONE,
														EmailHandler.addSpamMessage(
															conn,
															source,
															invalidateList,
															smtpRelay,
															message
														)
													);
												}
												break;
											case TICKETS :
												{
													Account.Name brand;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_46)>=0) {
														brand = Account.Name.valueOf(in.readUTF());
													} else {
														brand = AccountHandler.getRootAccount();
													}
													Account.Name account;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_126)>=0) {
														account = Account.Name.valueOf(in.readNullUTF());
													} else {
														Account.Name packageName = Account.Name.valueOf(in.readUTF());
														account = PackageHandler.getAccountForPackage(conn, packageName);
													}
													String language = source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_44)>=0 ? in.readUTF() : Language.EN;
													int category = source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_44)>=0 ? in.readCompressedInt() : -1;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) in.readUTF(); // username
													String type = in.readUTF();
													Email fromAddress;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_48)>=0) fromAddress = Email.valueOf(in.readNullUTF());
													else fromAddress = null;
													String summary = source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_44)>=0 ? in.readUTF() : "(No summary)";
													String details = source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_44)>=0 ? in.readNullLongUTF() : in.readUTF();
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) in.readLong(); // deadline
													String clientPriority=in.readUTF();
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) in.readUTF(); // adminPriority
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) in.readNullUTF(); // technology
													Set<Email> contactEmails;
													String contactPhoneNumbers;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_125)>=0) {
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) in.readNullUTF(); // assignedTo
														if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_22) >= 0) {
															int size = in.readCompressedInt();
															contactEmails = new LinkedHashSet<>(size*4/3+1);
															for(int i = 0; i < size; i++) {
																contactEmails.add(Email.valueOf(in.readUTF()));
															}
														} else {
															contactEmails = Profile.splitEmails(in.readUTF().trim());
														}
														contactPhoneNumbers=in.readUTF();
													} else {
														contactEmails = Collections.emptySet();
														contactPhoneNumbers="";
													}
													process.setCommand(
														"add_ticket",
														brand,
														account,
														language,
														category,
														type,
														fromAddress,
														summary,
														StringUtility.firstLineOnly(details, 60),
														clientPriority,
														contactEmails,
														contactPhoneNumbers
													);
													resp = Response.of(
														AoservProtocol.DONE,
														TicketHandler.addTicket(
															conn,
															source,
															invalidateList,
															brand,
															account,
															language,
															category,
															type,
															fromAddress,
															summary,
															details,
															clientPriority,
															contactEmails,
															contactPhoneNumbers
														)
													);
												}
												break;
											case TRANSACTIONS :
												{
													char timeType;
													Timestamp time;
													String commandArg;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														timeType = 'T';
														time = null;
														commandArg = "now";
													} else {
														timeType = (char)in.readByte();
														if(timeType == 'D') {
															Long millis = in.readNullLong();
															if(millis == null) {
																time = null;
																commandArg = "today";
															} else {
																time = new Timestamp(millis);
																commandArg = SQLUtility.formatDate(millis, Type.DATE_TIME_ZONE);
															}
														} else if(timeType == 'T') {
															time = in.readNullTimestamp();
															if(time == null) {
																commandArg = "now";
															} else {
																int nanos = time.getNanos();
																if(nanos == 0) {
																	commandArg = SQLUtility.formatDateTime(time);
																} else {
																	// TODO: Make a SQLUtility.formatDateTimeNanos?
																	StringBuilder nanoStr = new StringBuilder();
																	nanoStr.append(nanos);
																	while(nanoStr.length() < 9) nanoStr.insert(0, '0');
																	commandArg = SQLUtility.formatDateTime(time) + '.' + nanoStr;
																}
															}
														} else {
															throw new IOException("Unexpected value for timeType: " + timeType);
														}
													}
													Account.Name account = Account.Name.valueOf(in.readUTF());
													Account.Name sourceAccount = Account.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													String type = in.readUTF().trim();
													String description = in.readUTF().trim();
													int quantity = in.readCompressedInt();
													Money rate;
													if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
														rate = new Money(Currency.USD, in.readCompressedInt(), 2);
													} else {
														rate = MoneyUtil.readMoney(in);
													}
													String paymentType = in.readNullUTF();
													String paymentInfo = in.readNullUTF();
													String processor = in.readNullUTF();
													byte payment_confirmed = in.readByte();
													process.setCommand(
														Command.BILLING_TRANSACTION_ADD,
														commandArg,
														account,
														sourceAccount,
														administrator,
														type,
														description,
														SQLUtility.formatDecimal3(quantity),
														rate,
														paymentType,
														paymentInfo,
														processor,
														payment_confirmed == Transaction.CONFIRMED ? "Y"
														: payment_confirmed == Transaction.NOT_CONFIRMED ? "N"
														: "W"
													);
													resp = Response.of(
														AoservProtocol.DONE,
														BillingTransactionHandler.addTransaction(
															conn,
															source,
															invalidateList,
															timeType,
															time,
															account,
															sourceAccount,
															administrator,
															type,
															description,
															quantity,
															rate,
															paymentType,
															paymentInfo,
															processor,
															payment_confirmed
														)
													);
												}
												break;
											case USERNAMES :
												{
													Account.Name packageName = Account.Name.valueOf(in.readUTF());
													com.aoindustries.aoserv.client.account.User.Name name = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.ADD_USERNAME,
														packageName,
														name
													);
													AccountUserHandler.addUser(
														conn,
														source,
														invalidateList,
														packageName,
														name,
														false
													);
													resp = Response.DONE;
												}
												break;
											default :
												throw new IOException("Unknown table ID for add: clientTableID="+clientTableID+", tableID="+tableID);
										}
										sendInvalidateList=true;
										break;
									}
									case ADD_BACKUP_SERVER :
										{
											throw new RuntimeException("TODO: Update add_backup_server");
											/*
											String hostname=in.readUTF();
											String farm=in.readUTF();
											int owner=in.readCompressedInt();
											String description=in.readUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107)<=0) in.readUTF();
											int os_version=in.readCompressedInt();
											String username=in.readUTF();
											String password=in.readUTF();
											String contact_phone=in.readUTF();
											String contact_email=in.readUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_107)<=0) throw new IOException("addBackupServer call not supported for AOServ Client version <= "+AoservProtocol.VERSION_1_0_A_107+", please upgrade AOServ Client.");
											process.setCommand(
												Command.ADD_BACKUP_SERVER,
												hostname,
												farm,
												owner,
												description,
												os_version,
												username,
												AoservProtocol.FILTERED,
												contact_phone,
												contact_email
											);
											resp1=AoservProtocol.DONE;
											resp2Int = ServerHandler.addBackupServer(
												conn,
												source,
												invalidateList,
												hostname,
												farm,
												owner,
												description,
												os_version,
												username,
												password,
												contact_phone,
												contact_email
											);
											hasResp2Int=true;
											sendInvalidateList=true;
											break;
											 */
										}
									case ADD_MASTER_ENTROPY :
										{
											int numBytes=in.readCompressedInt();
											byte[] entropy=new byte[numBytes];
											for(int c=0;c<numBytes;c++) entropy[c]=in.readByte();
											process.setCommand(
												"add_master_entropy",
												numBytes
											);
											RandomHandler.addMasterEntropy(conn, source, entropy);
											resp = Response.DONE;
											sendInvalidateList=false;
										}
										break;
									/*case BOUNCE_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(Command.BOUNCE_TICKET);
											TicketHandler.bounceTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case ADD_SYSTEM_GROUP :
										{
											int linuxServer = in.readCompressedInt();
											Group.Name group = Group.Name.valueOf(in.readUTF());
											int gid = in.readCompressedInt();
											process.setCommand(
												"add_system_group",
												linuxServer,
												group,
												gid
											);
											resp = Response.of(
												AoservProtocol.DONE,
												LinuxAccountHandler.addSystemGroup(
													conn,
													source,
													invalidateList,
													linuxServer,
													group,
													gid
												)
											);
											sendInvalidateList = true;
										}
										break;
									case ADD_SYSTEM_USER:
										{
											int linuxServer = in.readCompressedInt();
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											int uid = in.readCompressedInt();
											int gid = in.readCompressedInt();
											Gecos fullName;
											{
												String s = in.readUTF();
												fullName = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											Gecos officeLocation;
											{
												String s = in.readUTF();
												officeLocation = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											Gecos officePhone;
											{
												String s = in.readUTF();
												officePhone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											Gecos homePhone;
											{
												String s = in.readUTF();
												homePhone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											PosixPath home = PosixPath.valueOf(in.readUTF());
											PosixPath shell = PosixPath.valueOf(in.readUTF());
											process.setCommand(
												"add_system_user",
												linuxServer,
												user,
												uid,
												gid,
												fullName,
												officeLocation,
												officePhone,
												homePhone,
												home,
												shell
											);
											resp = Response.of(
												AoservProtocol.DONE,
												LinuxAccountHandler.addSystemUser(
													conn,
													source,
													invalidateList,
													linuxServer,
													user,
													uid,
													gid,
													fullName,
													officeLocation,
													officePhone,
													homePhone,
													home,
													shell
												)
											);
											sendInvalidateList = true;
										}
										break;
									case CANCEL_BUSINESS :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											String cancelReason = in.readNullUTF();
											process.setCommand(Command.CANCEL_BUSINESS, account, cancelReason);
											AccountHandler.cancelAccount(conn, source, invalidateList, account, cancelReason);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									/*case CHANGE_TICKET_ADMIN_PRIORITY :
										{
											int ticketID=in.readCompressedInt(); 
											String priority=in.readUTF().trim();
											if(priority.length() == 0) priority=null;
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.CHANGE_TICKET_ADMIN_PRIORITY,
												ticketID,
												priority,
												username,
												comments
											);
											TicketHandler.changeTicketAdminPriority(
												conn,
												source,
												invalidateList,
												ticketID,
												priority,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case CHANGE_TICKET_CLIENT_PRIORITY :
										{
											int ticketID = in.readCompressedInt();
											String clientPriority = in.readUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) {
												String username = in.readUTF();
												String comments = in.readUTF();
											}
											process.setCommand(
												"change_ticket_client_priority",
												ticketID,
												clientPriority
											);
											TicketHandler.changeTicketClientPriority(
												conn,
												source,
												invalidateList,
												ticketID,
												clientPriority
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_TICKET_SUMMARY :
										{
											int ticketID = in.readCompressedInt();
											String summary = in.readUTF();
											process.setCommand(
												"set_ticket_summary",
												ticketID,
												summary
											);
											TicketHandler.setTicketSummary(
												conn,
												source,
												invalidateList,
												ticketID,
												summary
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case ADD_TICKET_ANNOTATION :
										{
											int ticketID = in.readCompressedInt();
											String summary = in.readUTF();
											String details = in.readNullLongUTF();
											process.setCommand(
												"add_ticket_annotation",
												ticketID,
												summary,
												StringUtility.firstLineOnly(details, 60)
											);
											TicketHandler.addTicketAnnotation(
												conn,
												source,
												invalidateList,
												ticketID,
												summary,
												details
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case CHANGE_TICKET_TYPE :
										{
											int ticketID = in.readCompressedInt();
											String oldType;
											String newType;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_48)>=0) {
												oldType = in.readUTF();
												newType = in.readUTF();
											} else {
												oldType = null;
												newType = in.readUTF();
												String username = in.readUTF(); // Unused
												String comments = in.readUTF(); // Unused
											}
											process.setCommand(
												"change_ticket_type",
												ticketID,
												oldType,
												newType
											);
											boolean updated = TicketHandler.setTicketType(
												conn,
												source,
												invalidateList,
												ticketID,
												oldType,
												newType
											);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_48) < 0) {
												resp = Response.DONE;
											} else {
												resp = Response.of(
													AoservProtocol.DONE,
													updated
												);
											}
											sendInvalidateList = true;
											break;
										}
									/*case COMPLETE_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.COMPLETE_TICKET,
												ticketID,
												username,
												comments
											);
											TicketHandler.completeTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case CHECK_SSL_CERTIFICATE :
										{
											int sslCertificate = in.readCompressedInt();
											process.setCommand(
												Command.CHECK_SSL_CERTIFICATE,
												sslCertificate
											);
											List<Certificate.Check> results = PkiCertificateHandler.check(
												conn,
												source,
												sslCertificate
											);
											conn.releaseConnection();
											out.writeByte(AoservProtocol.NEXT);
											int size = results.size();
											out.writeCompressedInt(size);
											for(int c = 0; c < size; c++) {
												Certificate.Check check = results.get(c);
												out.writeUTF(check.getCheck());
												out.writeUTF(check.getValue());
												out.writeUTF(check.getAlertLevel().name());
												out.writeNullUTF(check.getMessage());
											}
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case COMPARE_LINUX_SERVER_ACCOUNT_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readUTF();
											process.setCommand(
												Command.COMPARE_LINUX_SERVER_ACCOUNT_PASSWORD,
												userServer,
												AoservProtocol.FILTERED
											);
											boolean result = LinuxAccountHandler.comparePassword(
												conn,
												source,
												userServer,
												password
											);
											resp = Response.of(
												AoservProtocol.DONE,
												result
											);
											sendInvalidateList = false;
										}
										break;
									case COPY_HOME_DIRECTORY :
										{
											int from_lsa = in.readCompressedInt();
											int to_server = in.readCompressedInt();
											process.setCommand(
												Command.COPY_HOME_DIRECTORY,
												from_lsa,
												to_server
											);
											long byteCount = LinuxAccountHandler.copyHomeDirectory(
												conn,
												source,
												from_lsa,
												to_server
											);
											resp = Response.of(
												AoservProtocol.DONE,
												byteCount
											);
											sendInvalidateList = false;
										}
										break;
									case COPY_LINUX_SERVER_ACCOUNT_PASSWORD :
										{
											int from_lsa = in.readCompressedInt();
											int to_lsa = in.readCompressedInt();
											process.setCommand(
												Command.COPY_LINUX_SERVER_ACCOUNT_PASSWORD,
												from_lsa,
												to_lsa
											);
											LinuxAccountHandler.copyUserServerPassword(
												conn,
												source,
												invalidateList,
												from_lsa,
												to_lsa
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case COPY_PACKAGE_DEFINITION :
										{
											int packageDefinition = in.readCompressedInt();
											process.setCommand(
												"copy_package_definition",
												packageDefinition
											);
											resp = Response.of(
												AoservProtocol.DONE,
												PackageHandler.copyPackageDefinition(
													conn,
													source,
													invalidateList,
													packageDefinition
												)
											);
											sendInvalidateList = true;
										}
										break;
									case CREDIT_CARD_DECLINED :
										{
											int transid = in.readCompressedInt();
											String reason = in.readUTF().trim();
											process.setCommand(
												Command.DECLINE_CREDIT_CARD,
												transid,
												reason
											);
											PaymentHandler.creditCardDeclined(
												conn,
												source,
												invalidateList,
												transid,
												reason
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case CREDIT_CARD_TRANSACTION_SALE_COMPLETED :
										{
											int payment = in.readCompressedInt();
											String authorizationCommunicationResult = in.readNullUTF();
											String authorizationProviderErrorCode = in.readNullUTF();
											String authorizationErrorCode = in.readNullUTF();
											String authorizationProviderErrorMessage = in.readNullUTF();
											String authorizationProviderUniqueId = in.readNullUTF();
											String authorizationResult_providerReplacementMaskedCardNumber;
											String authorizationResult_replacementMaskedCardNumber;
											String authorizationResult_providerReplacementExpiration;
											Byte authorizationResult_replacementExpirationMonth;
											Short authorizationResult_replacementExpirationYear;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												authorizationResult_providerReplacementMaskedCardNumber = in.readNullUTF();
												authorizationResult_replacementMaskedCardNumber = in.readNullUTF();
												authorizationResult_providerReplacementExpiration = in.readNullUTF();
												authorizationResult_replacementExpirationMonth = in.readNullByte();
												authorizationResult_replacementExpirationYear = in.readNullShort();
											} else {
												authorizationResult_providerReplacementMaskedCardNumber = null;
												authorizationResult_replacementMaskedCardNumber = null;
												authorizationResult_providerReplacementExpiration = null;
												authorizationResult_replacementExpirationMonth = null;
												authorizationResult_replacementExpirationYear = null;
											}
											String providerApprovalResult = in.readNullUTF();
											String approvalResult = in.readNullUTF();
											String providerDeclineReason = in.readNullUTF();
											String declineReason = in.readNullUTF();
											String providerReviewReason = in.readNullUTF();
											String reviewReason = in.readNullUTF();
											String providerCvvResult = in.readNullUTF();
											String cvvResult = in.readNullUTF();
											String providerAvsResult = in.readNullUTF();
											String avsResult = in.readNullUTF();
											String approvalCode = in.readNullUTF();
											long captureTime = in.readLong();
											String capturePrincipalName = in.readNullUTF();
											String captureCommunicationResult = in.readNullUTF();
											String captureProviderErrorCode = in.readNullUTF();
											String captureErrorCode = in.readNullUTF();
											String captureProviderErrorMessage = in.readNullUTF();
											String captureProviderUniqueId = in.readNullUTF();
											String status = in.readNullUTF();
											process.setCommand(
												"credit_card_transaction_sale_completed",
												payment,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
												authorizationResult_providerReplacementMaskedCardNumber,
												authorizationResult_replacementMaskedCardNumber,
												authorizationResult_providerReplacementExpiration==null ? null : AoservProtocol.FILTERED,
												authorizationResult_replacementExpirationMonth==null ? null : AoservProtocol.FILTERED,
												authorizationResult_replacementExpirationYear==null ? null : AoservProtocol.FILTERED,
												providerApprovalResult,
												approvalResult,
												providerDeclineReason,
												declineReason,
												providerReviewReason,
												reviewReason,
												providerCvvResult,
												cvvResult,
												providerAvsResult,
												avsResult,
												approvalCode,
												captureTime == 0 ? null : new java.util.Date(captureTime),
												capturePrincipalName,
												captureCommunicationResult,
												captureProviderErrorCode,
												captureErrorCode,
												captureProviderErrorMessage,
												captureProviderUniqueId,
												status
											);
											PaymentHandler.paymentSaleCompleted(
												conn,
												source,
												invalidateList,
												payment,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
												authorizationResult_providerReplacementMaskedCardNumber,
												authorizationResult_replacementMaskedCardNumber,
												authorizationResult_providerReplacementExpiration,
												authorizationResult_replacementExpirationMonth,
												authorizationResult_replacementExpirationYear,
												providerApprovalResult,
												approvalResult,
												providerDeclineReason,
												declineReason,
												providerReviewReason,
												reviewReason,
												providerCvvResult,
												cvvResult,
												providerAvsResult,
												avsResult,
												approvalCode,
												captureTime,
												capturePrincipalName,
												captureCommunicationResult,
												captureProviderErrorCode,
												captureErrorCode,
												captureProviderErrorMessage,
												captureProviderUniqueId,
												status
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case CREDIT_CARD_TRANSACTION_AUTHORIZE_COMPLETED :
										{
											int payment = in.readCompressedInt();
											String authorizationCommunicationResult = in.readNullUTF();
											String authorizationProviderErrorCode = in.readNullUTF();
											String authorizationErrorCode = in.readNullUTF();
											String authorizationProviderErrorMessage = in.readNullUTF();
											String authorizationProviderUniqueId = in.readNullUTF();
											String authorizationResult_providerReplacementMaskedCardNumber;
											String authorizationResult_replacementMaskedCardNumber;
											String authorizationResult_providerReplacementExpiration;
											Byte authorizationResult_replacementExpirationMonth;
											Short authorizationResult_replacementExpirationYear;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												authorizationResult_providerReplacementMaskedCardNumber = in.readNullUTF();
												authorizationResult_replacementMaskedCardNumber = in.readNullUTF();
												authorizationResult_providerReplacementExpiration = in.readNullUTF();
												authorizationResult_replacementExpirationMonth = in.readNullByte();
												authorizationResult_replacementExpirationYear = in.readNullShort();
											} else {
												authorizationResult_providerReplacementMaskedCardNumber = null;
												authorizationResult_replacementMaskedCardNumber = null;
												authorizationResult_providerReplacementExpiration = null;
												authorizationResult_replacementExpirationMonth = null;
												authorizationResult_replacementExpirationYear = null;
											}
											String providerApprovalResult = in.readNullUTF();
											String approvalResult = in.readNullUTF();
											String providerDeclineReason = in.readNullUTF();
											String declineReason = in.readNullUTF();
											String providerReviewReason = in.readNullUTF();
											String reviewReason = in.readNullUTF();
											String providerCvvResult = in.readNullUTF();
											String cvvResult = in.readNullUTF();
											String providerAvsResult = in.readNullUTF();
											String avsResult = in.readNullUTF();
											String approvalCode = in.readNullUTF();
											String status = in.readNullUTF();
											process.setCommand(
												"credit_card_transaction_authorize_completed",
												payment,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
												authorizationResult_providerReplacementMaskedCardNumber,
												authorizationResult_replacementMaskedCardNumber,
												authorizationResult_providerReplacementExpiration==null ? null : AoservProtocol.FILTERED,
												authorizationResult_replacementExpirationMonth==null ? null : AoservProtocol.FILTERED,
												authorizationResult_replacementExpirationYear==null ? null : AoservProtocol.FILTERED,
												providerApprovalResult,
												approvalResult,
												providerDeclineReason,
												declineReason,
												providerReviewReason,
												reviewReason,
												providerCvvResult,
												cvvResult,
												providerAvsResult,
												avsResult,
												approvalCode,
												status
											);
											PaymentHandler.paymentAuthorizeCompleted(
												conn,
												source,
												invalidateList,
												payment,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
												authorizationResult_providerReplacementMaskedCardNumber,
												authorizationResult_replacementMaskedCardNumber,
												authorizationResult_providerReplacementExpiration,
												authorizationResult_replacementExpirationMonth,
												authorizationResult_replacementExpirationYear,
												providerApprovalResult,
												approvalResult,
												providerDeclineReason,
												declineReason,
												providerReviewReason,
												reviewReason,
												providerCvvResult,
												cvvResult,
												providerAvsResult,
												avsResult,
												approvalCode,
												status
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case DISABLE :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											int disableLog = in.readCompressedInt();
											switch(tableID) {
												case BUSINESSES :
													{
														Account.Name account = Account.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_BUSINESS,
															disableLog,
															account
														);
														AccountHandler.disableAccount(
															conn,
															source,
															invalidateList,
															disableLog,
															account
														);
													}
													break;
												case BUSINESS_ADMINISTRATORS :
													{
														com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_BUSINESS_ADMINISTRATOR,
															disableLog,
															administrator
														);
														AccountHandler.disableAdministrator(
															conn,
															source,
															invalidateList,
															disableLog,
															administrator
														);
													}
													break;
												case CVS_REPOSITORIES :
													{
														int cvsRepository = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_CVS_REPOSITORY,
															disableLog,
															cvsRepository
														);
														CvsHandler.disableCvsRepository(
															conn,
															source,
															invalidateList,
															disableLog,
															cvsRepository
														);
													}
													break;
												case EMAIL_LISTS :
													{
														int list = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_EMAIL_LIST,
															disableLog,
															list
														);
														EmailHandler.disableList(
															conn,
															source,
															invalidateList,
															disableLog,
															list
														);
													}
													break;
												case EMAIL_PIPES :
													{
														int pipe = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_EMAIL_PIPE,
															disableLog,
															pipe
														);
														EmailHandler.disablePipe(
															conn,
															source,
															invalidateList,
															disableLog,
															pipe
														);
													}
													break;
												case EMAIL_SMTP_RELAYS :
													{
														int smtpRelay = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_EMAIL_SMTP_RELAY,
															disableLog,
															smtpRelay
														);
														EmailHandler.disableSmtpRelay(
															conn,
															source,
															invalidateList,
															disableLog,
															smtpRelay
														);
													}
													break;
												case HTTPD_SHARED_TOMCATS :
													{
														int sharedTomcat = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_HTTPD_SHARED_TOMCAT,
															disableLog,
															sharedTomcat
														);
														WebHandler.disableSharedTomcat(
															conn,
															source,
															invalidateList,
															disableLog,
															sharedTomcat
														);
													}
													break;
												case HTTPD_SITES :
													{
														int site = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_HTTPD_SITE,
															disableLog,
															site
														);
														WebHandler.disableSite(
															conn,
															source,
															invalidateList,
															disableLog,
															site
														);
													}
													break;
												case HTTPD_SITE_BINDS :
													{
														int virtualHost = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_HTTPD_SITE_BIND,
															disableLog,
															virtualHost
														);
														WebHandler.disableVirtualHost(
															conn,
															source,
															invalidateList,
															disableLog,
															virtualHost
														);
													}
													break;
												case LINUX_ACCOUNTS :
													{
														com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_LINUX_ACCOUNT,
															disableLog,
															user
														);
														LinuxAccountHandler.disableUser(
															conn,
															source,
															invalidateList,
															disableLog,
															user
														);
													}
													break;
												case LINUX_SERVER_ACCOUNTS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_LINUX_SERVER_ACCOUNT,
															disableLog,
															userServer
														);
														LinuxAccountHandler.disableUserServer(
															conn,
															source,
															invalidateList,
															disableLog,
															userServer
														);
													}
													break;
												case MYSQL_SERVER_USERS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_MYSQL_SERVER_USER,
															disableLog,
															userServer
														);
														MysqlHandler.disableUserServer(
															conn,
															source,
															invalidateList,
															disableLog,
															userServer
														);
													}
													break;
												case MYSQL_USERS :
													{
														com.aoindustries.aoserv.client.mysql.User.Name user = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_MYSQL_USER,
															disableLog,
															user
														);
														MysqlHandler.disableUser(
															conn,
															source,
															invalidateList,
															disableLog,
															user
														);
													}
													break;
												case PACKAGES :
													{
														Account.Name name = Account.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_PACKAGE,
															disableLog,
															name
														);
														PackageHandler.disablePackage(
															conn,
															source,
															invalidateList,
															disableLog,
															name
														);
													}
													break;
												case POSTGRES_SERVER_USERS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.DISABLE_POSTGRES_SERVER_USER,
															disableLog,
															userServer
														);
														PostgresqlHandler.disableUserServer(
															conn,
															source,
															invalidateList,
															disableLog,
															userServer
														);
													}
													break;
												case POSTGRES_USERS :
													{
														com.aoindustries.aoserv.client.postgresql.User.Name user = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_POSTGRES_USER,
															disableLog,
															user
														);
														PostgresqlHandler.disableUser(
															conn,
															source,
															invalidateList,
															disableLog,
															user
														);
													}
													break;
												case USERNAMES :
													{
														com.aoindustries.aoserv.client.account.User.Name user = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.DISABLE_USERNAME,
															disableLog,
															user
														);
														AccountUserHandler.disableUser(
															conn,
															source,
															invalidateList,
															disableLog,
															user
														);
													}
													break;
												default :
													throw new IOException("Unknown table ID for disable: clientTableID="+clientTableID+", tableID="+tableID);
											}
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case DUMP_MYSQL_DATABASE :
										{
											process.setPriority(Thread.NORM_PRIORITY-1);
											currentThread.setPriority(Thread.NORM_PRIORITY-1);

											int database = in.readCompressedInt();
											boolean gzip;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
												gzip = in.readBoolean();
											} else {
												gzip = false;
											}
											process.setCommand(
												Command.DUMP_MYSQL_DATABASE,
												database,
												gzip
											);
											MysqlHandler.dumpDatabase(
												conn,
												source,
												out,
												database,
												gzip
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case DUMP_POSTGRES_DATABASE :
										{
											process.setPriority(Thread.NORM_PRIORITY-1);
											currentThread.setPriority(Thread.NORM_PRIORITY-1);

											int database = in.readCompressedInt();
											boolean gzip;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) >= 0) {
												gzip = in.readBoolean();
											} else {
												gzip = false;
											}
											process.setCommand(
												Command.DUMP_POSTGRES_DATABASE,
												database,
												gzip
											);
											PostgresqlHandler.dumpDatabase(
												conn,
												source,
												out,
												database,
												gzip
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case ENABLE :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											switch(tableID) {
												case BUSINESSES :
													{
														Account.Name account = Account.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_BUSINESS,
															account
														);
														AccountHandler.enableAccount(
															conn,
															source,
															invalidateList,
															account
														);
													}
													break;
												case BUSINESS_ADMINISTRATORS :
													{
														com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_BUSINESS_ADMINISTRATOR,
															administrator
														);
														AccountHandler.enableAdministrator(
															conn,
															source,
															invalidateList,
															administrator
														);
													}
													break;
												case CVS_REPOSITORIES :
													{
														int cvsRepository = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_CVS_REPOSITORY,
															cvsRepository
														);
														CvsHandler.enableCvsRepository(
															conn,
															source,
															invalidateList,
															cvsRepository
														);
													}
													break;
												case EMAIL_LISTS :
													{
														int list = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_EMAIL_LIST,
															list
														);
														EmailHandler.enableList(
															conn,
															source,
															invalidateList,
															list
														);
													}
													break;
												case EMAIL_PIPES :
													{
														int pipe = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_EMAIL_PIPE,
															pipe
														);
														EmailHandler.enablePipe(
															conn,
															source,
															invalidateList,
															pipe
														);
													}
													break;
												case EMAIL_SMTP_RELAYS :
													{
														int smtpRelay = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_EMAIL_SMTP_RELAY,
															smtpRelay
														);
														EmailHandler.enableSmtpRelay(
															conn,
															source,
															invalidateList,
															smtpRelay
														);
													}
													break;
												case HTTPD_SHARED_TOMCATS :
													{
														int sharedTomcat = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_HTTPD_SHARED_TOMCAT,
															sharedTomcat
														);
														WebHandler.enableSharedTomcat(
															conn,
															source,
															invalidateList,
															sharedTomcat
														);
													}
													break;
												case HTTPD_SITES :
													{
														int site = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_HTTPD_SITE,
															site
														);
														WebHandler.enableSite(
															conn,
															source,
															invalidateList,
															site
														);
													}
													break;
												case HTTPD_SITE_BINDS :
													{
														int virtualHost = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_HTTPD_SITE_BIND,
															virtualHost
														);
														WebHandler.enableVirtualHost(
															conn,
															source,
															invalidateList,
															virtualHost
														);
													}
													break;
												case LINUX_ACCOUNTS :
													{
														com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_LINUX_ACCOUNT,
															user
														);
														LinuxAccountHandler.enableUser(
															conn,
															source,
															invalidateList,
															user
														);
													}
													break;
												case LINUX_SERVER_ACCOUNTS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_LINUX_SERVER_ACCOUNT,
															userServer
														);
														LinuxAccountHandler.enableUserServer(
															conn,
															source,
															invalidateList,
															userServer
														);
													}
													break;
												case MYSQL_SERVER_USERS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_MYSQL_SERVER_USER,
															userServer
														);
														MysqlHandler.enableUserServer(
															conn,
															source,
															invalidateList,
															userServer
														);
													}
													break;
												case MYSQL_USERS :
													{
														com.aoindustries.aoserv.client.mysql.User.Name user = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_MYSQL_USER,
															user
														);
														MysqlHandler.enableUser(
															conn,
															source,
															invalidateList,
															user
														);
													}
													break;
												case PACKAGES :
													{
														Account.Name name = Account.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_PACKAGE,
															name
														);
														PackageHandler.enablePackage(
															conn,
															source,
															invalidateList,
															name
														);
													}
													break;
												case POSTGRES_SERVER_USERS :
													{
														int userServer = in.readCompressedInt();
														process.setCommand(
															Command.ENABLE_POSTGRES_SERVER_USER,
															userServer
														);
														PostgresqlHandler.enableUserServer(
															conn,
															source,
															invalidateList,
															userServer
														);
													}
													break;
												case POSTGRES_USERS :
													{
														com.aoindustries.aoserv.client.postgresql.User.Name user = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_POSTGRES_USER,
															user
														);
														PostgresqlHandler.enableUser(
															conn,
															source,
															invalidateList,
															user
														);
													}
													break;
												case USERNAMES :
													{
														com.aoindustries.aoserv.client.account.User.Name user = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
														process.setCommand(
															Command.ENABLE_USERNAME,
															user
														);
														AccountUserHandler.enableUser(
															conn,
															source,
															invalidateList,
															user
														);
													}
													break;
												default :
													throw new IOException("Unknown table ID for enable: clientTableID="+clientTableID+", tableID="+tableID);
											}
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case GENERATE_ACCOUNTING_CODE :
										{
											Account.Name template = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.GENERATE_ACCOUNTING,
												template
											);
											Account.Name account = AccountHandler.generateAccountName(
												conn,
												template
											);
											resp = Response.of(
												AoservProtocol.DONE,
												account
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_MYSQL_DATABASE_NAME :
										{
											String template_base = in.readUTF().trim();
											String template_added = in.readUTF().trim();
											process.setCommand(
												Command.GENERATE_MYSQL_DATABASE_NAME,
												template_base,
												template_added
											);
											Database.Name name = MysqlHandler.generateDatabaseName(
												conn,
												template_base,
												template_added
											);
											resp = Response.of(
												AoservProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_PACKAGE_NAME :
										{
											Account.Name template = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.GENERATE_PACKAGE_NAME,
												template
											);
											Account.Name name = PackageHandler.generatePackageName(
												conn,
												template
											);
											resp = Response.of(
												AoservProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_POSTGRES_DATABASE_NAME :
										{
											String template_base = in.readUTF().trim();
											String template_added = in.readUTF().trim();
											process.setCommand(
												Command.GENERATE_POSTGRES_DATABASE_NAME,
												template_base,
												template_added
											);
											com.aoindustries.aoserv.client.postgresql.Database.Name name = PostgresqlHandler.generateDatabaseName(
												conn,
												template_base,
												template_added
											);
											resp = Response.of(
												AoservProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_SHARED_TOMCAT_NAME :
										{
											String template = in.readUTF().trim();
											process.setCommand(
												Command.GENERATE_SHARED_TOMCAT_NAME,
												template
											);
											String name = WebHandler.generateSharedTomcatName(
												conn,
												template
											);
											resp = Response.of(
												AoservProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_SITE_NAME :
										{
											String template = in.readUTF().trim();
											process.setCommand(
												Command.GENERATE_SITE_NAME,
												template
											);
											String name = WebHandler.generateSiteName(
												conn,
												template
											);
											resp = Response.of(
												AoservProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GET_ACCOUNT_BALANCE :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												"get_account_balance",
												account
											);
											BillingTransactionHandler.getAccountBalance(
												conn,
												source,
												out,
												account
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_ACCOUNT_BALANCE_BEFORE :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											long before = in.readLong();
											process.setCommand(
												"get_account_balance_before",
												account,
												new java.util.Date(before)
											);
											BillingTransactionHandler.getAccountBalanceBefore(
												conn,
												source,
												out,
												account,
												before
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_BANK_TRANSACTIONS_ACCOUNT :
										{
											boolean provideProgress = in.readBoolean();
											String account = in.readUTF().trim();
											process.setCommand(
												"get_bank_transactions_account",
												provideProgress,
												account
											);
											BankAccountHandler.getTransactionsForAccount(
												conn,
												source,
												out,
												provideProgress,
												account
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_CONFIRMED_ACCOUNT_BALANCE :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												"get_confirmed_account_balance",
												account
											);
											BillingTransactionHandler.getConfirmedAccountBalance(
												conn,
												source,
												out,
												account
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_CONFIRMED_ACCOUNT_BALANCE_BEFORE :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											long before = in.readLong();
											process.setCommand(
												"get_confirmed_account_balance_before",
												account,
												new java.util.Date(before)
											);
											BillingTransactionHandler.getConfirmedAccountBalanceBefore(
												conn,
												source,
												out,
												account,
												before
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_AUTORESPONDER_CONTENT :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_AUTORESPONDER_CONTENT,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												LinuxAccountHandler.getAutoresponderContent(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AWSTATS_FILE :
										{
											int site = in.readCompressedInt();
											String path = in.readUTF();
											String queryString = in.readUTF();
											process.setCommand(
												Command.GET_AWSTATS_FILE,
												site,
												path,
												queryString
											);
											WebHandler.getAWStatsFile(
												conn,
												source,
												site,
												path,
												queryString,
												out
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_BACKUP_PARTITION_DISK_TOTAL_SIZE :
										{
											int backupPartition = in.readCompressedInt();
											process.setCommand(
												Command.GET_BACKUP_PARTITION_TOTAL_SIZE,
												backupPartition
											);
											resp = Response.of(
												AoservProtocol.DONE,
												BackupHandler.getBackupPartitionTotalSize(
													conn,
													source,
													backupPartition
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_BACKUP_PARTITION_DISK_USED_SIZE :
										{
											int backupPartition = in.readCompressedInt();
											process.setCommand(
												Command.GET_BACKUP_PARTITION_USED_SIZE,
												backupPartition
											);
											resp = Response.of(
												AoservProtocol.DONE,
												BackupHandler.getBackupPartitionUsedSize(
													conn,
													source,
													backupPartition
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_CACHED_ROW_COUNT :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											process.setCommand(
												"get_cached_row_count",
												TableHandler.getTableName(
													conn,
													tableID
												)
											);
											int count = TableHandler.getCachedRowCount(
												conn,
												source,
												tableID
											);
											resp = Response.of(
												AoservProtocol.DONE,
												count
											);
											sendInvalidateList = false;
										}
										break;
									case GET_CRON_TABLE :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_CRON_TABLE,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												LinuxAccountHandler.getCronTable(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_EMAIL_LIST_ADDRESS_LIST :
										{
											int list = in.readCompressedInt();
											process.setCommand(
												Command.GET_EMAIL_LIST,
												list
											);
											resp = Response.of(
												AoservProtocol.DONE,
												EmailHandler.getListFile(
													conn,
													source,
													list
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_FAILOVER_FILE_LOGS_FOR_REPLICATION :
										{
											int replication = in.readCompressedInt();
											int maxRows = in.readCompressedInt();
											FailoverHandler.getFileReplicationLogs(
												conn,
												source,
												out,
												replication,
												maxRows
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_FAILOVER_FILE_REPLICATION_ACTIVITY :
										{
											int replication = in.readCompressedInt();
											process.setCommand(
												Command.GET_FAILOVER_FILE_REPLICATION_ACTIVITY,
												replication
											);
											Tuple2<Long,String> activity = FailoverHandler.getFileReplicationActivity(
												conn,
												source,
												replication
											);
											resp = Response.of(
												AoservProtocol.DONE,
												activity.getElement1(),
												activity.getElement2()
											);
											sendInvalidateList = false;
										}
										break;
									case GET_HTTPD_SERVER_CONCURRENCY :
										{
											int httpdServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_HTTPD_SERVER_CONCURRENCY,
												httpdServer
											);
											int hsConcurrency = WebHandler.getHttpdServerConcurrency(
												conn,
												source,
												httpdServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												hsConcurrency
											);
											sendInvalidateList = false;
										}
										break;
									case GET_IMAP_FOLDER_SIZES :
										{
											int userServer = in.readCompressedInt();
											int numFolders = in.readCompressedInt();
											String[] folderNames = new String[numFolders];
											for(int c=0;c<numFolders;c++) folderNames[c] = in.readUTF();
											process.setCommand(
												Command.GET_IMAP_FOLDER_SIZES,
												userServer,
												numFolders,
												folderNames
											);
											resp = Response.of(
												AoservProtocol.DONE,
												EmailHandler.getImapFolderSizes(
													conn,
													source,
													userServer,
													folderNames
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_INBOX_ATTRIBUTES :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_INBOX_ATTRIBUTES,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												EmailHandler.getInboxAttributes(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MAJORDOMO_INFO_FILE :
										{
											int majordomoList = in.readCompressedInt();
											process.setCommand(
												Command.GET_MAJORDOMO_INFO_FILE,
												majordomoList
											);
											resp = Response.of(
												AoservProtocol.DONE,
												EmailHandler.getMajordomoInfoFile(
													conn,
													source,
													majordomoList
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MAJORDOMO_INTRO_FILE :
										{
											int majordomoList = in.readCompressedInt();
											process.setCommand(
												Command.GET_MAJORDOMO_INTRO_FILE,
												majordomoList
											);
											resp = Response.of(
												AoservProtocol.DONE,
												EmailHandler.getMajordomoIntroFile(
													conn,
													source,
													majordomoList
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MASTER_ENTROPY :
										{
											int numBytes = in.readCompressedInt();
											process.setCommand(
												"get_master_entropy",
												numBytes
											);
											byte[] bytes = RandomHandler.getMasterEntropy(conn, source, numBytes);
											conn.releaseConnection();
											out.writeByte(AoservProtocol.DONE);
											out.writeCompressedInt(bytes.length);
											for(int c=0;c<bytes.length;c++) out.writeByte(bytes[c]);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_MASTER_ENTROPY_NEEDED :
										{
											process.setCommand(
												"get_master_entropy_needed"
											);
											resp = Response.of(
												AoservProtocol.DONE,
												RandomHandler.getMasterEntropyNeeded(conn, source)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MRTG_FILE :
										{
											int linuxServer = in.readCompressedInt();
											String filename = in.readUTF().trim();
											process.setCommand(
												Command.GET_MRTG_FILE,
												linuxServer,
												filename
											);
											LinuxServerHandler.getMrtgFile(
												conn,
												source,
												linuxServer,
												filename,
												out
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_MYSQL_MASTER_STATUS :
										{
											int mysqlServer = in.readCompressedInt();
											process.setCommand(
												"get_mysql_master_status",
												mysqlServer
											);
											MysqlHandler.getMasterStatus(
												conn,
												source,
												mysqlServer,
												out
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_MYSQL_SLAVE_STATUS :
										{
											int failoverMySQLReplication = in.readCompressedInt();
											process.setCommand(
												"get_mysql_slave_status",
												failoverMySQLReplication
											);
											MysqlHandler.getSlaveStatus(
												conn,
												source,
												failoverMySQLReplication,
												out
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_MYSQL_TABLE_STATUS :
										{
											int mysqlDatabase = in.readCompressedInt();
											int mysqlSlave;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_60)>=0) {
												mysqlSlave = in.readCompressedInt();
											} else {
												mysqlSlave = -1;
											}
											process.setCommand(
												"get_mysql_table_status",
												mysqlDatabase,
												mysqlSlave==-1 ? null : mysqlSlave
											);
											MysqlHandler.getTableStatus(
												conn,
												source,
												mysqlDatabase,
												mysqlSlave,
												out
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case CHECK_MYSQL_TABLES :
										{
											int mysqlDatabase = in.readCompressedInt();
											int mysqlSlave;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_60)>=0) {
												mysqlSlave = in.readCompressedInt();
											} else {
												mysqlSlave = -1;
											}
											int numTables = in.readCompressedInt();
											List<Table_Name> tableNames = new ArrayList<>(numTables);
											for(int c=0;c<numTables;c++) {
												tableNames.add(Table_Name.valueOf(in.readUTF()));
											}
											process.setCommand(
												"check_mysql_tables",
												mysqlDatabase,
												mysqlSlave==-1 ? null : mysqlSlave,
												tableNames
											);
											logSQLException = false;
											MysqlHandler.checkTables(
												conn,
												source,
												mysqlDatabase,
												mysqlSlave,
												tableNames,
												out
											);
											logSQLException = true;
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_NET_DEVICE_BONDING_REPORT :
										{
											int device = in.readCompressedInt();
											process.setCommand(
												"get_net_device_bonding_report",
												device
											);
											resp = Response.of(
												AoservProtocol.DONE,
												NetDeviceHandler.getDeviceBondingReport(
													conn,
													source,
													device
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_NET_DEVICE_STATISTICS_REPORT :
										{
											int device = in.readCompressedInt();
											process.setCommand(
												"get_net_device_statistics_report",
												device
											);
											resp = Response.of(
												AoservProtocol.DONE,
												NetDeviceHandler.getDeviceStatisticsReport(
													conn,
													source,
													device
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_3WARE_RAID_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_3ware_raid_report",
												linuxServer
											);
											String report = LinuxServerHandler.get3wareRaidReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MD_STAT_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_md_stat_report",
												linuxServer
											);
											String report = LinuxServerHandler.getMdStatReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MD_MISMATCH_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_md_mismatch_report",
												linuxServer
											);
											String report = LinuxServerHandler.getMdMismatchReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_DRBD_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_drbd_report",
												linuxServer
											);
											String report = LinuxServerHandler.getDrbdReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_LVM_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_lvm_report",
												linuxServer
											);
											String[] report = LinuxServerHandler.getLvmReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report[0],
												report[1],
												report[2]
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_HDD_TEMP_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_hdd_temp_report",
												linuxServer
											);
											String report = LinuxServerHandler.getHddTempReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_HDD_MODEL_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_hdd_model_report",
												linuxServer
											);
											String report = LinuxServerHandler.getHddModelReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_FILESYSTEMS_CSV_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_filesystems_csv_report",
												linuxServer
											);
											String report = LinuxServerHandler.getFilesystemsCsvReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_LOADAVG_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_loadavg_report",
												linuxServer
											);
											String report = LinuxServerHandler.getLoadAvgReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MEMINFO_REPORT :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_meminfo_report",
												linuxServer
											);
											String report = LinuxServerHandler.getMemInfoReport(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case AO_SERVER_CHECK_PORT :
										{
											int linuxServer = in.readCompressedInt();
											InetAddress ipAddress = InetAddress.valueOf(in.readUTF());
											Port port;
											{
												int portNum = in.readCompressedInt();
												Protocol protocol;
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_0) < 0) {
													protocol = Protocol.valueOf(in.readUTF().toUpperCase(Locale.ROOT));
												} else {
													protocol = in.readEnum(Protocol.class);
												}
												port = Port.valueOf(portNum, protocol);
											}
											String appProtocol = in.readUTF();
											String monitoringParameters = in.readUTF();
											process.setCommand(
												"ao_server_check_port",
												linuxServer,
												ipAddress,
												port,
												appProtocol,
												monitoringParameters
											);
											// Do not log any IO exception
											logIOException = false;
											String result = LinuxServerHandler.checkPort(
												conn,
												source,
												linuxServer,
												ipAddress,
												port,
												appProtocol,
												monitoringParameters
											);
											logIOException = true;
											resp = Response.of(
												AoservProtocol.DONE,
												result
											);
											sendInvalidateList = false;
										}
										break;
									case AO_SERVER_CHECK_SMTP_BLACKLIST :
										{
											int linuxServer = in.readCompressedInt();
											InetAddress sourceIp = InetAddress.valueOf(in.readUTF());
											InetAddress connectIp = InetAddress.valueOf(in.readUTF());
											process.setCommand(
												"ao_server_check_smtp_blacklist",
												linuxServer,
												sourceIp,
												connectIp
											);
											// Do not log any IO exception
											logIOException = false;
											String result = LinuxServerHandler.checkSmtpBlacklist(
												conn,
												source,
												linuxServer,
												sourceIp,
												connectIp
											);
											logIOException = true;
											resp = Response.of(
												AoservProtocol.DONE,
												result
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_SYSTEM_TIME_MILLIS :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_system_time_millis",
												linuxServer
											);
											long systemTime = LinuxServerHandler.getSystemTimeMillis(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												systemTime
											);
											sendInvalidateList = false;
										}
										break;
									case GET_UPS_STATUS :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_UPS_STATUS,
												linuxServer
											);
											String status = LinuxServerHandler.getUpsStatus(
												conn,
												source,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												status
											);
											sendInvalidateList = false;
										}
										break;
									case GET_OBJECT :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											process.setCommand(
												"get_object",
												TableHandler.getTableName(
													conn,
													tableID
												)
											);
											TableHandler.getObject(
												conn,
												source,
												in,
												out,
												tableID
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_ROOT_BUSINESS :
										{
											process.setCommand(Command.GET_ROOT_BUSINESS);
											resp = Response.of(
												AoservProtocol.DONE,
												AccountHandler.getRootAccount()
											);
											sendInvalidateList = false;
										}
										break;
									case GET_ROW_COUNT :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											int count;
											if(tableID == null) {
												logger.log(Level.WARNING, "Client table not supported: #{0}, returning 0 from get_row_count", clientTableID);
												count = 0;
											} else {
												process.setCommand(
													"get_row_count",
													TableHandler.getTableName(
														conn,
														tableID
													)
												);
												count = TableHandler.getRowCount(
													conn,
													source,
													tableID
												);
											}
											resp = Response.of(
												AoservProtocol.DONE,
												count
											);
											sendInvalidateList = false;
										}
										break;
									case GET_SPAM_EMAIL_MESSAGES_FOR_EMAIL_SMTP_RELAY :
										{
											boolean provideProgress = in.readBoolean();
											int esr = in.readCompressedInt();
											process.setCommand(
												"get_spam_email_messages_for_email_smtp_relay",
												provideProgress,
												esr
											);
											EmailHandler.getSpamMessagesForSmtpRelay(
												conn,
												source,
												out,
												provideProgress,
												esr
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TABLE :
										{
											boolean provideProgress = in.readBoolean();
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) {
												// Get the table name, if possible
												int dbTableId = TableHandler.convertClientTableIDToDBTableID(
													conn,
													source.getProtocolVersion(),
													clientTableID
												);
												String tableName = dbTableId == -1
													? null
													: TableHandler.getTableNameForDBTableID(conn, dbTableId)
												;
												if(tableName != null) {
													// Is a recognized table name, give a chance for backward compatibility
													process.setCommand(
														Command.SELECT,
														"*",
														"from",
														tableName
													);
													TableHandler.getOldTable(
														conn,
														source,
														out,
														provideProgress,
														tableName
													);
												} else {
													// Not recognized table name, write empty response
													process.setCommand(
														Command.SELECT,
														"*",
														"from",
														clientTableID
													);
													conn.releaseConnection();
													writeObjects(source, out, provideProgress, Collections.emptyList());
												}
											} else {
												if(
													tableID == Table.TableID.DISTRO_FILES
												) {
													process.setPriority(Thread.NORM_PRIORITY-1);
													currentThread.setPriority(Thread.NORM_PRIORITY-1);
												}
												process.setCommand(
													Command.SELECT,
													"*",
													"from",
													TableHandler.getTableName(
														conn,
														tableID
													)
												);
												TableHandler.getTable(
													conn,
													source,
													out,
													provideProgress,
													tableID
												);
											}
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_DETAILS :
										{
											int ticket = in.readCompressedInt();
											process.setCommand(
												"get_ticket_details",
												ticket
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getTicketDetails(
													conn,
													source,
													ticket
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_RAW_EMAIL :
										{
											int ticket = in.readCompressedInt();
											process.setCommand(
												"get_ticket_raw_email",
												ticket
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getTicketRawEmail(
													conn,
													source,
													ticket
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_INTERNAL_NOTES :
										{
											int ticket = in.readCompressedInt();
											process.setCommand(
												"get_ticket_internal_notes",
												ticket
											);
											resp = Response.ofLongString(
												AoservProtocol.DONE,
												TicketHandler.getTicketInternalNotes(
													conn,
													source,
													ticket
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_OLD_VALUE :
										{
											int action = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_old_value",
												action
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getActionOldValue(
													conn,
													source,
													action
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_NEW_VALUE :
										{
											int action = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_new_value",
												action
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getActionNewValue(
													conn,
													source,
													action
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_DETAILS :
										{
											int action = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_details",
												action
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getActionDetails(
													conn,
													source,
													action
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_RAW_EMAIL :
										{
											int action = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_raw_email",
												action
											);
											resp = Response.ofNullLongString(
												AoservProtocol.DONE,
												TicketHandler.getActionRawEmail(
													conn,
													source,
													action
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_BUSINESS :
										{
											boolean provideProgress = in.readBoolean();
											Account.Name account = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												"get_transactions_business",
												provideProgress,
												account
											);
											BillingTransactionHandler.getTransactionsForAccount(
												conn,
												source,
												out,
												provideProgress,
												account
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_BUSINESS_ADMINISTRATOR :
										{
											boolean provideProgress = in.readBoolean();
											com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
											process.setCommand(
												"get_transactions_business_administrator",
												provideProgress,
												administrator
											);
											BillingTransactionHandler.getTransactionsForAdministrator(
												conn,
												source,
												out,
												provideProgress,
												administrator
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_SEARCH :
										{
											boolean provideProgress = in.readBoolean();
											TransactionSearchCriteria criteria = new TransactionSearchCriteria();
											criteria.read(in, source.getProtocolVersion());
											process.setCommand(
												"get_transactions_search",
												provideProgress,
												"..."
											);
											BillingTransactionHandler.getTransactionsSearch(
												conn,
												source,
												out,
												provideProgress,
												criteria
											);
											resp = Response.DONE;
											sendInvalidateList=false;
										}
										break;
									case GET_WHOIS_HISTORY_WHOIS_OUTPUT :
										{
											int whoisHistoryAccount = in.readCompressedInt();
											process.setCommand(
												"get_whois_history_whois_output",
												whoisHistoryAccount
											);
											Tuple2<String,String> whoisOutput = MasterServer.getService(WhoisHistoryService.class).getWhoisHistoryOutput(
												conn,
												source,
												whoisHistoryAccount
											);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_18) <= 0) {
												String output = whoisOutput.getElement1();
												String error = whoisOutput.getElement2();
												resp = Response.of(
													AoservProtocol.DONE,
													output.isEmpty() ? error : output
												);
											} else {
												resp = Response.of(
													AoservProtocol.DONE,
													whoisOutput.getElement1(),
													whoisOutput.getElement2()
												);
											}
											sendInvalidateList = false;
										}
										break;
									/*case HOLD_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.HOLD_TICKET,
												ticketID,
												comments
											);
											TicketHandler.holdTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									/*case INITIALIZE_HTTPD_SITE_PASSWD_FILE :
										{
											int sitePKey=in.readCompressedInt();
											String username=in.readUTF().trim();
											String encPassword=in.readUTF();
											process.setCommand(
												Command.INITIALIZE_HTTPD_SITE_PASSWD_FILE,
												sitePKey,
												username,
												encPassword
											);
											HttpdHandler.initializeHttpdSitePasswdFile(
												conn,
												source,
												sitePKey,
												username,
												encPassword
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=false;
										}
										break;*/
									case IS_ACCOUNTING_AVAILABLE :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_ACCOUNTING_AVAILABLE,
												account
											);
											boolean isAvailable = AccountHandler.isAccountNameAvailable(
												conn,
												account
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET :
										{
											com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET,
												administrator
											);
											boolean isAvailable = AccountHandler.isAdministratorPasswordSet(
												conn,
												source,
												administrator
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_DNS_ZONE_AVAILABLE :
										{
											String zone = in.readUTF().trim();
											process.setCommand(
												Command.IS_DNS_ZONE_AVAILABLE,
												zone
											);
											boolean isAvailable = MasterServer.getService(DnsService.class).isDNSZoneAvailable(
												conn,
												zone
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_EMAIL_DOMAIN_AVAILABLE :
										{
											int linuxServer = in.readCompressedInt();
											DomainName domain = DomainName.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_EMAIL_DOMAIN_AVAILABLE,
												linuxServer,
												domain
											);
											boolean isAvailable = EmailHandler.isDomainAvailable(
												conn,
												source,
												linuxServer,
												domain
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_GROUP_NAME_AVAILABLE :
										{
											Group.Name name = Group.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_LINUX_GROUP_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = LinuxAccountHandler.isLinuxGroupAvailable(
												conn,
												name
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												LinuxAccountHandler.isUserServerPasswordSet(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL,
												userServer
											);
											int isManual = LinuxAccountHandler.isUserServerProcmailManual(
												conn,
												source,
												userServer
											);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_6)>=0) {
												resp = Response.of(
													AoservProtocol.DONE,
													isManual
												);
											} else {
												if(isManual==AoservProtocol.FALSE) {
													resp = Response.of(
														AoservProtocol.DONE,
														false
													);
												} else if(isManual==AoservProtocol.TRUE) {
													resp = Response.of(
														AoservProtocol.DONE,
														true
													);
												} else {
													throw new IOException("Unsupported value for AOServClient protocol < "+AoservProtocol.Version.VERSION_1_6);
												}
											}
											sendInvalidateList = false;
										}
										break;
									case IS_MYSQL_DATABASE_NAME_AVAILABLE :
										{
											Database.Name name = Database.Name.valueOf(in.readUTF());
											int mysqlServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_MYSQL_DATABASE_NAME_AVAILABLE,
												name,
												mysqlServer
											);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_4)<0) throw new IOException(Command.IS_MYSQL_DATABASE_NAME_AVAILABLE+" call not supported for AoservProtocol < "+AoservProtocol.Version.VERSION_1_4+", please upgrade AOServ Client.");
											boolean isAvailable = MysqlHandler.isDatabaseNameAvailable(
												conn,
												source,
												name,
												mysqlServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_MYSQL_SERVER_USER_PASSWORD_SET :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_MYSQL_SERVER_USER_PASSWORD_SET,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												MysqlHandler.isUserServerPasswordSet(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case IS_PACKAGE_NAME_AVAILABLE :
										{
											Account.Name name = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_PACKAGE_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = PackageHandler.isPackageNameAvailable(
												conn,
												name
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_DATABASE_NAME_AVAILABLE :
										{
											com.aoindustries.aoserv.client.postgresql.Database.Name name = com.aoindustries.aoserv.client.postgresql.Database.Name.valueOf(in.readUTF());
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_POSTGRES_DATABASE_NAME_AVAILABLE,
												name,
												postgresServer
											);
											boolean isAvailable = PostgresqlHandler.isDatabaseNameAvailable(
												conn,
												source,
												name,
												postgresServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_SERVER_USER_PASSWORD_SET :
										{
											int userServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_POSTGRES_SERVER_USER_PASSWORD_SET,
												userServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												PostgresqlHandler.isUserServerPasswordSet(
													conn,
													source,
													userServer
												)
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_SERVER_NAME_AVAILABLE :
										{
											com.aoindustries.aoserv.client.postgresql.Server.Name name = com.aoindustries.aoserv.client.postgresql.Server.Name.valueOf(in.readUTF());
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.IS_POSTGRES_SERVER_NAME_AVAILABLE,
												name,
												linuxServer
											);
											boolean isAvailable = PostgresqlHandler.isServerNameAvailable(
												conn,
												source,
												name,
												linuxServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_SHARED_TOMCAT_NAME_AVAILABLE :
										{
											String name = in.readUTF().trim();
											process.setCommand(
												Command.IS_SHARED_TOMCAT_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = WebHandler.isSharedTomcatNameAvailable(
												conn,
												name
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_USERNAME_AVAILABLE :
										{
											com.aoindustries.aoserv.client.account.User.Name name = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.IS_USERNAME_AVAILABLE,
												name
											);
											boolean isAvailable = AccountUserHandler.isUserNameAvailable(
												conn,
												name
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_SITE_NAME_AVAILABLE :
										{
											String name = in.readUTF().trim();
											process.setCommand(
												Command.IS_SITE_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = WebHandler.isSiteNameAvailable(
												conn,
												name
											);
											resp = Response.of(
												AoservProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									/*case KILL_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.KILL_TICKET,
												username,
												comments
											);
											TicketHandler.killTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case MOVE_IP_ADDRESS :
										{
											int ipAddress = in.readCompressedInt();
											int toServer = in.readCompressedInt();
											process.setCommand(
												Command.MOVE_IP_ADDRESS,
												ipAddress,
												toServer
											);
											IpAddressHandler.moveIpAddress(
												conn,
												source,
												invalidateList,
												ipAddress,
												toServer
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									/*case REACTIVATE_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.REACTIVATE_TICKET,
												ticketID,
												username,
												comments
											);
											TicketHandler.reactivateTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case REFRESH_EMAIL_SMTP_RELAY :
										{
											process.setPriority(Thread.NORM_PRIORITY + 1);
											currentThread.setPriority(Thread.NORM_PRIORITY + 1);

											int smtpRelay = in.readCompressedInt();
											long minDuration = in.readLong();
											process.setCommand(
												Command.REFRESH_EMAIL_SMTP_RELAY,
												smtpRelay,
												minDuration
											);
											EmailHandler.refreshSmtpRelay(
												conn,
												source,
												invalidateList,
												smtpRelay,
												minDuration

											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case REMOVE : {
										int clientTableID = in.readCompressedInt();
										Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
										if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
										switch(tableID) {
											case BLACKHOLE_EMAIL_ADDRESSES :
												{
													int address = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_BLACKHOLE_EMAIL_ADDRESS,
														address
													);
													EmailHandler.removeBlackholeAddress(
														conn,
														source,
														invalidateList,
														address
													);
													resp = Response.DONE;
												}
												break;
											case BUSINESS_ADMINISTRATORS :
												{
													com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_BUSINESS_ADMINISTRATOR,
														administrator
													);
													AccountHandler.removeAdministrator(
														conn,
														source,
														invalidateList,
														administrator
													);
													resp = Response.DONE;
												}
												break;
											case BUSINESS_SERVERS :
												{
													int accountHost = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_BUSINESS_SERVER,
														accountHost
													);
													AccountHandler.removeAccountHost(
														conn,
														source,
														invalidateList,
														accountHost
													);
													resp = Response.DONE;
												}
												break;
											case CREDIT_CARDS :
												{
													int creditCard = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_CREDIT_CARD,
														creditCard
													);
													PaymentHandler.removeCreditCard(
														conn,
														source,
														invalidateList,
														creditCard
													);
													resp = Response.DONE;
												}
												break;
											case CVS_REPOSITORIES :
												{
													int cvsRepository = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_CVS_REPOSITORY,
														cvsRepository
													);
													CvsHandler.removeCvsRepository(
														conn,
														source,
														invalidateList,
														cvsRepository
													);
													resp = Response.DONE;
												}
												break;
											case DNS_RECORDS :
												{
													int record = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_DNS_RECORD,
														record
													);
													MasterServer.getService(DnsService.class).removeRecord(
														conn,
														source,
														invalidateList,
														record
													);
													resp = Response.DONE;
												}
												break;
											case DNS_ZONES :
												{
													String zone = in.readUTF().trim();
													process.setCommand(
														Command.REMOVE_DNS_ZONE,
														zone
													);
													MasterServer.getService(DnsService.class).removeDNSZone(
														conn,
														source,
														invalidateList,
														zone
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_ADDRESSES :
												{
													int address = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_ADDRESS,
														address
													);
													EmailHandler.removeAddress(
														conn,
														source,
														invalidateList,
														address
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_DOMAINS :
												{
													int domain = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_DOMAIN,
														domain
													);
													EmailHandler.removeDomain(
														conn,
														source,
														invalidateList,
														domain
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_FORWARDING :
												{
													int forwarding = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_FORWARDING,
														forwarding
													);
													EmailHandler.removeForwarding(
														conn,
														source,
														invalidateList,
														forwarding
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_LIST_ADDRESSES :
												{
													int listAddress = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_LIST_ADDRESS,
														listAddress
													);
													EmailHandler.removeListAddress(
														conn,
														source,
														invalidateList,
														listAddress
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_LISTS :
												{
													int list = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_LIST,
														list
													);
													EmailHandler.removeList(
														conn,
														source,
														invalidateList,
														list
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_PIPE_ADDRESSES :
												{
													int pipeAddress = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_PIPE_ADDRESS,
														pipeAddress
													);
													EmailHandler.removePipeAddress(
														conn,
														source,
														invalidateList,
														pipeAddress
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_PIPES :
												{
													int pipe = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_PIPE,
														pipe
													);
													EmailHandler.removePipe(
														conn,
														source,
														invalidateList,
														pipe
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_SMTP_RELAYS :
												{
													process.setPriority(Thread.NORM_PRIORITY + 1);
													currentThread.setPriority(Thread.NORM_PRIORITY + 1);

													int smtpRelay = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_EMAIL_SMTP_RELAY,
														smtpRelay
													);
													EmailHandler.removeSmtpRelay(
														conn,
														source,
														invalidateList,
														smtpRelay
													);
													resp = Response.DONE;
												}
												break;
											case FILE_BACKUP_SETTINGS :
												{
													int fileReplicationSetting = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_FILE_BACKUP_SETTING,
														fileReplicationSetting
													);
													BackupHandler.removeFileReplicationSetting(
														conn,
														source,
														invalidateList,
														fileReplicationSetting
													);
													resp = Response.DONE;
												}
												break;
											case FTP_GUEST_USERS :
												{
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_FTP_GUEST_USER,
														user
													);
													FTPHandler.removeGuestUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SHARED_TOMCATS :
												{
													int sharedTomcat = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_SHARED_TOMCAT,
														sharedTomcat
													);
													WebHandler.removeSharedTomcat(
														conn,
														source,
														invalidateList,
														sharedTomcat
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
												{
													int location = in.readCompressedInt();
													process.setCommand(
														"remove_httpd_site_authenticated_location",
														location
													);
													WebHandler.removeLocation(
														conn,
														source,
														invalidateList,
														location
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITES :
												{
													int site = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_SITE,
														site
													);
													WebHandler.removeSite(
														conn,
														source,
														invalidateList,
														site
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITE_URLS :
												{
													int virtualHostName = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_SITE_URL,
														virtualHostName
													);
													WebHandler.removeVirtualHostName(
														conn,
														source,
														invalidateList,
														virtualHostName
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_CONTEXTS :
												{
													int context = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_TOMCAT_CONTEXT,
														context
													);
													WebHandler.removeContext(
														conn,
														source,
														invalidateList,
														context
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_DATA_SOURCES :
												{
													int contextDataSource = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_TOMCAT_DATA_SOURCE,
														contextDataSource
													);
													WebHandler.removeContextDataSource(
														conn,
														source,
														invalidateList,
														contextDataSource
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_PARAMETERS :
												{
													int contextParameter = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_TOMCAT_PARAMETER,
														contextParameter
													);
													WebHandler.removeContextParameter(
														conn,
														source,
														invalidateList,
														contextParameter
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_SITE_JK_MOUNTS :
												{
													int jkMount = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_HTTPD_TOMCAT_SITE_JK_MOUNT,
														jkMount
													);
													WebHandler.removeJkMount(
														conn,
														source,
														invalidateList,
														jkMount
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_ACC_ADDRESSES :
												{
													int inboxAddress = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_LINUX_ACC_ADDRESS,
														inboxAddress
													);
													EmailHandler.removeInboxAddress(
														conn,
														source,
														invalidateList,
														inboxAddress
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_ACCOUNTS :
												{
													com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_LINUX_ACCOUNT,
														user
													);
													LinuxAccountHandler.removeUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_GROUP_ACCOUNTS :
												{
													int groupUser = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_LINUX_GROUP_ACCOUNT,
														groupUser
													);
													LinuxAccountHandler.removeGroupUser(
														conn,
														source,
														invalidateList,
														groupUser
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_GROUPS :
												{
													Group.Name name = Group.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_LINUX_GROUP,
														name
													);
													LinuxAccountHandler.removeGroup(
														conn,
														source,
														invalidateList,
														name
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_SERVER_ACCOUNTS :
												{
													int userServer = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_LINUX_SERVER_ACCOUNT,
														userServer
													);
													LinuxAccountHandler.removeUserServer(
														conn,
														source,
														invalidateList,
														userServer
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_SERVER_GROUPS :
												{
													int groupServer = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_LINUX_SERVER_GROUP,
														groupServer
													);
													LinuxAccountHandler.removeGroupServer(
														conn,
														source,
														invalidateList,
														groupServer
													);
													resp = Response.DONE;
												}
												break;
											case MAJORDOMO_SERVERS :
												{
													int domain = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_MAJORDOMO_SERVER,
														domain
													);
													EmailHandler.removeMajordomoServer(
														conn,
														source,
														invalidateList,
														domain
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_DATABASES :
												{
													int database = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_MYSQL_DATABASE,
														database
													);
													MysqlHandler.removeDatabase(
														conn,
														source,
														invalidateList,
														database
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_DB_USERS :
												{
													int databaseUser = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_MYSQL_DB_USER,
														databaseUser
													);
													MysqlHandler.removeDatabaseUser(
														conn,
														source,
														invalidateList,
														databaseUser
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_SERVER_USERS :
												{
													int userServer = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_MYSQL_SERVER_USER,
														userServer
													);
													MysqlHandler.removeUserServer(
														conn,
														source,
														invalidateList,
														userServer
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_USERS :
												{
													com.aoindustries.aoserv.client.mysql.User.Name user = com.aoindustries.aoserv.client.mysql.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_MYSQL_USER,
														user
													);
													MysqlHandler.removeUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case NET_BINDS :
												{
													int bind = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_NET_BIND,
														bind
													);
													NetBindHandler.removeBind(
														conn,
														source,
														invalidateList,
														bind
													);
													resp = Response.DONE;
												}
												break;
											case PACKAGE_DEFINITIONS :
												{
													int packageDefinition = in.readCompressedInt();
													process.setCommand(
														"remove_package_definition",
														packageDefinition
													);
													PackageHandler.removePackageDefinition(
														conn,
														source,
														invalidateList,
														packageDefinition
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_DATABASES :
												{
													int database = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_POSTGRES_DATABASE,
														database
													);
													PostgresqlHandler.removeDatabase(
														conn,
														source,
														invalidateList,
														database
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_SERVER_USERS :
												{
													int userServer = in.readCompressedInt();
													process.setCommand(
														Command.REMOVE_POSTGRES_SERVER_USER,
														userServer
													);
													PostgresqlHandler.removeUserServer(
														conn,
														source,
														invalidateList,
														userServer
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_USERS :
												{
													com.aoindustries.aoserv.client.postgresql.User.Name user = com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_POSTGRES_USER,
														user
													);
													PostgresqlHandler.removeUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											case USERNAMES :
												{
													com.aoindustries.aoserv.client.account.User.Name user = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
													process.setCommand(
														Command.REMOVE_USERNAME,
														user
													);
													AccountUserHandler.removeUser(
														conn,
														source,
														invalidateList,
														user
													);
													resp = Response.DONE;
												}
												break;
											default :
												throw new IOException("Unknown table ID for remove: clientTableID=" + clientTableID + ", tableID=" + tableID);
										}
										sendInvalidateList = true;
										break;
									}
									case REQUEST_REPLICATION_DAEMON_ACCESS :
										{
											int fileReplication = in.readCompressedInt();
											process.setCommand(
												"request_replication_daemon_access",
												fileReplication
											);
											Server.DaemonAccess daemonAccess = FailoverHandler.requestReplicationDaemonAccess(
												conn,
												source,
												fileReplication
											);
											resp = Response.of(
												AoservProtocol.DONE,
												daemonAccess.getProtocol(),
												daemonAccess.getHost(),
												daemonAccess.getPort().getPort(),
												daemonAccess.getKey()
											);
											sendInvalidateList = false;
										}
										break;
									case RESTART_APACHE :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.RESTART_APACHE,
												linuxServer
											);
											WebHandler.restartApache(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_CRON :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.RESTART_CRON,
												linuxServer
											);
											LinuxServerHandler.restartCron(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_MYSQL :
										{
											int mysqlServer = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_4) < 0) {
												throw new IOException(Command.RESTART_MYSQL + " call not supported for AOServ Client version < " + AoservProtocol.Version.VERSION_1_4 + ", please upgrade AOServ Client.");
											}
											process.setCommand(
												Command.RESTART_MYSQL,
												mysqlServer
											);
											MysqlHandler.restartServer(
												conn,
												source,
												mysqlServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_POSTGRESQL :
										{
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												Command.RESTART_POSTGRESQL,
												postgresServer
											);
											PostgresqlHandler.restartServer(
												conn,
												source,
												postgresServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_XFS :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.RESTART_XFS,
												linuxServer
											);
											LinuxServerHandler.restartXfs(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_XVFB :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.RESTART_XVFB,
												linuxServer
											);
											LinuxServerHandler.restartXvfb(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_AUTORESPONDER :
										{
											int userServer = in.readCompressedInt();
											int from = in.readCompressedInt();
											String subject = in.readNullUTF();
											String content = in.readNullUTF();
											boolean enabled = in.readBoolean();
											process.setCommand(
												Command.SET_AUTORESPONDER,
												userServer,
												from==-1?null:from,
												subject,
												content,
												enabled
											);
											LinuxAccountHandler.setAutoresponder(
												conn,
												source,
												invalidateList,
												userServer,
												from,
												subject,
												content,
												enabled
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_BUSINESS_ACCOUNTING :
										{
											Account.Name oldAccounting = Account.Name.valueOf(in.readUTF());
											Account.Name newAccounting = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_BUSINESS_ACCOUNTING,
												oldAccounting,
												newAccounting
											);
											AccountHandler.setAccountName(
												conn,
												source,
												invalidateList,
												oldAccounting,
												newAccounting
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_BUSINESS_ADMINISTRATOR_PASSWORD :
										{
											com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
											String password = in.readUTF();
											process.setCommand(
												Command.SET_BUSINESS_ADMINISTRATOR_PASSWORD,
												administrator,
												AoservProtocol.FILTERED
											);
											AccountHandler.setAdministratorPassword(
												conn,
												source,
												invalidateList,
												administrator,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_BUSINESS_ADMINISTRATOR_PROFILE :
										{
											com.aoindustries.aoserv.client.account.User.Name administrator = com.aoindustries.aoserv.client.account.User.Name.valueOf(in.readUTF());
											String name=in.readUTF().trim();
											String title=in.readNullUTF();
											long birthdayLong=in.readLong();
											Date birthday = birthdayLong==-1 ? null : new Date(birthdayLong);
											boolean isPrivate=in.readBoolean();
											String workPhone=in.readUTF().trim();
											String homePhone=in.readNullUTF();
											String cellPhone=in.readNullUTF();
											String fax=in.readNullUTF();
											String email=in.readUTF().trim();
											String address1=in.readNullUTF();
											String address2=in.readNullUTF();
											String city=in.readNullUTF();
											String state=in.readNullUTF();
											String country=in.readNullUTF();
											String zip=in.readNullUTF();
											process.setCommand(
												Command.SET_BUSINESS_ADMINISTRATOR_PROFILE,
												administrator,
												name,
												title,
												birthday,
												isPrivate,
												workPhone,
												homePhone,
												cellPhone,
												fax,
												email,
												address1,
												address2,
												city,
												state,
												country,
												zip
											);
											AccountHandler.setAdministratorProfile(
												conn,
												source,
												invalidateList,
												administrator,
												name,
												title,
												birthday,
												isPrivate,
												workPhone,
												homePhone,
												cellPhone,
												fax,
												email,
												address1,
												address2,
												city,
												state,
												country,
												zip
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_CRON_TABLE :
										{
											int userServer = in.readCompressedInt();
											String crontab = in.readUTF();
											process.setCommand(
												Command.SET_CRON_TABLE,
												userServer,
												crontab
											);
											LinuxAccountHandler.setCronTable(
												conn,
												source,
												userServer,
												crontab
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_CVS_REPOSITORY_MODE :
										{
											int cvsRepository = in.readCompressedInt();
											long mode = in.readLong();
											process.setCommand(
												Command.SET_CVS_REPOSITORY_MODE,
												cvsRepository,
												Long.toOctalString(mode)
											);
											CvsHandler.setMode(
												conn,
												source,
												invalidateList,
												cvsRepository,
												mode
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_DEFAULT_BUSINESS_SERVER :
										{
											int accountHost = in.readCompressedInt();
											process.setCommand(
												Command.SET_DEFAULT_BUSINESS_SERVER,
												accountHost
											);
											AccountHandler.setDefaultAccountHost(
												conn,
												source,
												invalidateList,
												accountHost
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_DNS_ZONE_TTL :
										{
											String zone = in.readUTF();
											int ttl = in.readCompressedInt();
											process.setCommand(
												Command.SET_DNS_ZONE_TTL,
												zone,
												ttl
											);
											MasterServer.getService(DnsService.class).setDNSZoneTTL(
												conn,
												source,
												invalidateList,
												zone,
												ttl
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_EMAIL_LIST_ADDRESS_LIST :
										{
											int list = in.readCompressedInt();
											String addresses = in.readUTF();
											process.setCommand(
												Command.SET_EMAIL_LIST,
												list,
												addresses
											);
											EmailHandler.setListFile(
												conn,
												source,
												list,
												addresses
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_FILE_BACKUP_SETTINGS :
										{
											int fileReplicationSetting = in.readCompressedInt();
											String path = in.readUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
												in.readCompressedInt(); // package
											}
											boolean backupEnabled;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_30)<=0) {
												short backupLevel=in.readShort();
												in.readShort(); // backup_retention
												in.readBoolean(); // recurse
												backupEnabled = backupLevel>0;
											} else {
												backupEnabled = in.readBoolean();
											}
											boolean required;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_62)>=0) {
												required = in.readBoolean();
											} else {
												required = false;
											}
											process.setCommand(
												Command.SET_FILE_BACKUP_SETTING,
												fileReplicationSetting,
												path,
												backupEnabled,
												required
											);
											BackupHandler.setFileReplicationSettings(
												conn,
												source,
												invalidateList,
												fileReplicationSetting,
												path,
												backupEnabled,
												required
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_FILE_BACKUP_SETTINGS_ALL_AT_ONCE :
										{
											int replication = in.readCompressedInt();
											int size = in.readCompressedInt();
											List<String> paths = new ArrayList<>(size);
											List<Boolean> backupEnableds = new ArrayList<>(size);
											List<Boolean> requireds = new ArrayList<>(size);
											for(int c=0;c<size;c++) {
												paths.add(in.readUTF());
												backupEnableds.add(in.readBoolean());
												boolean required;
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_62)>=0) {
													required = in.readBoolean();
												} else {
													required = false;
												}
												requireds.add(required);
											}

											process.setCommand(
												"set_file_backup_settings_all_at_once",
												replication,
												size
											);
											FailoverHandler.setFileReplicationSettings(
												conn,
												source,
												invalidateList,
												replication,
												paths,
												backupEnableds,
												requireds
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SHARED_TOMCAT_IS_MANUAL :
										{
											int sharedTomcat = in.readCompressedInt();
											boolean isManual = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SHARED_TOMCAT_IS_MANUAL,
												sharedTomcat,
												isManual
											);
											WebHandler.setSharedTomcatIsManual(
												conn,
												source,
												invalidateList,
												sharedTomcat,
												isManual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SHARED_TOMCAT_MAX_POST_SIZE :
										{
											int sharedTomcat = in.readCompressedInt();
											int maxPostSize = in.readInt();
											process.setCommand(
												Command.SET_HTTPD_SHARED_TOMCAT_MAX_POST_SIZE,
												sharedTomcat,
												maxPostSize
											);
											WebHandler.setSharedTomcatMaxPostSize(
												conn,
												source,
												invalidateList,
												sharedTomcat,
												maxPostSize
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SHARED_TOMCAT_UNPACK_WARS :
										{
											int sharedTomcat = in.readCompressedInt();
											boolean unpackWARs = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SHARED_TOMCAT_UNPACK_WARS,
												sharedTomcat,
												unpackWARs
											);
											WebHandler.setSharedTomcatUnpackWARs(
												conn,
												source,
												invalidateList,
												sharedTomcat,
												unpackWARs
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SHARED_TOMCAT_AUTO_DEPLOY :
										{
											int sharedTomcat = in.readCompressedInt();
											boolean autoDeploy = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SHARED_TOMCAT_AUTO_DEPLOY,
												sharedTomcat,
												autoDeploy
											);
											WebHandler.setSharedTomcatAutoDeploy(
												conn,
												source,
												invalidateList,
												sharedTomcat,
												autoDeploy
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SHARED_TOMCAT_VERSION :
										{
											int sharedTomcat = in.readCompressedInt();
											int version = in.readCompressedInt();
											process.setCommand(
												Command.SET_HTTPD_SHARED_TOMCAT_VERSION,
												sharedTomcat,
												version
											);
											WebHandler.setSharedTomcatVersion(
												conn,
												source,
												invalidateList,
												sharedTomcat,
												version
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_AUTHENTICATED_LOCATION_ATTRIBUTES :
										{
											int location = in.readCompressedInt();
											String path = in.readUTF().trim();
											boolean isRegularExpression = in.readBoolean();
											String authName = in.readUTF().trim();
											PosixPath authGroupFile;
											{
												String s = in.readUTF().trim();
												authGroupFile = s.isEmpty() ? null : PosixPath.valueOf(s);
											}
											PosixPath authUserFile;
											{
												String s = in.readUTF().trim();
												authUserFile = s.isEmpty() ? null : PosixPath.valueOf(s);
											}
											String require = in.readUTF().trim();
											String handler;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_13) >= 0) {
												String s = in.readUTF();
												handler = s.isEmpty() ? null : s;
											} else {
												// Keep current value
												handler = Location.Handler.CURRENT;
											}
											process.setCommand(
												Command.SET_HTTPD_SITE_AUTHENTICATED_LOCATION_ATTRIBUTES,
												location,
												path,
												isRegularExpression,
												authName,
												authGroupFile,
												authUserFile,
												require,
												handler
											);
											WebHandler.setLocationAttributes(
												conn,
												source,
												invalidateList,
												location,
												path,
												isRegularExpression,
												authName,
												authGroupFile,
												authUserFile,
												require,
												handler
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_IS_MANUAL :
										{
											int virtualHost = in.readCompressedInt();
											boolean isManual = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BIND_IS_MANUAL,
												virtualHost,
												isManual
											);
											WebHandler.setVirtualHostIsManual(
												conn,
												source,
												invalidateList,
												virtualHost,
												isManual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME :
										{
											int virtualHost = in.readCompressedInt();
											boolean redirectToPrimaryHostname = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME,
												virtualHost,
												redirectToPrimaryHostname
											);
											WebHandler.setVirtualHostRedirectToPrimaryHostname(
												conn,
												source,
												invalidateList,
												virtualHost,
												redirectToPrimaryHostname
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_IS_MANUAL :
										{
											int site = in.readCompressedInt();
											boolean isManual = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_IS_MANUAL,
												site,
												isManual
											);
											WebHandler.setSiteIsManual(
												conn,
												source,
												invalidateList,
												site,
												isManual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_SERVER_ADMIN :
										{
											int site = in.readCompressedInt();
											Email emailAddress = Email.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_HTTPD_SITE_SERVER_ADMIN,
												site,
												emailAddress
											);
											WebHandler.setSiteServerAdmin(
												conn,
												source,
												invalidateList,
												site,
												emailAddress
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_PHP_VERSION :
										{
											int site = in.readCompressedInt();
											int phpVersion = in.readCompressedInt();
											process.setCommand(
												Command.SET_HTTPD_SITE_PHP_VERSION,
												site,
												phpVersion
											);
											WebHandler.setSitePhpVersion(
												conn,
												source,
												invalidateList,
												site,
												phpVersion
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_CGI :
										{
											int site = in.readCompressedInt();
											boolean enableCgi = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_CGI,
												site,
												enableCgi
											);
											WebHandler.setSiteEnableCgi(
												conn,
												source,
												invalidateList,
												site,
												enableCgi
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_SSI :
										{
											int site = in.readCompressedInt();
											boolean enableSsi = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_SSI,
												site,
												enableSsi
											);
											WebHandler.setSiteEnableSsi(
												conn,
												source,
												invalidateList,
												site,
												enableSsi
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_HTACCESS :
										{
											int site = in.readCompressedInt();
											boolean enableHtaccess = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_HTACCESS,
												site,
												enableHtaccess
											);
											WebHandler.setSiteEnableHtaccess(
												conn,
												source,
												invalidateList,
												site,
												enableHtaccess
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_INDEXES :
										{
											int site = in.readCompressedInt();
											boolean enableIndexes = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_INDEXES,
												site,
												enableIndexes
											);
											WebHandler.setSiteEnableIndexes(
												conn,
												source,
												invalidateList,
												site,
												enableIndexes
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_FOLLOW_SYMLINKS :
										{
											int site = in.readCompressedInt();
											boolean enableFollowSymlinks = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_FOLLOW_SYMLINKS,
												site,
												enableFollowSymlinks
											);
											WebHandler.setSiteEnableFollowSymlinks(
												conn,
												source,
												invalidateList,
												site,
												enableFollowSymlinks
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_ENABLE_ANONYMOUS_FTP :
										{
											int site = in.readCompressedInt();
											boolean enableAnonymousFtp = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_ENABLE_ANONYMOUS_FTP,
												site,
												enableAnonymousFtp
											);
											WebHandler.setSiteEnableAnonymousFtp(
												conn,
												source,
												invalidateList,
												site,
												enableAnonymousFtp
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BLOCK_TRACE_TRACK :
										{
											int site = in.readCompressedInt();
											boolean blockTraceTrack = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BLOCK_TRACE_TRACK,
												site,
												blockTraceTrack
											);
											WebHandler.setSiteBlockTraceTrack(
												conn,
												source,
												invalidateList,
												site,
												blockTraceTrack
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BLOCK_SCM :
										{
											int site = in.readCompressedInt();
											boolean blockScm = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BLOCK_SCM,
												site,
												blockScm
											);
											WebHandler.setSiteBlockScm(
												conn,
												source,
												invalidateList,
												site,
												blockScm
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BLOCK_CORE_DUMPS :
										{
											int site = in.readCompressedInt();
											boolean blockCoreDumps = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BLOCK_CORE_DUMPS,
												site,
												blockCoreDumps
											);
											WebHandler.setSiteBlockCoreDumps(
												conn,
												source,
												invalidateList,
												site,
												blockCoreDumps
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BLOCK_EDITOR_BACKUPS :
										{
											int site = in.readCompressedInt();
											boolean blockEditorBackups = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_SITE_BLOCK_EDITOR_BACKUPS,
												site,
												blockEditorBackups
											);
											WebHandler.setSiteBlockEditorBackups(
												conn,
												source,
												invalidateList,
												site,
												blockEditorBackups
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_PREDISABLE_CONFIG :
										{
											int virtualHost = in.readCompressedInt();
											String config = in.readNullUTF();
											process.setCommand(
												"set_httpd_site_bind_predisable_config",
												virtualHost,
												AoservProtocol.FILTERED
											);
											WebHandler.setVirtualHostPredisableConfig(
												conn,
												source,
												invalidateList,
												virtualHost,
												config
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES :
										{
											int context = in.readCompressedInt();
											String className = in.readNullUTF();
											boolean cookies = in.readBoolean();
											boolean crossContext = in.readBoolean();
											PosixPath docBase = PosixPath.valueOf(in.readUTF());
											boolean override = in.readBoolean();
											String path = in.readUTF().trim();
											boolean privileged = in.readBoolean();
											boolean reloadable = in.readBoolean();
											boolean useNaming = in.readBoolean();
											String wrapperClass = in.readNullUTF();
											int debug = in.readCompressedInt();
											PosixPath workDir = PosixPath.valueOf(in.readNullUTF());
											boolean serverXmlConfigured;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_2) <= 0) {
												serverXmlConfigured = Context.DEFAULT_SERVER_XML_CONFIGURED;
											} else {
												serverXmlConfigured = in.readBoolean();
											}
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES,
												context,
												className,
												cookies,
												crossContext,
												docBase,
												override,
												path,
												privileged,
												reloadable,
												useNaming,
												wrapperClass,
												debug,
												workDir,
												serverXmlConfigured
											);
											WebHandler.setContextAttributes(
												conn,
												source,
												invalidateList,
												context,
												className,
												cookies,
												crossContext,
												docBase,
												override,
												path,
												privileged,
												reloadable,
												useNaming,
												wrapperClass,
												debug,
												workDir,
												serverXmlConfigured
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_SITE_BLOCK_WEBINF :
										{
											int tomcatSite = in.readCompressedInt();
											boolean blockWebinf = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_SITE_BLOCK_WEBINF,
												tomcatSite,
												blockWebinf
											);
											WebHandler.setTomcatSiteBlockWebinf(
												conn,
												source,
												invalidateList,
												tomcatSite,
												blockWebinf
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_STD_SITE_MAX_POST_SIZE :
										{
											int privateTomcatSite = in.readCompressedInt();
											int maxPostSize = in.readInt();
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_STD_SITE_MAX_POST_SIZE,
												privateTomcatSite,
												maxPostSize
											);
											WebHandler.setPrivateTomcatSiteMaxPostSize(
												conn,
												source,
												invalidateList,
												privateTomcatSite,
												maxPostSize
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_STD_SITE_UNPACK_WARS :
										{
											int privateTomcatSite = in.readCompressedInt();
											boolean unpackWARs = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_STD_SITE_UNPACK_WARS,
												privateTomcatSite,
												unpackWARs
											);
											WebHandler.setPrivateTomcatSiteUnpackWARs(
												conn,
												source,
												invalidateList,
												privateTomcatSite,
												unpackWARs
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_STD_SITE_AUTO_DEPLOY :
										{
											int privateTomcatSite = in.readCompressedInt();
											boolean autoDeploy = in.readBoolean();
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_STD_SITE_AUTO_DEPLOY,
												privateTomcatSite,
												autoDeploy
											);
											WebHandler.setPrivateTomcatSiteAutoDeploy(
												conn,
												source,
												invalidateList,
												privateTomcatSite,
												autoDeploy
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_STD_SITE_VERSION :
										{
											int privateTomcatSite = in.readCompressedInt();
											int version = in.readCompressedInt();
											process.setCommand(
												Command.SET_HTTPD_TOMCAT_STD_SITE_VERSION,
												privateTomcatSite,
												version
											);
											WebHandler.setPrivateTomcatSiteVersion(
												conn,
												source,
												invalidateList,
												privateTomcatSite,
												version
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_IP_ADDRESS_DHCP_ADDRESS :
										{
											int ipAddress = in.readCompressedInt();
											InetAddress dhcpAddress = InetAddress.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_IP_ADDRESS_DHCP_ADDRESS,
												ipAddress,
												dhcpAddress
											);
											IpAddressHandler.setDhcpAddressDestination(
												conn,
												source,
												invalidateList,
												ipAddress,
												dhcpAddress
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_IP_ADDRESS_HOSTNAME :
										{
											int ipAddress = in.readCompressedInt();
											DomainName hostname = DomainName.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_IP_ADDRESS_HOSTNAME,
												ipAddress,
												hostname
											);
											IpAddressHandler.setIpAddressHostname(
												conn,
												source,
												invalidateList,
												ipAddress,
												hostname
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_IP_ADDRESS_MONITORING_ENABLED :
										{
											int ipAddress = in.readCompressedInt();
											boolean enabled = in.readBoolean();
											process.setCommand(
												Command.SET_IP_ADDRESS_MONITORING_ENABLED,
												ipAddress,
												enabled
											);
											IpAddressHandler.setIpAddressMonitoringEnabled(
												conn,
												source,
												invalidateList,
												ipAddress,
												enabled
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_IP_ADDRESS_PACKAGE :
										{
											int ipAddress = in.readCompressedInt();
											Account.Name packageName = Account.Name.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_IP_ADDRESS_PACKAGE,
												ipAddress,
												packageName
											);
											IpAddressHandler.setIpAddressPackage(
												conn,
												source,
												invalidateList,
												ipAddress,
												packageName
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case ADD_IP_REPUTATION :
										{
											int ipReputationSet = in.readCompressedInt();
											int size = in.readCompressedInt();
											AddReputation[] addReputations = new AddReputation[size];
											for(int i=0; i<size; i++) {
												int            host           = in.readInt();
												ConfidenceType confidence     = ConfidenceType.fromChar(in.readChar());
												ReputationType reputationType = ReputationType.fromChar(in.readChar());
												short          score          = in.readShort();
												addReputations[i] = new AddReputation(
													host,
													confidence,
													reputationType,
													score
												);
											}
											process.setCommand(
												Command.ADD_IP_REPUTATION,
												ipReputationSet,
												size
											);
											NetReputationSetHandler.addIpReputation(
												conn,
												source,
												invalidateList,
												ipReputationSet,
												addReputations
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LAST_DISTRO_TIME :
										{
											process.setPriority(Thread.MIN_PRIORITY+1);
											currentThread.setPriority(Thread.MIN_PRIORITY+1);

											int linuxServer = in.readCompressedInt();
											long time = in.readLong();
											process.setCommand(
												"set_last_distro_time",
												linuxServer,
												new java.util.Date(time)
											);
											LinuxServerHandler.setLastDistroTime(
												conn,
												source,
												invalidateList,
												linuxServer,
												time
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_HOME_PHONE :
										{
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											Gecos phone;
											{
												String s = in.readUTF();
												phone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_HOME_PHONE,
												user,
												phone
											);
											LinuxAccountHandler.setUserHomePhone(
												conn,
												source,
												invalidateList,
												user,
												phone
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_NAME :
										{
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											Gecos fullName;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_80_1) < 0) {
												fullName = Gecos.valueOf(in.readUTF());
											} else {
												String s = in.readUTF();
												fullName = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_NAME,
												user,
												fullName
											);
											LinuxAccountHandler.setUserFullName(
												conn,
												source,
												invalidateList,
												user,
												fullName
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_OFFICE_LOCATION :
										{
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											Gecos location;
											{
												String s = in.readUTF();
												location = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_OFFICE_LOCATION,
												user,
												location
											);
											LinuxAccountHandler.setUserOfficeLocation(
												conn,
												source,
												invalidateList,
												user,
												location
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_OFFICE_PHONE :
										{
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											Gecos phone;
											{
												String s = in.readUTF();
												phone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_OFFICE_PHONE,
												user,
												phone
											);
											LinuxAccountHandler.setUserOfficePhone(
												conn,
												source,
												invalidateList,
												user,
												phone
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_SHELL :
										{
											com.aoindustries.aoserv.client.linux.User.Name user = com.aoindustries.aoserv.client.linux.User.Name.valueOf(in.readUTF());
											PosixPath shell = PosixPath.valueOf(in.readUTF());
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_SHELL,
												user,
												shell
											);
											LinuxAccountHandler.setUserShell(
												conn,
												source,
												invalidateList,
												user,
												shell
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION :
										{
											int userServer = in.readCompressedInt();
											int days = in.readCompressedInt();
											process.setCommand(
												Command.SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION,
												userServer,
												days
											);
											LinuxAccountHandler.setUserServerJunkEmailRetention(
												conn,
												source,
												invalidateList,
												userServer,
												days
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readUTF();
											process.setCommand(
												Command.SET_LINUX_ACCOUNT_PASSWORD,
												userServer,
												AoservProtocol.FILTERED
											);
											LinuxAccountHandler.setUserServerPassword(
												conn,
												source,
												invalidateList,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_PREDISABLE_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_linux_server_account_predisable_password",
												userServer,
												AoservProtocol.FILTERED
											);
											LinuxAccountHandler.setUserServerPredisablePassword(
												conn,
												source,
												invalidateList,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_EMAIL_SPAMASSASSIN_INTEGRATION_MODE:
										{
											int userServer = in.readCompressedInt();
											String mode = in.readUTF();
											process.setCommand(
												Command.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_INTEGRATION_MODE,
												userServer,
												mode
											);
											LinuxAccountHandler.setUserServerSpamAssassinIntegrationMode(
												conn,
												source,
												invalidateList,
												userServer,
												mode
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE:
										{
											int userServer = in.readCompressedInt();
											float required_score = in.readFloat();
											process.setCommand(
												Command.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE,
												userServer,
												required_score
											);
											LinuxAccountHandler.setUserServerSpamAssassinRequiredScore(
												conn,
												source,
												invalidateList,
												userServer,
												required_score
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_DISCARD_SCORE:
										{
											int userServer = in.readCompressedInt();
											int discardScore = in.readCompressedInt();
											process.setCommand(
												"set_linux_server_account_spamassassin_discard_score",
												userServer,
												discardScore==-1 ? "\"\"" : Integer.toString(discardScore)
											);
											LinuxAccountHandler.setUserServerSpamAssassinDiscardScore(
												conn,
												source,
												invalidateList,
												userServer,
												discardScore
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION :
										{
											int userServer = in.readCompressedInt();
											int days = in.readCompressedInt();
											process.setCommand(
												Command.SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION,
												userServer,
												days
											);
											LinuxAccountHandler.setUserServerTrashEmailRetention(
												conn,
												source,
												invalidateList,
												userServer,
												days
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_USE_INBOX :
										{
											int userServer = in.readCompressedInt();
											boolean useInbox = in.readBoolean();
											process.setCommand(
												Command.SET_LINUX_SERVER_ACCOUNT_USE_INBOX,
												userServer,
												useInbox
											);
											LinuxAccountHandler.setUserServerUseInbox(
												conn,
												source,
												invalidateList,
												userServer,
												useInbox
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_MAJORDOMO_INFO_FILE :
										{
											int majordomoList = in.readCompressedInt();
											String file = in.readUTF();
											process.setCommand(
												Command.SET_MAJORDOMO_INFO_FILE,
												majordomoList,
												file
											);
											EmailHandler.setMajordomoInfoFile(
												conn,
												source,
												majordomoList,
												file
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MAJORDOMO_INTRO_FILE :
										{
											int majordomoList = in.readCompressedInt();
											String file = in.readUTF();
											process.setCommand(
												Command.SET_MAJORDOMO_INTRO_FILE,
												majordomoList,
												file
											);
											EmailHandler.setMajordomoIntroFile(
												conn,
												source,
												majordomoList,
												file
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MYSQL_SERVER_USER_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												Command.SET_MYSQL_SERVER_USER_PASSWORD,
												userServer,
												AoservProtocol.FILTERED
											);
											MysqlHandler.setUserServerPassword(
												conn,
												source,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MYSQL_SERVER_USER_PREDISABLE_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_mysql_server_user_predisable_password",
												userServer,
												AoservProtocol.FILTERED
											);
											MysqlHandler.setUserServerPredisablePassword(
												conn,
												source,
												invalidateList,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_NET_BIND_FIREWALLD_ZONES :
										{
											int bind = in.readCompressedInt();
											int numZones = in.readCompressedInt();
											Set<FirewallZone.Name> firewalldZones = new LinkedHashSet<>(numZones*4/3+1);
											for(int i = 0; i < numZones; i++) {
												FirewallZone.Name name = FirewallZone.Name.valueOf(in.readUTF());
												if(!firewalldZones.add(name)) {
													throw new IOException("Duplicate firewalld name: " + name);
												}
											}
											Object[] command = new Object[2 + numZones];
											command[0] = Command.SET_NET_BIND_FIREWALLD_ZONES;
											command[1] = bind;
											System.arraycopy(
												firewalldZones.toArray(new FirewallZone.Name[numZones]),
												0,
												command,
												2,
												numZones
											);
											process.setCommand(command);
											NetBindHandler.setBindFirewalldZones(
												conn,
												source,
												invalidateList,
												bind,
												firewalldZones
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_NET_BIND_MONITORING :
										{
											int bind = in.readCompressedInt();
											boolean enabled = in.readBoolean();
											process.setCommand(
												Command.SET_NET_BIND_MONITORING_ENABLED,
												bind,
												enabled
											);
											NetBindHandler.setBindMonitoringEnabled(
												conn,
												source,
												invalidateList,
												bind,
												enabled
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									// This exists for compatibility with older clients (versions &lt;= 1.80.2) only.
									case UNUSED_SET_NET_BIND_OPEN_FIREWALL :
										{
											int bind = in.readCompressedInt();
											boolean openFirewall = in.readBoolean();
											process.setCommand(
												"set_net_bind_open_firewall",
												bind,
												openFirewall
											);
											NetBindHandler.setBindOpenFirewall(
												conn,
												source,
												invalidateList,
												bind,
												openFirewall
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PACKAGE_DEFINITION_ACTIVE :
										{
											int packageDefinition = in.readCompressedInt();
											boolean isActive = in.readBoolean();
											process.setCommand(
												"set_package_definition_active",
												packageDefinition,
												isActive
											);
											PackageHandler.setPackageDefinitionActive(
												conn,
												source,
												invalidateList,
												packageDefinition,
												isActive
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PACKAGE_DEFINITION_LIMITS :
										{
											int packageDefinition = in.readCompressedInt();
											int count = in.readCompressedInt();
											String[] resources = new String[count];
											int[] soft_limits = new int[count];
											int[] hard_limits = new int[count];
											Money[] additionalRates = new Money[count];
											String[] additional_transaction_types = new String[count];
											for(int c = 0; c < count; c++) {
												resources[c] = in.readUTF().trim();
												soft_limits[c] = in.readCompressedInt();
												hard_limits[c] = in.readCompressedInt();
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
													int pennies = in.readCompressedInt();
													additionalRates[c] = pennies == -1 || pennies == 0 ? null : new Money(Currency.USD, pennies, 2);
												} else {
													additionalRates[c] = MoneyUtil.readNullMoney(in);
												}
												additional_transaction_types[c] = in.readNullUTF();
											}
											process.setCommand(
												"set_package_definition_limits",
												packageDefinition,
												count,
												resources,
												soft_limits,
												hard_limits,
												additionalRates,
												additional_transaction_types
											);
											PackageHandler.setPackageDefinitionLimits(
												conn,
												source,
												invalidateList,
												packageDefinition,
												resources,
												soft_limits,
												hard_limits,
												additionalRates,
												additional_transaction_types
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_POSTGRES_SERVER_USER_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												Command.SET_POSTGRES_SERVER_USER_PASSWORD,
												userServer,
												password
											);
											PostgresqlHandler.setUserServerPassword(
												conn,
												source,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_POSTGRES_SERVER_USER_PREDISABLE_PASSWORD :
										{
											int userServer = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_postgres_server_user_predisable_password",
												userServer,
												AoservProtocol.FILTERED
											);
											PostgresqlHandler.setUserServerPredisablePassword(
												conn,
												source,
												invalidateList,
												userServer,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PRIMARY_HTTPD_SITE_URL :
										{
											int virtualHostName = in.readCompressedInt();
											process.setCommand(
												Command.SET_PRIMARY_HTTPD_SITE_URL,
												virtualHostName
											);
											WebHandler.setPrimaryVirtualHostName(
												conn,
												source,
												invalidateList,
												virtualHostName
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PRIMARY_LINUX_GROUP_ACCOUNT :
										{
											int groupUser = in.readCompressedInt();
											process.setCommand(
												Command.SET_PRIMARY_LINUX_GROUP_ACCOUNT,
												groupUser
											);
											LinuxAccountHandler.setPrimaryGroupUser(
												conn,
												source,
												invalidateList,
												groupUser
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									/*case SET_TICKET_ASSIGNED_TO :
										{
											int ticketID=in.readCompressedInt(); 
											String assignedTo=in.readUTF().trim();
											if(assignedTo.length() == 0) assignedTo=null;
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												"set_ticket_assigned_to",
												ticketID,
												assignedTo,
												username,
												comments
											);
											TicketHandler.setTicketAssignedTo(
												conn,
												source,
												invalidateList,
												ticketID,
												assignedTo,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case SET_TICKET_CONTACT_EMAILS :
										{
											int ticketID = in.readCompressedInt();
											Set<Email> contactEmails;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_81_22) >= 0) {
												int size = in.readCompressedInt();
												contactEmails = new LinkedHashSet<>(size*4/3+1);
												for(int i = 0; i < size; i++) {
													contactEmails.add(Email.valueOf(in.readUTF()));
												}
											} else {
												contactEmails = Profile.splitEmails(in.readUTF().trim());
											}
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) {
												String username=in.readUTF();
												String comments=in.readUTF();
											}
											process.setCommand(
												"set_ticket_contact_emails",
												ticketID,
												contactEmails
											);
											TicketHandler.setTicketContactEmails(
												conn,
												source,
												invalidateList,
												ticketID,
												contactEmails
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_TICKET_CONTACT_PHONE_NUMBERS :
										{
											int ticketID = in.readCompressedInt(); 
											String contactPhoneNumbers = in.readUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) {
												String username=in.readUTF();
												String comments=in.readUTF();
											}
											process.setCommand(
												"set_ticket_contact_phone_numbers",
												ticketID,
												contactPhoneNumbers
											);
											TicketHandler.setTicketContactPhoneNumbers(
												conn,
												source,
												invalidateList,
												ticketID,
												contactPhoneNumbers
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_TICKET_BUSINESS :
										{
											int ticketID = in.readCompressedInt();
											Account.Name oldAccounting;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_48)>=0) {
												// Added old accounting to behave like atomic variable
												String oldAccountingS = in.readUTF();
												oldAccounting = oldAccountingS.length() == 0 ? null : Account.Name.valueOf(oldAccountingS);
											} else {
												oldAccounting = null;
											}
											String newAccountingS = in.readUTF();
											Account.Name newAccounting = newAccountingS.length() == 0 ? null : Account.Name.valueOf(newAccountingS);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_43)<=0) {
												String username = in.readUTF();
												String comments = in.readUTF();
											}
											process.setCommand(
												"set_ticket_business",
												ticketID,
												oldAccounting,
												newAccounting
											);
											boolean updated = TicketHandler.setTicketAccount(
												conn,
												source,
												invalidateList,
												ticketID,
												oldAccounting,
												newAccounting
											);
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_48)<0) {
												resp = Response.DONE;
											} else {
												// Added boolean updated response
												resp = Response.of(
													AoservProtocol.DONE,
													updated
												);
											}
											sendInvalidateList = true;
										}
										break;
									case SET_TICKET_STATUS :
										{
											int ticketID = in.readCompressedInt();
											String oldStatus = in.readUTF();
											String newStatus = in.readUTF();
											long statusTimeout = in.readLong();
											process.setCommand(
												"set_ticket_status",
												ticketID,
												oldStatus,
												newStatus,
												new java.util.Date(statusTimeout)
											);
											boolean updated = TicketHandler.setTicketStatus(
												conn,
												source,
												invalidateList,
												ticketID,
												oldStatus,
												newStatus,
												statusTimeout
											);
											resp = Response.of(
												AoservProtocol.DONE,
												updated
											);
											sendInvalidateList = true;
										}
										break;
									case SET_TICKET_INTERNAL_NOTES :
										{
											int ticketID = in.readCompressedInt();
											String oldInternalNotes = in.readLongUTF();
											String newInternalNotes = in.readLongUTF();
											process.setCommand(
												"set_ticket_internal_notes",
												ticketID,
												oldInternalNotes.length(),
												newInternalNotes.length()
											);
											boolean updated = TicketHandler.setTicketInternalNotes(
												conn,
												source,
												invalidateList,
												ticketID,
												oldInternalNotes,
												newInternalNotes
											);
											resp = Response.of(
												AoservProtocol.DONE,
												updated
											);
											sendInvalidateList = true;
										}
										break;
									case START_APACHE :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.START_APACHE,
												linuxServer
											);
											WebHandler.startApache(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_CRON :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.START_CRON,
												linuxServer
											);
											LinuxServerHandler.startCron(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_DISTRO :
										{
											process.setPriority(Thread.MIN_PRIORITY+1);
											currentThread.setPriority(Thread.MIN_PRIORITY+1);

											int linuxServer = in.readCompressedInt();
											boolean includeUser = in.readBoolean();
											process.setCommand(
												Command.START_DISTRO,
												linuxServer,
												includeUser
											);
											LinuxServerHandler.startDistro(
												conn,
												source,
												linuxServer,
												includeUser
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_JVM :
										{
											int tomcatSite = in.readCompressedInt();
											process.setCommand(
												Command.START_JVM,
												tomcatSite
											);
											String message = WebHandler.startJVM(
												conn,
												source,
												tomcatSite
											);
											resp = Response.ofNullString(
												AoservProtocol.DONE,
												message
											);
											sendInvalidateList = false;
										}
										break;
									case START_MYSQL :
										{
											int mysqlServer = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_4) < 0) {
												throw new IOException(Command.START_MYSQL + " call not supported for AOServ Client version < " + AoservProtocol.Version.VERSION_1_4 + ", please upgrade AOServ Client.");
											}
											process.setCommand(
												Command.START_MYSQL,
												mysqlServer
											);
											MysqlHandler.startServer(
												conn,
												source,
												mysqlServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_POSTGRESQL :
										{
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												Command.START_POSTGRESQL,
												postgresServer
											);
											PostgresqlHandler.startServer(
												conn,
												source,
												postgresServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_XFS :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.START_XFS,
												linuxServer
											);
											LinuxServerHandler.startXfs(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_XVFB :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.START_XVFB,
												linuxServer
											);
											LinuxServerHandler.startXvfb(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_APACHE :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.STOP_APACHE,
												linuxServer
											);
											WebHandler.stopApache(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_CRON :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.STOP_CRON,
												linuxServer
											);
											LinuxServerHandler.stopCron(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_JVM :
										{
											int tomcatSite = in.readCompressedInt();
											process.setCommand(
												Command.STOP_JVM,
												tomcatSite
											);
											String message = WebHandler.stopJVM(
												conn,
												source,
												tomcatSite
											);
											resp = Response.ofNullString(
												AoservProtocol.DONE,
												message
											);
											sendInvalidateList = false;
										}
										break;
									case STOP_MYSQL :
										{
											int mysqlServer = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_4) < 0) {
												throw new IOException(Command.STOP_MYSQL + " call not supported for AOServ Client version < " + AoservProtocol.Version.VERSION_1_4 + ", please upgrade AOServ Client.");
											}
											process.setCommand(
												Command.STOP_MYSQL,
												mysqlServer
											);
											MysqlHandler.stopServer(
												conn,
												source,
												mysqlServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_POSTGRESQL :
										{
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												Command.STOP_POSTGRESQL,
												postgresServer
											);
											PostgresqlHandler.stopServer(
												conn,
												source,
												postgresServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_XFS :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.STOP_XFS,
												linuxServer
											);
											LinuxServerHandler.stopXfs(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_XVFB :
										{
											int linuxServer = in.readCompressedInt();
											process.setCommand(
												Command.STOP_XVFB,
												linuxServer
											);
											LinuxServerHandler.stopXvfb(
												conn,
												source,
												linuxServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									/*case TICKET_WORK :
										{
											int ticketID=in.readCompressedInt();
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												Command.ADD_TICKET_WORK,
												ticketID,
												username,
												comments
											);
											TicketHandler.ticketWork(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AoservProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case TRANSACTION_APPROVED :
										{
											int transid = in.readCompressedInt();
											int creditCardTransaction;
											String paymentInfo;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_28)<=0) {
												String paymentType=in.readUTF();
												paymentInfo = in.readNullUTF();
												String merchant=in.readNullUTF();
												String apr_num;
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_0_A_128)<0) apr_num=Integer.toString(in.readCompressedInt());
												else apr_num=in.readUTF();
												throw new SQLException("approve_transaction for protocol version "+AoservProtocol.Version.VERSION_1_28+" or older is no longer supported.");
											} else {
												creditCardTransaction = in.readCompressedInt();
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
													paymentInfo = in.readNullUTF();
												} else {
													paymentInfo = null;
												}
											}
											process.setCommand(
												"approve_transaction",
												transid,
												creditCardTransaction,
												paymentInfo
											);
											BillingTransactionHandler.transactionApproved(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction,
												paymentInfo
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case TRANSACTION_DECLINED :
										{
											int transid = in.readCompressedInt();
											int creditCardTransaction;
											String paymentInfo;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_28)<=0) {
												String paymentType=in.readUTF().trim();
												paymentInfo = in.readNullUTF();
												String merchant=in.readNullUTF();
												throw new SQLException("decline_transaction for protocol version "+AoservProtocol.Version.VERSION_1_28+" or older is no longer supported.");
											} else {
												creditCardTransaction = in.readCompressedInt();
												if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
													paymentInfo = in.readNullUTF();
												} else {
													paymentInfo = null;
												}
											}
											process.setCommand(
												"decline_transaction",
												transid,
												creditCardTransaction,
												paymentInfo
											);
											BillingTransactionHandler.transactionDeclined(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction,
												paymentInfo
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case TRANSACTION_HELD :
										{
											int transid = in.readCompressedInt();
											int creditCardTransaction = in.readCompressedInt();
											String paymentInfo;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												paymentInfo = in.readNullUTF();
											} else {
												paymentInfo = null;
											}
											process.setCommand(
												"hold_transaction",
												transid,
												creditCardTransaction,
												paymentInfo
											);
											BillingTransactionHandler.transactionHeld(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction,
												paymentInfo
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case REACTIVATE_CREDIT_CARD :
										{
											int creditCard = in.readCompressedInt();
											process.setCommand(
												"reactivate_credit_card",
												creditCard
											);
											PaymentHandler.reactivateCreditCard(
												conn,
												source,
												invalidateList,
												creditCard
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_CREDIT_CARD_USE_MONTHLY :
										{
											Account.Name account = Account.Name.valueOf(in.readUTF());
											int creditCard = in.readCompressedInt();
											process.setCommand(
												"set_credit_card_use_monthly",
												account,
												creditCard
											);
											PaymentHandler.setCreditCardUseMonthly(
												conn,
												source,
												invalidateList,
												account,
												creditCard
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_FAILOVER_FILE_REPLICATION_BIT_RATE :
										{
											int fileReplication = in.readCompressedInt();
											final Long bitRate;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_61)<=0) {
												int bitRateInt = in.readCompressedInt();
												bitRate = bitRateInt==-1 ? null : (long)bitRateInt;
											} else {
												long bitRateLong = in.readLong();
												bitRate = bitRateLong==-1 ? null : bitRateLong;
											}
											process.setCommand(
												"set_failover_file_replication_bit_rate",
												fileReplication,
												bitRate==null ? "unlimited" : bitRate.toString()
											);
											FailoverHandler.setFileReplicationBitRate(
												conn,
												source,
												invalidateList,
												fileReplication,
												bitRate
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_FAILOVER_FILE_SCHEDULES :
										{
											int replication = in.readCompressedInt();
											int size = in.readCompressedInt();
											List<Short> hours = new ArrayList<>(size);
											List<Short> minutes = new ArrayList<>(size);
											for(int c=0;c<size;c++) {
												hours.add(in.readShort());
												minutes.add(in.readShort());
											}
											process.setCommand(
												"set_failover_file_schedules",
												replication,
												size
											);
											FailoverHandler.setFileReplicationSchedules(
												conn,
												source,
												invalidateList,
												replication,
												hours,
												minutes
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									 case UPDATE_CREDIT_CARD :
										{
											int creditCard = in.readCompressedInt();
											String cardInfo;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												cardInfo = in.readUTF().trim();
											} else {
												cardInfo = null;
											}
											String firstName=in.readUTF().trim();
											String lastName=in.readUTF().trim();
											String companyName=in.readUTF().trim();
											if(companyName.length() == 0) companyName=null;
											String email=in.readUTF().trim();
											if(email.length() == 0) email=null;
											String phone=in.readUTF().trim();
											if(phone.length() == 0) phone=null;
											String fax=in.readUTF().trim();
											if(fax.length() == 0) fax=null;
											String customerId;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_1) >= 0) {
												customerId = in.readUTF().trim();
												if(customerId.length() == 0) customerId = null;
											} else {
												customerId = null;
											}
											String customerTaxId=in.readUTF().trim();
											if(customerTaxId.length() == 0) customerTaxId=null;
											String streetAddress1=in.readUTF().trim();
											String streetAddress2=in.readUTF().trim();
											if(streetAddress2.length() == 0) streetAddress2=null;
											String city=in.readUTF().trim();
											String state=in.readUTF().trim();
											if(state.length() == 0) state=null;
											String postalCode=in.readUTF().trim();
											if(postalCode.length() == 0) postalCode=null;
											String countryCode=in.readUTF().trim();
											String description=in.readUTF().trim();
											if(description.length() == 0) description=null;
											process.setCommand(
												"update_credit_card",
												creditCard,
												cardInfo,
												firstName,
												lastName,
												companyName,
												email,
												phone,
												fax,
												customerId,
												customerTaxId,
												streetAddress1,
												streetAddress2,
												city,
												state,
												postalCode,
												countryCode,
												description
											);
											PaymentHandler.updateCreditCard(
												conn,
												source,
												invalidateList,
												creditCard,
												cardInfo,
												firstName,
												lastName,
												companyName,
												email,
												phone,
												fax,
												customerId,
												customerTaxId,
												streetAddress1,
												streetAddress2,
												city,
												state,
												postalCode,
												countryCode,
												description
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_CREDIT_CARD_NUMBER_AND_EXPIRATION :
										{
											int creditCard = in.readCompressedInt();
											String maskedCardNumber = in.readUTF();
											Byte expirationMonth;
											Short expirationYear;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												expirationMonth = in.readByte();
												expirationYear = in.readShort();
											} else {
												expirationMonth = null;
												expirationYear = null;
											}
											String encryptedCardNumber = in.readNullUTF();
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) < 0) {
												String encryptedExpiration = in.readNullUTF();
											}
											int encryptionFrom = in.readCompressedInt();
											int encryptionRecipient = in.readCompressedInt();
											process.setCommand(
												"update_credit_card_number_and_expiration",
												creditCard,
												maskedCardNumber,
												expirationMonth==null ? null : AoservProtocol.FILTERED,
												expirationYear==null ? null : AoservProtocol.FILTERED,
												encryptedCardNumber==null ? null : AoservProtocol.FILTERED,
												encryptionFrom==-1 ? null : encryptionFrom,
												encryptionRecipient==-1 ? null : encryptionRecipient
											);
											PaymentHandler.updateCreditCardNumberAndExpiration(
												conn,
												source,
												invalidateList,
												creditCard,
												maskedCardNumber,
												expirationMonth,
												expirationYear,
												encryptedCardNumber,
												encryptionFrom,
												encryptionRecipient
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_CREDIT_CARD_EXPIRATION :
										{
											int creditCard = in.readCompressedInt();
											Byte expirationMonth;
											Short expirationYear;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_82_0) >= 0) {
												expirationMonth = in.readByte();
												expirationYear = in.readShort();
											} else {
												expirationMonth = null;
												expirationYear = null;
												String encryptedExpiration = in.readUTF();
												int encryptionFrom = in.readCompressedInt();
												int encryptionRecipient = in.readCompressedInt();
											}
											process.setCommand(
												"update_credit_card_expiration",
												creditCard,
												expirationMonth==null ? null : AoservProtocol.FILTERED,
												expirationYear==null ? null : AoservProtocol.FILTERED
											);
											PaymentHandler.updateCreditCardExpiration(
												conn,
												source,
												invalidateList,
												creditCard,
												expirationMonth,
												expirationYear
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_HTTPD_TOMCAT_DATA_SOURCE:
										{
											int contextDataSource = in.readCompressedInt();
											String name=in.readUTF();
											String driverClassName=in.readUTF();
											String url=in.readUTF();
											String username=in.readUTF();
											String password=in.readUTF();
											int maxActive=in.readCompressedInt();
											int maxIdle=in.readCompressedInt();
											int maxWait=in.readCompressedInt();
											String validationQuery=in.readUTF();
											if(validationQuery.length() == 0) validationQuery=null;
											process.setCommand(
												Command.UPDATE_HTTPD_TOMCAT_DATA_SOURCE,
												contextDataSource,
												name,
												driverClassName,
												url,
												username,
												AoservProtocol.FILTERED,
												maxActive,
												maxIdle,
												maxWait,
												validationQuery
											);
											WebHandler.updateContextDataSource(
												conn,
												source,
												invalidateList,
												contextDataSource,
												name,
												driverClassName,
												url,
												username,
												password,
												maxActive,
												maxIdle,
												maxWait,
												validationQuery
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_HTTPD_TOMCAT_PARAMETER :
										{
											int contextParameter = in.readCompressedInt();
											String name=in.readUTF();
											String value=in.readUTF();
											boolean override=in.readBoolean();
											String description=in.readUTF();
											if(description.length() == 0) description=null;
											process.setCommand(
												Command.UPDATE_HTTPD_TOMCAT_PARAMETER,
												contextParameter,
												name,
												value,
												override,
												description
											);
											WebHandler.updateContextParameter(
												conn,
												source,
												invalidateList,
												contextParameter,
												name,
												value,
												override,
												description
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_PACKAGE_DEFINITION :
										{
											int packageDefinition = in.readCompressedInt();
											Account.Name account = Account.Name.valueOf(in.readUTF());
											String category=in.readUTF();
											String name=in.readUTF().trim();
											String version=in.readUTF().trim();
											String display=in.readUTF().trim();
											String description=in.readUTF().trim();
											Money setupFee;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
												int pennies = in.readCompressedInt();
												setupFee = pennies == -1 || pennies == 0 ? null : new Money(Currency.USD, pennies, 2);
											} else {
												setupFee = MoneyUtil.readNullMoney(in);
											}
											String setupFeeTransactionType=in.readNullUTF();
											Money monthlyRate;
											if(source.getProtocolVersion().compareTo(AoservProtocol.Version.VERSION_1_83_0) < 0) {
												monthlyRate = new Money(Currency.USD, in.readCompressedInt(), 2);
											} else {
												monthlyRate = MoneyUtil.readMoney(in);
											}
											String monthlyRateTransactionType=in.readUTF();
											process.setCommand(
												"update_package_definition",
												packageDefinition,
												account,
												category,
												name,
												version,
												display,
												description,
												setupFee,
												setupFeeTransactionType,
												monthlyRate,
												monthlyRateTransactionType
											);
											PackageHandler.updatePackageDefinition(
												conn,
												source,
												invalidateList,
												packageDefinition,
												account,
												category,
												name,
												version,
												display,
												description,
												setupFee,
												setupFeeTransactionType,
												monthlyRate,
												monthlyRateTransactionType
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case WAIT_FOR_REBUILD :
										{
											int clientTableID = in.readCompressedInt();
											Table.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											int linuxServer = in.readCompressedInt();
											switch(tableID) {
												case HTTPD_SITES :
													process.setCommand(
														Command.WAIT_FOR_HTTPD_SITE_REBUILD,
														linuxServer
													);
													WebHandler.waitForHttpdSiteRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case LINUX_ACCOUNTS :
													process.setCommand(
														Command.WAIT_FOR_LINUX_ACCOUNT_REBUILD,
														linuxServer
													);
													LinuxAccountHandler.waitForUserRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case MYSQL_DATABASES :
													process.setCommand(
														Command.WAIT_FOR_MYSQL_DATABASE_REBUILD,
														linuxServer
													);
													MysqlHandler.waitForDatabaseRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case MYSQL_DB_USERS :
													process.setCommand(
														Command.WAIT_FOR_MYSQL_DB_USER_REBUILD,
														linuxServer
													);
													MysqlHandler.waitForDatabaseUserRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case MYSQL_SERVERS :
													process.setCommand(
														Command.WAIT_FOR_MYSQL_SERVER_REBUILD,
														linuxServer
													);
													MysqlHandler.waitForServerRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case MYSQL_USERS :
													process.setCommand(
														Command.WAIT_FOR_MYSQL_USER_REBUILD,
														linuxServer
													);
													MysqlHandler.waitForUserRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case POSTGRES_DATABASES :
													process.setCommand(
														Command.WAIT_FOR_POSTGRES_DATABASE_REBUILD,
														linuxServer
													);
													PostgresqlHandler.waitForDatabaseRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case POSTGRES_SERVERS :
													process.setCommand(
														Command.WAIT_FOR_POSTGRES_SERVER_REBUILD,
														linuxServer
													);
													PostgresqlHandler.waitForServerRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												case POSTGRES_USERS :
													process.setCommand(
														Command.WAIT_FOR_POSTGRES_USER_REBUILD,
														linuxServer
													);
													PostgresqlHandler.waitForUserRebuild(
														conn,
														source,
														linuxServer
													);
													break;
												default :
													throw new IOException("Unable to wait for rebuild on table: clientTableID="+clientTableID+", tableID="+tableID);
											}
											resp = Response.DONE;
											sendInvalidateList = false;
											break;
										}
									// <editor-fold desc="Virtual Servers">
									case REQUEST_VNC_CONSOLE_DAEMON_ACCESS :
										{
											int virtualServer=in.readCompressedInt();
											process.setCommand(
												"request_vnc_console_daemon_access",
												virtualServer
											);
											Server.DaemonAccess daemonAccess = VirtualServerHandler.requestVncConsoleDaemonAccess(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												daemonAccess.getProtocol(),
												daemonAccess.getHost(),
												daemonAccess.getPort().getPort(),
												daemonAccess.getKey()
											);
											sendInvalidateList = false;
										}
										break;
									case VERIFY_VIRTUAL_DISK :
										{
											int virtualDisk = in.readCompressedInt();
											process.setCommand(
												Command.VERIFY_VIRTUAL_DISK,
												virtualDisk
											);
											long lastVerified = VirtualServerHandler.verifyVirtualDisk(
												conn,
												source,
												virtualDisk
											);
											resp = Response.of(
												AoservProtocol.DONE,
												lastVerified
											);
											sendInvalidateList = false;
										}
										break;
									case CREATE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.CREATE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.createVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case REBOOT_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.REBOOT_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.rebootVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case SHUTDOWN_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.SHUTDOWN_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.shutdownVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case DESTROY_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.DESTROY_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.destroyVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case PAUSE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.PAUSE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.pauseVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case UNPAUSE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.UNPAUSE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.unpauseVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case GET_VIRTUAL_SERVER_STATUS :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_VIRTUAL_SERVER_STATUS,
												virtualServer
											);
											int status = VirtualServerHandler.getVirtualServerStatus(
												conn,
												source,
												virtualServer
											);
											resp = Response.of(
												AoservProtocol.DONE,
												status
											);
											sendInvalidateList = false;
										}
										break;
									case GET_PRIMARY_PHYSICAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_PRIMARY_PHYSICAL_SERVER,
												virtualServer
											);
											int physicalServer = ClusterHandler.getPrimaryPhysicalServer(conn, source, virtualServer);
											resp = Response.of(
												AoservProtocol.DONE,
												physicalServer
											);
											sendInvalidateList = false;
										}
										break;
									case GET_SECONDARY_PHYSICAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												Command.GET_SECONDARY_PHYSICAL_SERVER,
												virtualServer
											);
											int physicalServer = ClusterHandler.getSecondaryPhysicalServer(conn, source, virtualServer);
											resp = Response.of(
												AoservProtocol.DONE,
												physicalServer
											);
											sendInvalidateList = false;
										}
										break;
									// </editor-fold>
									default :
										keepOpen = false;
										throw new IOException("Unknown task code: " + taskCode);
								}

								// Convert the invalidate list to client table IDs before releasing the connection
								if(sendInvalidateList) {
									clientInvalidateList=new IntArrayList();
									for(Table.TableID tableID : tableIDs) {
										if(invalidateList.isInvalid(tableID)) {
											int clientTableID = TableHandler.convertToClientTableID(conn, source, tableID);
											if(clientTableID != -1) clientInvalidateList.add(clientTableID);
										}
									}
								}
							} catch(RuntimeException | ValidationException | IOException err) {
								if(conn.rollback()) {
									connRolledBack=true;
									invalidateList=null;
								}
								throw err;
							} catch(SQLException err) {
								if(conn.rollbackAndClose()) {
									connRolledBack=true;
									invalidateList=null;
								}
								throw err;
							} finally {
								if(!connRolledBack && !conn.isClosed()) conn.commit();
							}
						} finally {
							conn.releaseConnection();
						}
						// Invalidate the affected tables
						invalidateTables(invalidateList, source);

						// Write the response codes
						if(resp != null) resp.writeResponse(out, source.getProtocolVersion());

						// Write the invalidate list
						if(sendInvalidateList) {
							assert clientInvalidateList!=null;
							int numTables=clientInvalidateList.size();
							for(int c=0;c<numTables;c++) {
								int tableID=clientInvalidateList.getInt(c);
								out.writeCompressedInt(tableID);
							}
							out.writeCompressedInt(-1);
						}
					} catch(RuntimeException err) {
						logger.log(Level.SEVERE, null, err);
						keepOpen = false;
					} catch(SQLException err) {
						if(logSQLException) logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AoservProtocol.SQL_EXCEPTION);
						out.writeUTF(message==null?"":message);
					} catch(ValidationException err) {
						logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AoservProtocol.IO_EXCEPTION);
						out.writeUTF(message==null?"":message);
						keepOpen = false; // Close on ValidationException
					} catch(IOException err) {
						if(logIOException) logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AoservProtocol.IO_EXCEPTION);
						out.writeUTF(message==null?"":message);
						keepOpen = false; // Close on IOException
					} finally {
						if(currentThread.getPriority()!=Thread.NORM_PRIORITY) {
							currentThread.setPriority(Thread.NORM_PRIORITY);
							process.setPriority(Thread.NORM_PRIORITY);
						}
					}
				}
			}
			out.flush();
			process.commandCompleted();
		} finally {
			if(addTime) {
				concurrency.decrementAndGet();
				totalTime.addAndGet(System.currentTimeMillis()-requestStartTime);
			}
		}
		return keepOpen;
	}

	/**
	 * Invalidates a table by notifying all connected clients, except the client
	 * that initiated this request.
	 * <p>
	 * TODO: We need a way to convert invalidations of current tables to old table mappings.
	 * This would be the counterpart to {@link TableHandler#getOldTable(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, java.lang.String)}.
	 * </p>
	 */
	public static void invalidateTables(
		InvalidateList invalidateList,
		RequestSource invalidateSource
	) throws IOException, SQLException {
		// Invalidate the internally cached data first
		invalidateList.invalidateMasterCaches();

		// Values used inside the loops
		SmallIdentifier invalidateSourceConnectorID = invalidateSource == null ? null : invalidateSource.getConnectorID();

		IntList tableList=new IntArrayList();
		final DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
		// Grab a copy of cacheListeners to help avoid deadlock
		List<RequestSource> listenerCopy=new ArrayList<>(cacheListeners.size());
		synchronized(cacheListeners) {
			listenerCopy.addAll(cacheListeners);
		}
		Iterator<RequestSource> I=listenerCopy.iterator();
		while(I.hasNext()) {
			try {
				RequestSource source=I.next();
				if(invalidateSourceConnectorID != null && invalidateSourceConnectorID.equals(source.getConnectorID())) {
					tableList.clear();
					// Build the list with a connection, but don't send until the connection is released
					try {
						try {
							for(Table.TableID tableID : tableIDs) {
								int clientTableID=TableHandler.convertToClientTableID(conn, source, tableID);
								if(clientTableID!=-1) {
									List<Account.Name> affectedBusinesses = invalidateList.getAffectedAccounts(tableID);
									List<Integer> affectedHosts = invalidateList.getAffectedHosts(tableID);
									if(
										affectedBusinesses!=null
										&& affectedHosts!=null
									) {
										boolean businessMatches;
										int size=affectedBusinesses.size();
										if(size == 0) businessMatches=true;
										else {
											businessMatches=false;
											for(int c=0;c<size;c++) {
												if(AccountHandler.canAccessAccount(conn, source, affectedBusinesses.get(c))) {
													businessMatches=true;
													break;
												}
											}
										}

										// Filter by server
										boolean serverMatches;
										size=affectedHosts.size();
										if(size == 0) serverMatches=true;
										else {
											serverMatches=false;
											for(int c=0;c<size;c++) {
												int host = affectedHosts.get(c);
												if(NetHostHandler.canAccessHost(conn, source, host)) {
													serverMatches=true;
													break;
												}
												if(
													tableID==Table.TableID.AO_SERVERS
													|| tableID==Table.TableID.IP_ADDRESSES
													|| tableID==Table.TableID.LINUX_ACCOUNTS
													|| tableID==Table.TableID.LINUX_SERVER_ACCOUNTS
													|| tableID==Table.TableID.NET_DEVICES
													|| tableID==Table.TableID.SERVERS
													|| tableID==Table.TableID.USERNAMES
												) {
													// These tables invalidations are also sent to the servers failover parent
													int failoverServer=NetHostHandler.getFailoverServer(conn, host);
													if(failoverServer!=-1 && NetHostHandler.canAccessHost(conn, source, failoverServer)) {
														serverMatches=true;
														break;
													}
												}
											}
										}


										// Send the invalidate through
										if(businessMatches && serverMatches) tableList.add(clientTableID);
									}
								}
							}
						} catch(SQLException err) {
							conn.rollbackAndClose();
							throw err;
						}
					} finally {
						conn.releaseConnection();
					}
					source.cachesInvalidated(tableList);
				}
			} catch(IOException err) {
				logger.log(Level.SEVERE, null, err);
			}
		}
	}

	private static class MasterServiceState {
		private volatile boolean started;
	}

	private static final PolymorphicMultimap<Object,MasterServiceState> serviceRegistry = new PolymorphicMultimap<>(Object.class);

	/**
	 * Gets a started service of the given class or interface.
	 * If more than one started service is of the given class, there is no
	 * guarantee which is returned.  TODO: round-robin from the registry.
	 *
	 * @throws NoServiceException when no services are of the given class
	 * @throws ServiceNotStartedException when no services of the given class are started
	 */
	public static <T> T getService(Class<? extends T> clazz) throws MasterServiceException {
		List<Map.Entry<T,MasterServiceState>> entries = serviceRegistry.getEntries(clazz);
		if(!entries.isEmpty()) {
			for(Map.Entry<T,MasterServiceState> entry : entries) {
				MasterServiceState state = entry.getValue();
				if(state.started) {
					return entry.getKey();
				}
			}
			throw new ServiceNotStartedException(
				entries.size() + " failed " + (entries.size() == 1 ? "service" : "services") + " found for class: " + clazz.getName()
			);
		}
		throw new NoServiceException("No service found for class: " + clazz.getName());
	}

	/**
	 * Gets all started services of the given class or interface.
	 *
	 * @return  The list of services or an empty list when none found
	 */
	public static <T> List<T> getStartedServices(Class<? extends T> clazz) {
		return serviceRegistry.getKeysFilterEntry(
			clazz,
			(Map.Entry<T, MasterServiceState> e) -> e.getValue().started
		);
	}

	/**
	 * Gets all services of the given class or interface.
	 *
	 * @return  The list of services or an empty list when none found
	 *
	 * @throws ServiceNotStartedException when a matching service is not started
	 */
	public static <T> List<T> getServices(Class<? extends T> clazz) throws ServiceNotStartedException {
		// Make sure all are started
		int[] notStartedCount = new int[1];
		List<T> matches = serviceRegistry.getKeysFilterEntry(
			clazz,
			(Map.Entry<T,MasterServiceState> e) -> {
				if(!e.getValue().started) notStartedCount[0]++;
				// Stop storing, but continue to count, once a failed service is located
				return notStartedCount[0] == 0;
			}
		);
		int failedCount = notStartedCount[0];
		if(failedCount != 0) {
			throw new ServiceNotStartedException(
				failedCount + " failed " + (failedCount == 1 ? "service" : "services") + " found for class: " + clazz.getName()
			);
		} else {
			return matches;
		}
	}


	/**
	 * Loads and starts all {@link MasterService}.
	 * Runs all of the configured protocols of {@link MasterServer}
	 * processes as configured in <code>com/aoindustries/aoserv/master/aoserv-master.properties</code>.
	 */
	public static void main(String[] args) {
		try {
			// Configure the SSL
			String trustStorePath=MasterConfiguration.getSSLTruststorePath();
			if(trustStorePath!=null && trustStorePath.length()>0) {
				System.setProperty("javax.net.ssl.trustStore", trustStorePath);
			}
			String trustStorePassword=MasterConfiguration.getSSLTruststorePassword();
			if(trustStorePassword!=null && trustStorePassword.length()>0) {
				System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
			}
			String keyStorePath=MasterConfiguration.getSSLKeystorePath();
			if(keyStorePath!=null && keyStorePath.length()>0) {
				System.setProperty("javax.net.ssl.keyStore", keyStorePath);
			}
			String keyStorePassword=MasterConfiguration.getSSLKeystorePassword();
			if(keyStorePassword!=null && keyStorePassword.length()>0) {
				System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
			}

			// TODO: Convert these to MasterService
			AccountCleaner.start();
			ClusterHandler.start();
			PaymentHandler.start();
			FailoverHandler.start();
			SignupHandler.start();
			TableHandler.start();
			TicketHandler.start();

			// TODO: A way to get the instance of a esrvice given its class
			// TODO: A way to start services in dependency order

			// Instantiate all services
			System.out.print("Loading services: ");
			List<Tuple2<MasterService,MasterServiceState>> servicesToStart = new ArrayList<>();
			ServiceLoader<MasterService> loader = ServiceLoader.load(MasterService.class);
			Iterator<MasterService> iter = loader.iterator();
			while(iter.hasNext()) {
				MasterService service = iter.next();
				MasterServiceState state = new MasterServiceState();
				servicesToStart.add(new Tuple2<>(service, state));
				serviceRegistry.put(service, state);
			}
			System.out.println(servicesToStart.size() + " " + (servicesToStart.size() == 1 ? "service" : "services") + " loaded");

			List<Tuple2<MasterService,MasterServiceState>> failedServices = startServices(servicesToStart, true, System.out);

			// Start listening after initialization to allow all modules to be loaded
			// TODO: Should the network protocol be a service, too?
			List<String> protocols=MasterConfiguration.getProtocols();
			if(protocols.isEmpty()) throw new IllegalArgumentException("protocols is empty");
			for(String protocol : protocols) {
				List<String> binds=MasterConfiguration.getBinds(protocol);
				if(binds.isEmpty()) throw new IllegalArgumentException("binds is empty for protocol="+protocol);

				List<Integer> ports = MasterConfiguration.getPorts(protocol);
				if(ports.isEmpty()) throw new IllegalArgumentException("ports is empty for protocol="+protocol);

				for(String bind : binds) {
					for(int port : ports) {
						switch (protocol) {
							case TCPServer.PROTOCOL_TCP:
								new TCPServer(bind, port).start();
								break;
							case SSLServer.PROTOCOL_SSL:
								new SSLServer(bind, port).start();
								break;
							default:
								throw new IllegalArgumentException("Unknown protocol: "+protocol);
						}
					}
				}
			}

			while(!failedServices.isEmpty()) {
				try {
					Thread.sleep(SERVICE_RETRY_INTERVAL);
				} catch(InterruptedException e) {
					logger.log(Level.WARNING, null, e);
				}
				failedServices = startServices(failedServices, false, System.out);
			}
		} catch (IOException | IllegalArgumentException err) {
			logger.log(Level.SEVERE, null, err);
		}
	}

	/**
	 * Starts the given services, returning a list of those that failed to start.
	 */
	private static List<Tuple2<MasterService,MasterServiceState>> startServices(List<Tuple2<MasterService,MasterServiceState>> servicesToStart, boolean isFirstStart, PrintStream out) {
		// TODO: Support starting in dependency order
		out.println(isFirstStart ? "Starting services:" : "Starting failed services:");
		List<Tuple2<MasterService,MasterServiceState>> failedServices = new ArrayList<>();
		for(Tuple2<MasterService,MasterServiceState> serviceAndState : servicesToStart) {
			MasterService service = serviceAndState.getElement1();
			out.print("    " + service.getClass().getName());
			boolean started = false;
			try {
				service.start();
				serviceAndState.getElement2().started = true;
				started = true;
				// Fatal, will no retry adding handlers when exception happens on first attempt
				{
					Iterable<TableHandler.GetObjectHandler> handlers = service.startGetObjectHandlers();
					TableHandler.GetObjectHandler handler = service.startGetObjectHandler();
					if(handler != null) {
						// Combine into a single list
						List<TableHandler.GetObjectHandler> merged = new ArrayList<>();
						for(TableHandler.GetObjectHandler h : handlers) {
							merged.add(h);
						}
						merged.add(handler);
						handlers = merged;
					}
					TableHandler.initGetObjectHandlers(handlers.iterator(), out, true);
				}
				{
					Iterable<TableHandler.GetTableHandler> handlers = service.startGetTableHandlers();
					TableHandler.GetTableHandler handler = service.startGetTableHandler();
					if(handler != null) {
						// Combine into a single list
						List<TableHandler.GetTableHandler> merged = new ArrayList<>();
						for(TableHandler.GetTableHandler h : handlers) {
							merged.add(h);
						}
						merged.add(handler);
						handlers = merged;
					}
					TableHandler.initGetTableHandlers(handlers.iterator(), out, true);
				}
				out.println(": Success");
			} catch(Exception e) {
				if(!started) failedServices.add(serviceAndState);
				out.println(": " + e.toString());
				logger.log(Level.SEVERE, null, e);
			}
		}
		if(!failedServices.isEmpty()) {
			if(isFirstStart) {
				out.println(failedServices.size() + " failed " + (failedServices.size() == 1 ? "service" : "services") + " will be retried");
			} else {
				out.println(failedServices.size() + " failed " + (failedServices.size() == 1 ? "service remains" : "services remain"));
			}
		}
		return failedServices;
	}

	private static void removeCacheListener(RequestSource source) {
		synchronized(cacheListeners) {
			int size=cacheListeners.size();
			for(int c=0;c<size;c++) {
				RequestSource O=cacheListeners.get(c);
				if(O==source) {
					cacheListeners.remove(c);
					break;
				}
			}
		}
	}

	/**
	 * Writes all rows of a results set.
	 *
	 * @return  The number of rows written
	 */
	public static <T extends AOServObject<?,?>> long writeObjects(
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		T obj,
		ResultSet results
	) throws IOException, SQLException {
		AoservProtocol.Version version = source.getProtocolVersion();

		// Make one pass counting the rows if providing progress information
		final long progressCount;
		if(provideProgress) {
			//progressCount = 0;
			//while (results.next()) progressCount++;
			if(results.last()) {
				progressCount = results.getRow();
				results.beforeFirst();
			} else {
				progressCount = 0;
			}
			out.writeByte(AoservProtocol.NEXT);
			if(version.compareTo(AoservProtocol.Version.VERSION_1_81_19) < 0) {
				if(progressCount > CompressedDataOutputStream.MAX_COMPRESSED_INT_VALUE) {
					throw new IOException(
						"Too many rows to send via " + CompressedDataOutputStream.class.getSimpleName() + ".writeCompressedInt: "
						+ progressCount + " > " + CompressedDataOutputStream.MAX_COMPRESSED_INT_VALUE
						+ ", please upgrade to client " + AoservProtocol.Version.VERSION_1_81_19 + " or newer.");
				}
				out.writeCompressedInt((int)progressCount);
			} else {
				out.writeLong(progressCount);
			}
		} else {
			progressCount = -1;
		}
		long rowCount = 0;
		while(results.next()) {
			obj.init(results);
			out.writeByte(AoservProtocol.NEXT);
			obj.write(out, version);
			rowCount++;
		}
		if(rowCount > CursorMode.AUTO_CURSOR_ABOVE) {
			logger.log(
				Level.WARNING,
				null,
				new SQLWarning(
					"Warning: provideProgress==true caused non-cursor select with more than "
					+ CursorMode.class.getSimpleName()
					+ ".AUTO_CURSOR_ABOVE ("
					+ CursorMode.AUTO_CURSOR_ABOVE
					+ ") rows: "
					+ rowCount
				)
			);
		}
		if(provideProgress && progressCount != rowCount) throw new AssertionError("progressCount != rowCount: " + progressCount + " != " + rowCount);
		return rowCount;
	}

	/**
	 * Writes all rows of a results set.
	 *
	 * @return  The number of rows written
	 */
	public static long writeObjects(
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		Collection<? extends AOServWritable> objs
	) throws IOException {
		AoservProtocol.Version version = source.getProtocolVersion();

		int size = objs.size();
		if (provideProgress) {
			out.writeByte(AoservProtocol.NEXT);
			out.writeCompressedInt(size);
		}
		long count = 0;
		for(AOServWritable obj : objs) {
			count++;
			if(count > size) throw new ConcurrentModificationException("Too many objects during iteration: " + count + " > " + size);
			out.writeByte(AoservProtocol.NEXT);
			obj.write(out, version);
		}
		if(count < size) throw new ConcurrentModificationException("Too few objects during iteration: " + count + " < " + size);
		return count;
	}

	/**
	 * Writes all rows of a results set while synchronizing on each object.
	 *
	 * @return  The number of rows written
	 */
	public static long writeObjectsSynced(
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		Collection<? extends AOServWritable> objs
	) throws IOException {
		AoservProtocol.Version version = source.getProtocolVersion();

		int size = objs.size();
		if (provideProgress) {
			out.writeByte(AoservProtocol.NEXT);
			out.writeCompressedInt(size);
		}
		long count = 0;
		for(AOServWritable obj : objs) {
			count++;
			if(count > size) throw new ConcurrentModificationException("Too many objects during iteration: " + count + " > " + size);
			out.writeByte(AoservProtocol.NEXT);
			synchronized(obj) {
				obj.write(out, version);
			}
		}
		if(count < size) throw new ConcurrentModificationException("Too few objects during iteration: " + count + " < " + size);
		return count;
	}

	public static String authenticate(
		DatabaseConnection conn,
		String remoteHost, 
		com.aoindustries.aoserv.client.account.User.Name connectAs, 
		com.aoindustries.aoserv.client.account.User.Name authenticateAs, 
		String password
	) throws IOException, SQLException {
		if(connectAs == null) return "Connection attempted with empty connect username";
		if(authenticateAs == null) return "Connection attempted with empty authentication username";

		if(!AccountHandler.isAdministrator(conn, authenticateAs)) return "Unable to find Administrator: "+authenticateAs;

		if(AccountHandler.isAdministratorDisabled(conn, authenticateAs)) return "Administrator disabled: "+authenticateAs;

		if (!isHostAllowed(conn, authenticateAs, remoteHost)) return "Connection from "+remoteHost+" as "+authenticateAs+" not allowed.";

		// Authenticate the client first
		if(password.length() == 0) return "Connection attempted with empty password";

		HashedPassword correctCrypted=AccountHandler.getAdministrator(conn, authenticateAs).getPassword();
		if(
			correctCrypted==null
			|| !correctCrypted.passwordMatches(password)
		) return "Connection attempted with invalid password";

		// If connectAs is not authenticateAs, must be authenticated with switch user permissions
		if(!connectAs.equals(authenticateAs)) {
			if(!AccountHandler.isAdministrator(conn, connectAs)) return "Unable to find Administrator: "+connectAs;
			// Must have can_switch_users permissions and must be switching to a subaccount user
			if(!AccountHandler.canSwitchUser(conn, authenticateAs, connectAs)) return "Not allowed to switch users from "+authenticateAs+" to "+connectAs;
		}

		// Let them in
		return null;
	}

	/**
	 * @see  #checkAccessHostname(MasterDatabaseConnection,RequestSource,String,String,String[])
	 */
	public static void checkAccessHostname(DatabaseConnection conn, RequestSource source, String action, String hostname) throws IOException, SQLException {
		checkAccessHostname(conn, source, action, hostname, MasterServer.getService(DnsService.class).getDNSTLDs(conn));
	}

	/**
	 * Determines if this hostname may be used by the source.  The <code>dns.ForbiddenZone</code>,
	 * <code>dns.Zone</code>, <code>web.VirtualHostName</code>, and <code>email.Domain</code> tables are searched, in this order,
	 * for a match.  If a match is found with an owner of this source, then access is
	 * granted.  If the source is not restricted by either server or business, then
	 * access is granted and the previous checks are avoided.
	 *
	 * TODO: What about ending '.' on zones vs DomainName objects here?
	 */
	public static void checkAccessHostname(DatabaseConnection conn, RequestSource source, String action, String hostname, List<DomainName> tlds) throws IOException, SQLException {
		String zone = ZoneTable.getDNSZoneForHostname(hostname, tlds);

		if(conn.executeBooleanQuery(
			"select (select zone from dns.\"ForbiddenZone\" where zone=?) is not null",
			zone
		)) throw new SQLException("Access to this hostname forbidden: Exists in dns.ForbiddenZone: "+hostname);

		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();

		String existingZone=conn.executeStringQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			"select zone from dns.\"Zone\" where zone=?",
			zone
		);
		if(existingZone!=null && !MasterServer.getService(DnsService.class).canAccessDNSZone(conn, source, existingZone)) throw new SQLException("Access to this hostname forbidden: Exists in dns.Zone: "+hostname);

		String domain = zone.substring(0, zone.length()-1);

		IntList httpdSites=conn.executeIntListQuery(
			"select\n"
			+ "  hsb.httpd_site\n"
			+ "from\n"
			+ "  web.\"VirtualHostName\" hsu,\n"
			+ "  web.\"VirtualHost\" hsb\n"
			+ "where\n"
			+ "  (hsu.hostname=? or hsu.hostname like ?)\n"
			+ "  and hsu.httpd_site_bind=hsb.id",
			domain,
			"%."+domain
		);
		// Must be able to access all of the sites
		for(int httpdSite : httpdSites) if(!WebHandler.canAccessSite(conn, source, httpdSite)) throw new SQLException("Access to this hostname forbidden: Exists in web.VirtualHostName: "+hostname);

		IntList emailDomains=conn.executeIntListQuery(
			"select id from email.\"Domain\" where (domain=? or domain like ?)",
			domain,
			"%."+domain
		);
		// Must be able to access all of the domains
		for(int emailDomain : emailDomains) if(!EmailHandler.canAccessDomain(conn, source, emailDomain)) throw new SQLException("Access to this hostname forbidden: Exists in email.Domain: "+hostname);
	}

	public static UserHost[] getUserHosts(DatabaseConnection conn, com.aoindustries.aoserv.client.account.User.Name user) throws IOException, SQLException {
		synchronized(masterServersLock) {
			if(masterServers==null) masterServers=new HashMap<>();
			UserHost[] mss=masterServers.get(user);
			if(mss!=null) return mss;
			try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement("select ms.* from master.\"User\" mu, master.\"UserHost\" ms where mu.is_active and mu.username=? and mu.username=ms.username")) {
				try {
					List<UserHost> v=new ArrayList<>();
					pstmt.setString(1, user.toString());
					try (ResultSet results = pstmt.executeQuery()) {
						while(results.next()) {
							UserHost ms=new UserHost();
							ms.init(results);
							v.add(ms);
						}
					}
					mss=new UserHost[v.size()];
					v.toArray(mss);
					masterServers.put(user, mss);
					return mss;
				} catch(SQLException e) {
					throw new WrappedSQLException(e, pstmt);
				}
			}
		}
	}

	public static Map<com.aoindustries.aoserv.client.account.User.Name,User> getUsers(DatabaseConnection conn) throws IOException, SQLException {
		synchronized(masterUsersLock) {
			if(masterUsers==null) {
				try (Statement stmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement()) {
					Map<com.aoindustries.aoserv.client.account.User.Name,User> table=new HashMap<>();
					ResultSet results=stmt.executeQuery("select * from master.\"User\" where is_active");
					while(results.next()) {
						User mu=new User();
						mu.init(results);
						table.put(mu.getKey(), mu);
					}
					masterUsers = Collections.unmodifiableMap(table);
				}
			}
			return masterUsers;
		}
	}

	public static User getUser(DatabaseConnection conn, com.aoindustries.aoserv.client.account.User.Name name) throws IOException, SQLException {
		return getUsers(conn).get(name);
	}

	/**
	 * Gets the hosts that are allowed for the provided username.
	 */
	public static boolean isHostAllowed(DatabaseConnection conn, com.aoindustries.aoserv.client.account.User.Name user, String host) throws IOException, SQLException {
		Map<com.aoindustries.aoserv.client.account.User.Name,List<HostAddress>> myMasterHosts;
		synchronized(masterHostsLock) {
			if(masterHosts==null) {
				try (Statement stmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement()) {
					Map<com.aoindustries.aoserv.client.account.User.Name,List<HostAddress>> table=new HashMap<>();
					ResultSet results=stmt.executeQuery("select mh.username, mh.host from master.\"UserAcl\" mh, master.\"User\" mu where mh.username=mu.username and mu.is_active");
					while(results.next()) {
						com.aoindustries.aoserv.client.account.User.Name un;
						HostAddress ho;
						try {
							un=com.aoindustries.aoserv.client.account.User.Name.valueOf(results.getString(1));
							ho=HostAddress.valueOf(results.getString(2));
						} catch(ValidationException e) {
							throw new SQLException(e);
						}
						List<HostAddress> sv=table.get(un);
						if(sv==null) table.put(un, sv=new SortedArrayList<>());
						sv.add(ho);
					}
					masterHosts = table;
				}
			}
			myMasterHosts = masterHosts;
		}
		if(getUser(conn, user)!=null) {
			List<HostAddress> hosts=myMasterHosts.get(user);
			// Allow from anywhere if no hosts are provided
			if(hosts==null) return true;
			String remoteHost=java.net.InetAddress.getByName(host).getHostAddress();
			int size = hosts.size();
			for (int c = 0; c < size; c++) {
				String tempAddress = java.net.InetAddress.getByName(hosts.get(c).toString()).getHostAddress();
				if (tempAddress.equals(remoteHost)) return true;
			}
			return false;
		} else {
			// Normal users can connect from any where
			return AccountHandler.getAdministrator(conn, user)!=null;
		}
	}

	/**
	 * Writes a single object, possibly null if there is no row, from a query.
	 * The query must result in either zero or one row.  If zero, {@code null}
	 * is written.  If more than one row, an {@link SQLException} is thrown.
	 *
	 * @throws SQLException when more than one row is in the result set
	 */
	public static void writeObject(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		AoservProtocol.Version version = source.getProtocolVersion();
		Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
		try (PreparedStatement pstmt = dbConn.prepareStatement(sql)) {
			try {
				DatabaseConnection.setParams(dbConn, pstmt, params);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						obj.init(results);
						if(results.next()) throw new SQLException("More than one row in result set");
						out.writeByte(AoservProtocol.NEXT);
						obj.write(out, version);
					} else {
						out.writeByte(AoservProtocol.DONE);
					}
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	/**
	 * Performs a query using a cursor and writes all rows of the result set.
	 *
	 * @return  The number of rows written
	 *
	 * @see  #writeObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.master.CursorMode, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...)
	 */
	private static long fetchObjects(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		AoservProtocol.Version version = source.getProtocolVersion();

		long progressCount;
		long rowCount;
		Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false);
		try (
			PreparedStatement pstmt = dbConn.prepareStatement(
				"DECLARE fetch_objects "
				+ (provideProgress ? "SCROLL" : "NO SCROLL")
				+ " CURSOR FOR\n"
				+ sql
			)
		) {
			try {
				DatabaseConnection.setParams(dbConn, pstmt, params);
				pstmt.executeUpdate();
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
		try (Statement stmt = dbConn.createStatement()) {
			try {
				final String fetchSql = "FETCH " + TableHandler.RESULT_SET_BATCH_SIZE + " FROM fetch_objects";

				// Make one pass counting the rows if providing progress information
				if(provideProgress) {
					try {
						progressCount = 0;
						while(true) {
							final int batchSize;
							try (ResultSet results = stmt.executeQuery(fetchSql)) {
								if(results.last()) {
									batchSize = results.getRow();
								} else {
									batchSize = 0;
								}
							}
							progressCount += batchSize;
							if(batchSize < TableHandler.RESULT_SET_BATCH_SIZE) break;
						}
					} catch(SQLException err) {
						throw new WrappedSQLException(err, fetchSql);
					}
					out.writeByte(AoservProtocol.NEXT);
					if(version.compareTo(AoservProtocol.Version.VERSION_1_81_19) < 0) {
						if(progressCount > CompressedDataOutputStream.MAX_COMPRESSED_INT_VALUE) {
							throw new IOException(
								"Too many rows to send via " + CompressedDataOutputStream.class.getSimpleName() + ".writeCompressedInt: "
								+ progressCount + " > " + CompressedDataOutputStream.MAX_COMPRESSED_INT_VALUE
								+ ", please upgrade to client " + AoservProtocol.Version.VERSION_1_81_19 + " or newer.");
						}
						out.writeCompressedInt((int)progressCount);
					} else {
						out.writeLong(progressCount);
					}
					// If progressCount is zero, no rows from query, short-cut here to avoid resetting and refetching empty results
					if(progressCount == 0) return 0;
					// Reset cursor
					final String resetSql = "FETCH ABSOLUTE 0 FROM fetch_objects";
					try {
						stmt.executeQuery(resetSql);
					} catch(SQLException err) {
						throw new WrappedSQLException(err, resetSql);
					}
				} else {
					progressCount = -1;
				}
				rowCount = 0;
				try {
					while(true) {
						int batchSize = 0;
						try (ResultSet results = stmt.executeQuery(fetchSql)) {
							while(results.next()) {
								obj.init(results);
								out.writeByte(AoservProtocol.NEXT);
								obj.write(out, version);
								batchSize++;
							}
						}
						rowCount += batchSize;
						if(batchSize < TableHandler.RESULT_SET_BATCH_SIZE) break;
					}
				} catch(SQLException err) {
					throw new WrappedSQLException(err, fetchSql);
				}
			} finally {
				final String closeSql = "CLOSE fetch_objects";
				try {
					stmt.executeUpdate(closeSql);
				} catch(SQLException err) {
					throw new WrappedSQLException(err, closeSql);
				}
			}
		}
		if(provideProgress && progressCount != rowCount) throw new AssertionError("progressCount != rowCount: " + progressCount + " != " + rowCount);
		return rowCount;
	}

	/**
	 * Performs a query without cursors and writes all rows of the result set.
	 *
	 * @return  The number of rows written
	 *
	 * @see  #writeObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.master.CursorMode, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...)
	 */
	private static long selectObjects(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
		try (
			PreparedStatement pstmt = dbConn.prepareStatement(
				sql,
				provideProgress ? ResultSet.TYPE_SCROLL_SENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY
			)
		) {
			try {
				DatabaseConnection.setParams(dbConn, pstmt, params);
				try (ResultSet results = pstmt.executeQuery()) {
					return writeObjects(source, out, provideProgress, obj, results);
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	/**
	 * Performs a query and writes all rows of the result set.
	 * Calls either {@link #selectObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...) selectObjects}
	 * or {@link #fetchObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...) fetchObjects}
	 * based on the {@link CursorMode}.
	 * <p>
	 * In particular, implements the {@link CursorMode#AUTO} mode for cursor selection.
	 * </p>
	 *
	 * @return  The number of rows written
	 *
	 * @see  CursorMode#AUTO
	 * @see  #selectObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...)}
	 * @see  #fetchObjects(com.aoindustries.dbc.DatabaseConnection, com.aoindustries.aoserv.master.RequestSource, com.aoindustries.io.CompressedDataOutputStream, boolean, com.aoindustries.aoserv.client.AOServObject, java.lang.String, java.lang.Object...)}
	 */
	public static long writeObjects(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		CursorMode cursorMode,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		if(cursorMode == CursorMode.FETCH) {
			return fetchObjects(conn, source, out, provideProgress, obj, sql, params);
		} else if(cursorMode == CursorMode.SELECT) {
			return selectObjects(conn, source, out, provideProgress, obj, sql, params);
		} else if(cursorMode == CursorMode.AUTO) {
			// TODO: More crafty selection here per CursorMode.AUTO description.  This is old behavior
			if(!provideProgress) {
				return fetchObjects(conn, source, out, provideProgress, obj, sql, params);
			} else {
				return selectObjects(conn, source, out, provideProgress, obj, sql, params);
			}
		} else {
			throw new AssertionError("Unexpected value for cursorMode: " + cursorMode);
		}
	}

	/**
	 * The query must result in precisely one row.
	 * If zero rows, {@link NoRowException} is thrown.
	 * If more than one row, an {@link SQLException} is thrown.
	 *
	 * @throws NoRowException when no rows are in the result set
	 * @throws SQLException when more than one row is in the result set
	 */
	public static void writePenniesCheckBusiness(
		DatabaseConnection conn,
		RequestSource source,
		String action,
		Account.Name account,
		CompressedDataOutputStream out,
		String sql,
		String param1,
		String param2,
		Timestamp param3
	) throws IOException, NoRowException, SQLException {
		AccountHandler.checkAccessAccount(conn, source, action, account);
		try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql)) {
			try {
				pstmt.setString(1, param1);
				pstmt.setString(2, param2);
				pstmt.setTimestamp(3, param3);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						int pennies = SQLUtility.parseDecimal2(results.getString(1));
						if(results.next()) throw new SQLException("More than one row in result set");
						out.writeByte(AoservProtocol.DONE);
						out.writeCompressedInt(pennies);
					} else {
						throw new NoRowException();
					}
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	/**
	 * The query must result in precisely one row.
	 * If zero rows, {@link NoRowException} is thrown.
	 * If more than one row, an {@link SQLException} is thrown.
	 *
	 * @throws NoRowException when no rows are in the result set
	 * @throws SQLException when more than one row is in the result set
	 */
	public static void writePenniesCheckBusiness(
		DatabaseConnection conn,
		RequestSource source,
		String action,
		Account.Name account,
		CompressedDataOutputStream out,
		String sql,
		String param1,
		String param2
	) throws IOException, NoRowException, SQLException {
		AccountHandler.checkAccessAccount(conn, source, action, account);
		try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql)) {
			try {
				pstmt.setString(1, param1);
				pstmt.setString(2, param2);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						int pennies = SQLUtility.parseDecimal2(results.getString(1));
						if(results.next()) throw new SQLException("More than one row in result set");
						out.writeByte(AoservProtocol.DONE);
						out.writeCompressedInt(pennies);
					} else {
						throw new NoRowException();
					}
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	public static void invalidateTable(Table.TableID tableID) {
		if(tableID==Table.TableID.MASTER_HOSTS) {
			synchronized(masterHostsLock) {
				masterHosts=null;
			}
		} else if(tableID==Table.TableID.MASTER_SERVERS) {
			synchronized(masterHostsLock) {
				masterHosts=null;
			}
			synchronized(masterServersLock) {
				masterServers=null;
			}
		} else if(tableID==Table.TableID.MASTER_USERS) {
			synchronized(masterHostsLock) {
				masterHosts=null;
			}
			synchronized(masterServersLock) {
				masterServers=null;
			}
			synchronized(masterUsersLock) {
				masterUsers=null;
			}
		}
	}

	public static void updateAOServProtocolLastUsed(DatabaseConnection conn, AoservProtocol.Version protocolVersion) throws IOException, SQLException {
		conn.executeUpdate("update schema.\"AoservProtocol\" set \"lastUsed\" = now()::date where version = ? and (\"lastUsed\" is null or \"lastUsed\" < now()::date)", protocolVersion.getVersion());
	}
}
