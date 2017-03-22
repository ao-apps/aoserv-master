/*
 * Copyright 2000-2013, 2014, 2015, 2017 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOSHCommand;
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.client.DNSRecord;
import com.aoindustries.aoserv.client.DNSZoneTable;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.IpReputationSet.AddReputation;
import com.aoindustries.aoserv.client.IpReputationSet.ConfidenceType;
import com.aoindustries.aoserv.client.IpReputationSet.ReputationType;
import com.aoindustries.aoserv.client.Language;
import com.aoindustries.aoserv.client.MasterProcess;
import com.aoindustries.aoserv.client.MasterServerStat;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.Transaction;
import com.aoindustries.aoserv.client.TransactionSearchCriteria;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.Gecos;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.HashedPassword;
import com.aoindustries.aoserv.client.validator.MySQLDatabaseName;
import com.aoindustries.aoserv.client.validator.MySQLTableName;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.PostgresDatabaseName;
import com.aoindustries.aoserv.client.validator.PostgresServerName;
import com.aoindustries.aoserv.client.validator.PostgresUserId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.dbc.NoRowException;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.io.FifoFile;
import com.aoindustries.io.FifoFileInputStream;
import com.aoindustries.io.FifoFileOutputStream;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.Email;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.net.Protocol;
import com.aoindustries.sql.AOConnectionPool;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.BufferManager;
import com.aoindustries.util.IntArrayList;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.ThreadUtility;
import com.aoindustries.util.Tuple2;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
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

	/**
	 * An unbounded executor for master-wide tasks.
	 */
	public final static ExecutorService executorService = Executors.newCachedThreadPool();

	/**
	 * The database values are read the first time this data is needed.
	 */
	private static final Object masterUsersLock = new Object();
	private static Map<UserId,MasterUser> masterUsers;
	private static final Object masterHostsLock = new Object();
	private static Map<UserId,List<HostAddress>> masterHosts;
	private static final Object masterServersLock = new Object();
	private static Map<UserId,com.aoindustries.aoserv.client.MasterServer[]> masterServers;

	/**
	 * The time the system started up
	 */
	private static final long startTime=System.currentTimeMillis();

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

	/**
	 * The last connector ID that was returned.
	 */
	private static final Object lastIDLock = new Object();
	private static long lastID=-1;

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

	public static long getNextConnectorID() {
		synchronized(lastIDLock) {
			long time=System.currentTimeMillis();
			long id;
			if(lastID<time) id=time;
			else id=lastID+1;
			lastID=id;
			return id;
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
		return startTime;
	}

	/** Used to avoid cloning of array for each access. */
	private static final AOServProtocol.CommandID[] commandIDs = AOServProtocol.CommandID.values();

	/** Copy used to avoid copying for each access. */
	private static final SchemaTable.TableID[] tableIDs = SchemaTable.TableID.values();

	static abstract class Response {

		abstract void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException;

		static final Response DONE = valueOf(AOServProtocol.DONE);

		/* TODO
		final byte resp1;

		short resp2Short=-1;
		boolean hasResp2Short=false;

		String resp2NullLongString = null;
		boolean hasResp2NullLongString = false;

		String resp2LongString = null;

		InboxAttributes resp2InboxAttributes=null;
		boolean hasResp2InboxAttributes=false;

		long[] resp2LongArray=null;

		String resp3String=null;

		String resp4String=null;
		int resp4Int=-1;
		boolean hasResp4Int=false;

		long resp5Long=-1;
		boolean hasResp5Long=false;
		 */

		static Response valueOf(int resp1) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
				}
			};
		}

		static Response valueOf(int resp1, int resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeCompressedInt(resp2);
				}
			};
		}

		static Response valueOf(int resp1, long resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLong(resp2);
				}
			};
		}

		static Response valueOf(int resp1, boolean resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeBoolean(resp2);
				}
			};
		}

		static Response valueOf(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
				}
			};
		}

		static Response valueOf(int resp1, AccountingCode resp2) {
			return valueOf(resp1, resp2.toString());
		}

		static Response valueOf(int resp1, MySQLDatabaseName resp2) {
			return valueOf(resp1, resp2.toString());
		}

		static Response valueOf(int resp1, PostgresDatabaseName resp2) {
			return valueOf(resp1, resp2.toString());
		}

		static Response valueOf(int resp1, long resp2, String resp3) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLong(resp2);
					out.writeUTF(resp3);
				}
			};
		}

		static Response valueOf(int resp1, long[] resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					for(int c=0;c<resp2.length;c++) out.writeLong(resp2[c]);
				}
			};
		}

		static Response valueOf(int resp1, InboxAttributes resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeBoolean(resp2!=null);
					if(resp2!=null) resp2.write(out, protocolVersion);
				}
			};
		}

		static Response valueOf(int resp1, String resp2, String resp3, String resp4) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
					out.writeUTF(resp3);
					out.writeUTF(resp4);
				}
			};
		}

		static Response valueOfNullLongString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeNullLongUTF(resp2);
				}
			};
		}

		static Response valueOfLongString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeLongUTF(resp2);
				}
			};
		}

		static Response valueOf(
			int resp1,
			String resp2,
			HostAddress resp3,
			int resp4,
			long resp5
		) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeUTF(resp2);
					out.writeUTF(resp3.toString());
					out.writeCompressedInt(resp4);
					out.writeLong(resp5);
				}
			};
		}

		static Response valueOfNullString(int resp1, String resp2) {
			return new Response() {
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					out.writeNullUTF(resp2);
				}
			};
		}
	}

	/* TODO
				@Override
				void writeResponse(CompressedDataOutputStream out, AOServProtocol.Version protocolVersion) throws IOException {
					out.writeByte(resp1);
					// response 2
					if(hasResp2Int) out.writeCompressedInt(resp2Int);
					else if(hasResp2Long) out.writeLong(resp2Long);
					else if(hasResp2Short) out.writeShort(resp2Short);
					else if(resp2String!=null) out.writeUTF(resp2String);
					else if(hasResp2NullLongString) out.writeNullLongUTF(resp2NullLongString);
					else if(resp2LongString!=null) out.writeLongUTF(resp2LongString);
					else if(hasResp2Boolean) out.writeBoolean(resp2Boolean);
					else if(hasResp2InboxAttributes) {
						out.writeBoolean(resp2InboxAttributes!=null);
						if(resp2InboxAttributes!=null) resp2InboxAttributes.write(out, protocolVersion);
					} else if(resp2LongArray!=null) {
						for(int c=0;c<resp2LongArray.length;c++) out.writeLong(resp2LongArray[c]);
					}
					// response 3
					if(resp3String!=null) out.writeUTF(resp3String);
					// response 4
					if(hasResp4Int) out.writeCompressedInt(resp4Int);
					else if(resp4String!=null) out.writeUTF(resp4String);
					// response 5
					if(hasResp5Long) out.writeLong(resp5Long);
				}
	*/

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
		CompressedDataInputStream in,
		CompressedDataOutputStream out,
		MasterProcess process
	) throws IOException, SQLException {
		// Time is not added for the cache invalidation connection
		boolean addTime=true;

		// The return value
		boolean keepOpen = true;

		process.commandCompleted();
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
				AOServProtocol.CommandID taskCode=commandIDs[taskCodeOrdinal];
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
								final AOServProtocol.Version protocolVersion = source.getProtocolVersion();
								final UserId username = source.getUsername();
								while(!BusinessHandler.isBusinessAdministratorDisabled(conn, username)) {
									conn.releaseConnection();
									process.commandSleeping();
									long endTime=System.currentTimeMillis()+60000;
									InvalidateCacheEntry ice;
									synchronized(source) {
										while((ice=source.getNextInvalidatedTables())==null) {
											long delay=endTime-System.currentTimeMillis();
											if(delay<=0 || delay>60000) break;
											try {
												source.wait(delay);
											} catch(InterruptedException err) {
												logger.log(Level.WARNING, null, err);
												// Restore the interrupted status
												Thread.currentThread().interrupt();
											}
										}
									}
									if(ice!=null) {
										process.commandRunning();
										IntList clientTableIDs=ice.getInvalidateList();
										int size=clientTableIDs.size();
										if(protocolVersion.compareTo(AOServProtocol.Version.VERSION_1_47)>=0) out.writeBoolean(ice.getCacheSyncID()!=null);
										out.writeCompressedInt(size);
										for(int c=0;c<size;c++) out.writeCompressedInt(clientTableIDs.getInt(c));
									} else {
										if(protocolVersion.compareTo(AOServProtocol.Version.VERSION_1_47)>=0) out.writeBoolean(true);
										out.writeCompressedInt(-1);
									}
									out.flush();

									if(ice!=null) {
										int server=ice.getServer();
										Long id=ice.getCacheSyncID();
										if(
											id!=null
											|| protocolVersion.compareTo(AOServProtocol.Version.VERSION_1_47)<0 // Before version 1.47 was always synchronous
										) {
											if(!in.readBoolean()) throw new IOException("Unexpected invalidate sync response.");
										}
										if(server!=-1 && id!=null) ServerHandler.removeInvalidateSyncEntry(server, id);
									} else {
										if(!in.readBoolean()) throw new IOException("Unexpected invalidate sync response.");
									}
								}
							} finally {
								conn.releaseConnection();
							}
						} finally {
							removeCacheListener(source);
						}
						return false;
					case PING :
						process.setCommand(AOSHCommand.PING);
						out.writeByte(AOServProtocol.DONE);
						done=true;
						break;
					case QUIT :
						process.setCommand("quit");
						addTime=false;
						concurrency.decrementAndGet();
						return false;
					case TEST_CONNECTION :
						process.setCommand("test_connection");
						out.writeByte(AOServProtocol.DONE);
						done=true;
						break;
					default :
						done=false;
				}
				if(!done) {
					// These commands automatically have the try/catch and the database connection releasing
					// And a finally block to reset thread priority
					boolean logIOException = true;
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
								if(BusinessHandler.isBusinessAdministratorDisabled(conn, source.getUsername())) throw new IOException("BusinessAdministrator disabled: "+source.getUsername());

								switch(taskCode) {
									case INVALIDATE_TABLE :
										{
											int clientTableID = in.readCompressedInt();
											SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
											int server;
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
												String hostname = in.readNullUTF();
												server = hostname==null ? -1 : ServerHandler.getServerForAOServerHostname(conn, hostname);
											} else {
												server = in.readCompressedInt();
											}
											process.setCommand(
												AOSHCommand.INVALIDATE,
												TableHandler.getTableName(
													conn,
													tableID
												),
												server==-1 ? null : server
											);
											TableHandler.invalidate(
												conn,
												source,
												invalidateList,
												tableID,
												server
											);
											resp = Response.DONE;
											sendInvalidateList=true;
										}
										break;
									case ADD : {
										int clientTableID = in.readCompressedInt();
										SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
										if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
										switch(tableID) {
											case BUSINESS_ADMINISTRATORS :
												{
													UserId username = UserId.valueOf(in.readUTF());
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
														source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_44)>=0
														? in.readBoolean()
														: false
													;
													process.setCommand(
														AOSHCommand.ADD_BUSINESS_ADMINISTRATOR,
														username,
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
													BusinessHandler.addBusinessAdministrator(
														conn,
														source,
														invalidateList,
														username,
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
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
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
													String billingEmail=in.readUTF().trim();
													String technicalContact=in.readUTF().trim();
													String technicalEmail=in.readUTF().trim();
													process.setCommand(
														AOSHCommand.ADD_BUSINESS_PROFILE,
														accounting,
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
														technicalContact,
														technicalEmail
													);
													int pkey=BusinessHandler.addBusinessProfile(
														conn,
														source,
														invalidateList,
														accounting,
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
														technicalContact,
														technicalEmail
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case BUSINESS_SERVERS :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													int server=in.readCompressedInt();
													if(
														source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_102)>=0
														&& source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0
													) {
														boolean can_configure_backup=in.readBoolean();
													}
													process.setCommand(
														AOSHCommand.ADD_BUSINESS_SERVER,
														accounting,
														server
													);
													int pkey=BusinessHandler.addBusinessServer(
														conn,
														source,
														invalidateList,
														accounting,
														server
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case BUSINESSES :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String contractVersion=in.readNullUTF();
													int defaultServer;
													String hostname;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
														defaultServer = -1;
														hostname = in.readUTF();
													} else {
														defaultServer = in.readCompressedInt();
														hostname = null;
													}
													AccountingCode parent = AccountingCode.valueOf(in.readUTF());
													boolean can_add_backup_servers=
														source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_102)>=0
														?in.readBoolean()
														:false
													;
													boolean can_add_businesses=in.readBoolean();
													boolean can_see_prices=
														source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_103)>=0
														?in.readBoolean()
														:true
													;
													boolean billParent=in.readBoolean();
													// Convert old hostname to servers.pkey
													if(defaultServer==-1) {
														defaultServer = ServerHandler.getServerForAOServerHostname(conn, hostname);
													}
													process.setCommand(
														AOSHCommand.ADD_BUSINESS,
														accounting,
														contractVersion,
														defaultServer,
														parent,
														can_add_backup_servers,
														can_add_businesses,
														can_see_prices,
														billParent
													);
													BusinessHandler.addBusiness(
														conn,
														source,
														invalidateList,
														accounting,
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
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_28)<=0) {
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
														throw new SQLException("add_credit_card for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
													}
													String processorName = in.readUTF();
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String groupName = in.readNullUTF();
													String cardInfo = in.readUTF().trim();
													String providerUniqueId = in.readUTF();
													String firstName = in.readUTF().trim();
													String lastName = in.readUTF().trim();
													String companyName = in.readNullUTF();
													String email = in.readNullUTF();
													String phone = in.readNullUTF();
													String fax = in.readNullUTF();
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
													String encryptedExpiration;
													int encryptionFrom;
													int encryptionRecipient;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
														encryptedCardNumber = null;
														encryptedExpiration = null;
														encryptionFrom = -1;
														encryptionRecipient = -1;
													} else {
														encryptedCardNumber = in.readNullUTF();
														encryptedExpiration = in.readNullUTF();
														encryptionFrom = in.readCompressedInt();
														encryptionRecipient = in.readCompressedInt();
													}

													process.setCommand(
														"add_credit_card",
														processorName,
														accounting,
														groupName,
														cardInfo,
														providerUniqueId,
														firstName,
														lastName,
														companyName,
														email,
														phone,
														fax,
														customerTaxId,
														streetAddress1,
														streetAddress2,
														city,
														state,
														postalCode,
														countryCode,
														principalName,
														description,
														encryptedCardNumber==null ? null : AOServProtocol.FILTERED,
														encryptedExpiration==null ? null : AOServProtocol.FILTERED,
														encryptionFrom==-1 ? null : encryptionFrom,
														encryptionRecipient==-1 ? null : encryptionRecipient
													);
													int pkey=CreditCardHandler.addCreditCard(
														conn,
														source,
														invalidateList,
														processorName,
														accounting,
														groupName,
														cardInfo,
														providerUniqueId,
														firstName,
														lastName,
														companyName,
														email,
														phone,
														fax,
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
														encryptedExpiration,
														encryptionFrom,
														encryptionRecipient
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case CREDIT_CARD_TRANSACTIONS :
												{
													String processor = in.readUTF();
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String groupName = in.readNullUTF();
													boolean testMode = in.readBoolean();
													int duplicateWindow = in.readCompressedInt();
													String orderNumber = in.readNullUTF();
													String currencyCode = in.readUTF();
													String amount = in.readUTF();
													String taxAmount = in.readNullUTF();
													boolean taxExempt = in.readBoolean();
													String shippingAmount = in.readNullUTF();
													String dutyAmount = in.readNullUTF();
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
													UserId creditCardCreatedBy = UserId.valueOf(in.readUTF());
													String creditCardPrincipalName = in.readNullUTF();
													AccountingCode creditCardAccounting = AccountingCode.valueOf(in.readUTF());
													String creditCardGroupName = in.readNullUTF();
													String creditCardProviderUniqueId = in.readNullUTF();
													String creditCardMaskedCardNumber = in.readUTF();
													String creditCardFirstName = in.readUTF();
													String creditCardLastName = in.readUTF();
													String creditCardCompanyName = in.readNullUTF();
													String creditCardEmail = in.readNullUTF();
													String creditCardPhone = in.readNullUTF();
													String creditCardFax = in.readNullUTF();
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
														accounting,
														groupName,
														testMode,
														duplicateWindow,
														orderNumber,
														currencyCode,
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
														creditCardFirstName,
														creditCardLastName,
														creditCardCompanyName,
														creditCardEmail,
														creditCardPhone,
														creditCardFax,
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
													int pkey=CreditCardHandler.addCreditCardTransaction(
														conn,
														source,
														invalidateList,
														processor,
														accounting,
														groupName,
														testMode,
														duplicateWindow,
														orderNumber,
														currencyCode,
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
														creditCardFirstName,
														creditCardLastName,
														creditCardCompanyName,
														creditCardEmail,
														creditCardPhone,
														creditCardFax,
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
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case CVS_REPOSITORIES :
												{
													int aoServer = in.readCompressedInt();
													UnixPath path = UnixPath.valueOf(in.readUTF());
													int lsa = in.readCompressedInt();
													int lsg = in.readCompressedInt();
													long mode = in.readLong();
													process.setCommand(
														AOSHCommand.ADD_CVS_REPOSITORY,
														aoServer,
														path,
														lsa,
														lsg,
														Long.toOctalString(mode)
													);
													int pkey = CvsHandler.addCvsRepository(
														conn,
														source,
														invalidateList,
														aoServer,
														path,
														lsa,
														lsg,
														mode
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case DISABLE_LOG :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String disableReason = in.readNullUTF();
													process.setCommand(
														"add_disable_log",
														accounting,
														disableReason
													);
													int pkey = BusinessHandler.addDisableLog(
														conn,
														source,
														invalidateList,
														accounting,
														disableReason
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
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
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_72)>=0) {
														weight         = in.readCompressedInt();
														port           = in.readCompressedInt();
													} else {
														weight         = DNSRecord.NO_WEIGHT;
														port           = DNSRecord.NO_PORT;
													}
													String destination = in.readUTF().trim();
													int ttl;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_127)>=0) {
														ttl            = in.readCompressedInt();
													} else {
														ttl            = DNSRecord.NO_TTL;
													}
													process.setCommand(
														AOSHCommand.ADD_DNS_RECORD,
														zone,
														domain,
														type,
														priority==DNSRecord.NO_PRIORITY ? null : priority,
														weight==DNSRecord.NO_WEIGHT ? null : weight,
														port==DNSRecord.NO_PORT ? null : port,
														destination,
														ttl==DNSRecord.NO_TTL ? null : ttl
													);
													int pkey = DNSHandler.addDNSRecord(
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
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case DNS_ZONES :
												{
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													String zone = in.readUTF().trim();
													InetAddress ip = InetAddress.valueOf(in.readUTF());
													int ttl = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_DNS_ZONE,
														packageName,
														zone,
														ip,
														ttl
													);
													DNSHandler.addDNSZone(
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
														AOSHCommand.ADD_EMAIL_ADDRESS,
														address,
														domain
													);
													int pkey=EmailHandler.addEmailAddress(
														conn,
														source,
														invalidateList,
														address,
														domain
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_DOMAINS :
												{
													DomainName domain = DomainName.valueOf(in.readUTF());
													int aoServer = in.readCompressedInt();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_EMAIL_DOMAIN,
														domain,
														aoServer,
														packageName
													);
													int pkey = EmailHandler.addEmailDomain(
														conn,
														source,
														invalidateList,
														domain,
														aoServer,
														packageName
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_FORWARDING :
												{
													int address = in.readCompressedInt();
													Email destination = Email.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_EMAIL_FORWARDING,
														address,
														destination
													);
													int pkey = EmailHandler.addEmailForwarding(
														conn,
														source,
														invalidateList,
														address,
														destination
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_LIST_ADDRESSES :
												{
													int address=in.readCompressedInt();
													int email_list=in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_EMAIL_LIST_ADDRESS,
														address,
														email_list
													);
													int pkey=EmailHandler.addEmailListAddress(
														conn,
														source,
														invalidateList,
														address,
														email_list
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_LISTS :
												{
													UnixPath path = UnixPath.valueOf(in.readUTF());
													int linuxServerAccount = in.readCompressedInt();
													int linuxServerGroup = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_EMAIL_LIST,
														path,
														linuxServerAccount,
														linuxServerGroup
													);
													int pkey = EmailHandler.addEmailList(
														conn,
														source,
														invalidateList,
														path,
														linuxServerAccount,
														linuxServerGroup
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_PIPE_ADDRESSES :
												{
													int address=in.readCompressedInt();
													int pipe=in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_EMAIL_PIPE_ADDRESS,
														address,
														pipe
													);
													int pkey=EmailHandler.addEmailPipeAddress(
														conn,
														source,
														invalidateList,
														address,
														pipe
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_PIPES :
												{
													int ao_server = in.readCompressedInt();
													String command = in.readUTF();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_EMAIL_PIPE,
														ao_server,
														command,
														packageName
													);
													int pkey=EmailHandler.addEmailPipe(
														conn,
														source,
														invalidateList,
														ao_server,
														command,
														packageName
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case EMAIL_SMTP_RELAYS :
												{
													process.setPriority(Thread.NORM_PRIORITY+1);
													currentThread.setPriority(Thread.NORM_PRIORITY+1);

													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													int aoServer= in.readCompressedInt();
													HostAddress host = HostAddress.valueOf(in.readUTF());
													String type=in.readUTF();
													long duration=in.readLong();
													process.setCommand(
														AOSHCommand.ADD_EMAIL_SMTP_RELAY,
														packageName,
														aoServer==-1?null:aoServer,
														host,
														type,
														duration
													);
													int pkey = EmailHandler.addEmailSmtpRelay(
														conn,
														source,
														invalidateList,
														packageName,
														aoServer,
														host,
														type,
														duration
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case FAILOVER_FILE_LOG :
												{
													int replication=in.readCompressedInt();
													long fflStartTime=in.readLong();
													long endTime=in.readLong();
													int scanned=in.readCompressedInt();
													int updated=in.readCompressedInt();
													long bytes=in.readLong();
													boolean isSuccessful=in.readBoolean();
													process.setCommand(
														"add_failover_file_log",
														replication,
														new java.util.Date(fflStartTime),
														new java.util.Date(endTime),
														scanned,
														updated,
														bytes,
														isSuccessful
													);
													int pkey=FailoverHandler.addFailoverFileLog(
														conn,
														source,
														invalidateList,
														replication,
														fflStartTime,
														endTime,
														scanned,
														updated,
														bytes,
														isSuccessful
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case FILE_BACKUP_SETTINGS :
												{
													int replication=in.readCompressedInt();
													String path=in.readUTF();
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
														int packageNum=in.readCompressedInt();
													}
													boolean backupEnabled;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
														short backupLevel=in.readShort();
														short backupRetention=in.readShort();
														boolean recurse=in.readBoolean();
														backupEnabled = backupLevel>0;
													} else {
														backupEnabled = in.readBoolean();
													}
													boolean required;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_62)>=0) {
														required = in.readBoolean();
													} else {
														required = false;
													}
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_31)<0) {
														throw new IOException(AOSHCommand.ADD_FILE_BACKUP_SETTING+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
													}
													process.setCommand(
														AOSHCommand.ADD_FILE_BACKUP_SETTING,
														replication,
														path,
														backupEnabled,
														required
													);
													int pkey = BackupHandler.addFileBackupSetting(
														conn,
														source,
														invalidateList,
														replication,
														path,
														backupEnabled,
														required
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case FTP_GUEST_USERS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_FTP_GUEST_USER,
														username
													);
													FTPHandler.addFTPGuestUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SHARED_TOMCATS :
												{
													String name=in.readUTF().trim();
													int aoServer=in.readCompressedInt();
													int version=in.readCompressedInt();
													UserId linuxServerAccount = UserId.valueOf(in.readUTF());
													GroupId linuxServerGroup = GroupId.valueOf(in.readUTF());
													boolean isSecure=in.readBoolean();
													boolean isOverflow=in.readBoolean();
													process.setCommand(
														AOSHCommand.ADD_HTTPD_SHARED_TOMCAT,
														name,
														aoServer,
														version,
														linuxServerAccount,
														linuxServerGroup,
														isSecure,
														isOverflow
													);
													int pkey = HttpdHandler.addHttpdSharedTomcat(
														conn,
														source,
														invalidateList,
														name,
														aoServer,
														version,
														linuxServerAccount,
														linuxServerGroup,
														isSecure,
														isOverflow,
														false
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case HTTPD_JBOSS_SITES :
												{
													int aoServer=in.readCompressedInt();
													String siteName=in.readUTF().trim();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													UserId username = UserId.valueOf(in.readUTF());
													GroupId group = GroupId.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache=in.readBoolean();
													int ipAddress=in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													int jBossVersion = in.readCompressedInt();
													UnixPath contentSrc = UnixPath.valueOf(in.readNullUTF());
													process.setCommand(
														AOSHCommand.ADD_HTTPD_JBOSS_SITE,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														jBossVersion,
														contentSrc
													);
													int pkey = HttpdHandler.addHttpdJBossSite(
														conn,
														source,
														invalidateList,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														jBossVersion,
														contentSrc
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
												{
													int httpd_site = in.readCompressedInt();
													String path = in.readUTF();
													boolean isRegularExpression = in.readBoolean();
													String authName = in.readUTF();
													UnixPath authGroupFile;
													{
														String s = in.readUTF();
														authGroupFile = s.isEmpty() ? null : UnixPath.valueOf(s);
													}
													UnixPath authUserFile;
													{
														String s = in.readUTF();
														authUserFile = s.isEmpty() ? null : UnixPath.valueOf(s);
													}
													String require = in.readUTF();
													process.setCommand(
														"add_httpd_site_authenticated_location",
														httpd_site,
														path,
														isRegularExpression,
														authName,
														authGroupFile,
														authUserFile,
														require
													);
													int pkey = HttpdHandler.addHttpdSiteAuthenticatedLocation(
														conn,
														source,
														invalidateList,
														httpd_site,
														path,
														isRegularExpression,
														authName,
														authGroupFile,
														authUserFile,
														require
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;							
											case HTTPD_SITE_URLS :
												{
													int hsb_pkey = in.readCompressedInt();
													DomainName hostname = DomainName.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_HTTPD_SITE_URL,
														hsb_pkey,
														hostname
													);
													int pkey = HttpdHandler.addHttpdSiteURL(
														conn,
														source,
														invalidateList,
														hsb_pkey,
														hostname
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case HTTPD_TOMCAT_CONTEXTS :
												{
													int tomcat_site = in.readCompressedInt();
													String className = in.readNullUTF();
													boolean cookies = in.readBoolean();
													boolean crossContext = in.readBoolean();
													UnixPath docBase = UnixPath.valueOf(in.readUTF());
													boolean override = in.readBoolean();
													String path = in.readUTF().trim();
													boolean privileged = in.readBoolean();
													boolean reloadable = in.readBoolean();
													boolean useNaming = in.readBoolean();
													String wrapperClass = in.readNullUTF();
													int debug = in.readCompressedInt();
													UnixPath workDir = UnixPath.valueOf(in.readNullUTF());
													process.setCommand(
														AOSHCommand.ADD_HTTPD_TOMCAT_CONTEXT,
														tomcat_site,
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
														workDir
													);
													int pkey = HttpdHandler.addHttpdTomcatContext(
														conn,
														source,
														invalidateList,
														tomcat_site,
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
														workDir
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;							
											case HTTPD_TOMCAT_DATA_SOURCES :
												{
													int tomcat_context=in.readCompressedInt();
													String name=in.readUTF();
													String driverClassName=in.readUTF();
													String url=in.readUTF();
													String username=in.readUTF();
													String password=in.readUTF();
													int maxActive=in.readCompressedInt();
													int maxIdle=in.readCompressedInt();
													int maxWait=in.readCompressedInt();
													String validationQuery=in.readUTF();
													if(validationQuery.length()==0) validationQuery=null;
													process.setCommand(
														AOSHCommand.ADD_HTTPD_TOMCAT_DATA_SOURCE,
														tomcat_context,
														name,
														driverClassName,
														url,
														username,
														AOServProtocol.FILTERED,
														maxActive,
														maxIdle,
														maxWait,
														validationQuery
													);
													int pkey = HttpdHandler.addHttpdTomcatDataSource(
														conn,
														source,
														invalidateList,
														tomcat_context,
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
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;							
											case HTTPD_TOMCAT_PARAMETERS :
												{
													int tomcat_context=in.readCompressedInt();
													String name=in.readUTF();
													String value=in.readUTF();
													boolean override=in.readBoolean();
													String description=in.readUTF();
													if(description.length()==0) description=null;
													process.setCommand(
														AOSHCommand.ADD_HTTPD_TOMCAT_PARAMETER,
														tomcat_context,
														name,
														value,
														override,
														description
													);
													int pkey = HttpdHandler.addHttpdTomcatParameter(
														conn,
														source,
														invalidateList,
														tomcat_context,
														name,
														value,
														override,
														description
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;							
											case HTTPD_TOMCAT_SHARED_SITES :
												{
													int aoServer = in.readCompressedInt();
													String siteName=in.readUTF().trim();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													UserId username = UserId.valueOf(in.readUTF());
													GroupId group = GroupId.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache = in.readBoolean();
													int ipAddress = in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													String sharedTomcatName=in.readNullUTF();
													int version=in.readCompressedInt();
													UnixPath contentSrc = UnixPath.valueOf(in.readNullUTF());
													process.setCommand(
														AOSHCommand.ADD_HTTPD_TOMCAT_SHARED_SITE,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														sharedTomcatName,
														version==-1?null:version,
														contentSrc
													);
													int pkey = HttpdHandler.addHttpdTomcatSharedSite(
														conn,
														source,
														invalidateList,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														sharedTomcatName,
														version,
														contentSrc
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case HTTPD_TOMCAT_STD_SITES :
												{
													int aoServer = in.readCompressedInt();
													String siteName = in.readUTF().trim();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													UserId username = UserId.valueOf(in.readUTF());
													GroupId group = GroupId.valueOf(in.readUTF());
													Email serverAdmin = Email.valueOf(in.readUTF());
													boolean useApache = in.readBoolean();
													int ipAddress = in.readCompressedInt();
													DomainName primaryHttpHostname = DomainName.valueOf(in.readUTF());
													int len = in.readCompressedInt();
													DomainName[] altHttpHostnames = new DomainName[len];
													for(int c=0;c<len;c++) altHttpHostnames[c] = DomainName.valueOf(in.readUTF());
													int tomcatVersion = in.readCompressedInt();
													UnixPath contentSrc = UnixPath.valueOf(in.readNullUTF());
													process.setCommand(
														AOSHCommand.ADD_HTTPD_TOMCAT_STD_SITE,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress==-1?null:ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														tomcatVersion,
														contentSrc
													);
													int pkey = HttpdHandler.addHttpdTomcatStdSite(
														conn,
														source,
														invalidateList,
														aoServer,
														siteName,
														packageName,
														username,
														group,
														serverAdmin,
														useApache,
														ipAddress,
														primaryHttpHostname,
														altHttpHostnames,
														tomcatVersion,
														contentSrc
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case LINUX_ACC_ADDRESSES :
												{
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_31)<0) {
														int address = in.readCompressedInt();
														String username = in.readUTF().trim();
														throw new IOException(AOSHCommand.ADD_LINUX_ACC_ADDRESS+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
													}
													int address = in.readCompressedInt();
													int lsa = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_LINUX_ACC_ADDRESS,
														address,
														lsa
													);
													int pkey = EmailHandler.addLinuxAccAddress(
														conn,
														source,
														invalidateList,
														address,
														lsa
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case LINUX_ACCOUNTS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													GroupId primary_group = GroupId.valueOf(in.readUTF());
													Gecos name = Gecos.valueOf(in.readUTF());
													Gecos office_location = Gecos.valueOf(in.readNullUTF());
													Gecos office_phone = Gecos.valueOf(in.readNullUTF());
													Gecos home_phone = Gecos.valueOf(in.readNullUTF());
													String type = in.readUTF().trim();
													UnixPath shell = UnixPath.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_LINUX_ACCOUNT,
														username,
														primary_group,
														name,
														office_location,
														office_phone,
														home_phone,
														type,
														shell
													);
													LinuxAccountHandler.addLinuxAccount(
														conn,
														source,
														invalidateList,
														username,
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
													GroupId groupName = GroupId.valueOf(in.readUTF());
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_LINUX_GROUP_ACCOUNT,
														groupName,
														username
													);
													int pkey = LinuxAccountHandler.addLinuxGroupAccount(
														conn,
														source,
														invalidateList,
														groupName,
														username,
														false,
														false
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case LINUX_GROUPS :
												{
													GroupId groupName = GroupId.valueOf(in.readUTF());
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													String type = in.readUTF().trim();
													process.setCommand(
														AOSHCommand.ADD_LINUX_GROUP,
														groupName,
														packageName,
														type
													);
													LinuxAccountHandler.addLinuxGroup(
														conn,
														source,
														invalidateList,
														groupName,
														packageName,
														type,
														false
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_SERVER_ACCOUNTS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													int aoServer = in.readCompressedInt();
													UnixPath home = UnixPath.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_LINUX_SERVER_ACCOUNT,
														username,
														aoServer,
														home
													);
													int pkey = LinuxAccountHandler.addLinuxServerAccount(
														conn,
														source,
														invalidateList,
														username,
														aoServer,
														home,
														false
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case LINUX_SERVER_GROUPS :
												{
													GroupId groupName = GroupId.valueOf(in.readUTF());
													int aoServer = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_LINUX_SERVER_GROUP,
														groupName,
														aoServer
													);
													int pkey = LinuxAccountHandler.addLinuxServerGroup(
														conn,
														source,
														invalidateList,
														groupName,
														aoServer,
														false
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case MAJORDOMO_LISTS :
												{
													int majordomoServer = in.readCompressedInt();
													String listName = in.readUTF().trim();
													process.setCommand(
														AOSHCommand.ADD_MAJORDOMO_LIST,
														majordomoServer,
														listName
													);
													int pkey = EmailHandler.addMajordomoList(
														conn,
														source,
														invalidateList,
														majordomoServer,
														listName
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
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
														AOSHCommand.ADD_MAJORDOMO_SERVER,
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
													MySQLDatabaseName name = MySQLDatabaseName.valueOf(in.readUTF());
													int mysqlServer = in.readCompressedInt();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_MYSQL_DATABASE,
														name,
														mysqlServer,
														packageName
													);
													int pkey = MySQLHandler.addMySQLDatabase(
														conn,
														source,
														invalidateList,
														name,
														mysqlServer,
														packageName
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case MYSQL_DB_USERS :
												{
													int mysql_database = in.readCompressedInt();
													int mysql_server_user = in.readCompressedInt();
													boolean canSelect=in.readBoolean();
													boolean canInsert=in.readBoolean();
													boolean canUpdate=in.readBoolean();
													boolean canDelete=in.readBoolean();
													boolean canCreate=in.readBoolean();
													boolean canDrop=in.readBoolean();
													boolean canIndex=in.readBoolean();
													boolean canAlter=in.readBoolean();
													boolean canCreateTempTable;
													boolean canLockTables;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_111)>=0) {
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
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)>=0) {
														canCreateView=in.readBoolean();
														canShowView=in.readBoolean();
														canCreateRoutine=in.readBoolean();
														canAlterRoutine=in.readBoolean();
														canExecute=in.readBoolean();
														if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_54)>=0) {
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
														AOSHCommand.ADD_MYSQL_DB_USER,
														mysql_database,
														mysql_server_user,
														canSelect,
														canInsert,
														canUpdate,
														canDelete,
														canCreate,
														canDrop,
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
													int pkey = MySQLHandler.addMySQLDBUser(
														conn,
														source,
														invalidateList,
														mysql_database,
														mysql_server_user,
														canSelect,
														canInsert,
														canUpdate,
														canDelete,
														canCreate,
														canDrop,
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
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case MYSQL_SERVER_USERS :
												{
													MySQLUserId username = MySQLUserId.valueOf(in.readUTF());
													int mysqlServer = in.readCompressedInt();
													String host = in.readNullUTF();
													process.setCommand(
														AOSHCommand.ADD_MYSQL_SERVER_USER,
														username,
														mysqlServer,
														host
													);
													int pkey = MySQLHandler.addMySQLServerUser(
														conn,
														source,
														invalidateList,
														username,
														mysqlServer,
														host
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case MYSQL_USERS :
												{
													MySQLUserId username = MySQLUserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_MYSQL_USER,
														username
													);
													MySQLHandler.addMySQLUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case NET_BINDS :
												{
													int server = in.readCompressedInt();
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													int ipAddress = in.readCompressedInt();
													Port port = Port.valueOf(
														in.readCompressedInt(),
														Protocol.valueOf(in.readUTF().toUpperCase(Locale.ROOT))
													);
													String appProtocol = in.readUTF().trim();
													boolean openFirewall = in.readBoolean();
													boolean monitoringEnabled;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_103)<=0) {
														monitoringEnabled = in.readCompressedInt() != -1;
														in.readNullUTF();
														in.readNullUTF();
														in.readNullUTF();
													} else {
														monitoringEnabled = in.readBoolean();
													}
													process.setCommand(
														AOSHCommand.ADD_NET_BIND,
														server,
														packageName,
														ipAddress,
														port,
														appProtocol,
														openFirewall,
														monitoringEnabled
													);
													int pkey = NetBindHandler.addNetBind(
														conn,
														source,
														invalidateList,
														server,
														packageName,
														ipAddress,
														port,
														appProtocol,
														openFirewall,
														monitoringEnabled
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case NOTICE_LOG :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String billingContact = in.readUTF().trim();
													String emailAddress = in.readUTF().trim();
													int balance = in.readCompressedInt();
													String type = in.readUTF().trim();
													int transid = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_NOTICE_LOG,
														accounting,
														billingContact,
														emailAddress,
														SQLUtility.getDecimal(balance),
														type,
														transid
													);
													BusinessHandler.addNoticeLog(
														conn,
														source,
														invalidateList,
														accounting,
														billingContact,
														emailAddress,
														balance,
														type,
														transid
													);
													resp = Response.DONE;
												}
												break;
											case PACKAGES :
												{
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													int packageDefinition;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_122)<=0) {
														// Try to find a package definition owned by the source accounting with matching rates and limits
														String level=in.readUTF().trim();
														int rate=in.readCompressedInt();
														int userLimit=in.readCompressedInt();
														int additionalUserRate=in.readCompressedInt();
														int popLimit=in.readCompressedInt();
														int additionalPopRate=in.readCompressedInt();
														AccountingCode baAccounting = UsernameHandler.getBusinessForUsername(conn, source.getUsername());
														packageDefinition=PackageHandler.findActivePackageDefinition(
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
																+ SQLUtility.getDecimal(rate)
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
														AOSHCommand.ADD_PACKAGE,
														packageName,
														accounting,
														packageDefinition
													);
													int pkey = PackageHandler.addPackage(
														conn,
														source,
														invalidateList,
														packageName,
														accounting,
														packageDefinition
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case PACKAGE_DEFINITIONS :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													String category = in.readUTF().trim();
													String name = in.readUTF().trim();
													String version = in.readUTF().trim();
													String display = in.readUTF().trim();
													String description = in.readUTF().trim();
													int setupFee = in.readCompressedInt();
													String setupFeeTransactionType = in.readNullUTF();
													int monthlyRate = in.readCompressedInt();
													String monthlyRateTransactionType = in.readUTF();
													process.setCommand(
														"add_package_definition",
														accounting,
														category,
														name,
														version,
														display,
														description,
														SQLUtility.getDecimal(setupFee),
														setupFeeTransactionType,
														SQLUtility.getDecimal(monthlyRate),
														monthlyRateTransactionType
													);
													int pkey = PackageHandler.addPackageDefinition(
														conn,
														source,
														invalidateList,
														accounting,
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
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case POSTGRES_DATABASES :
												{
													PostgresDatabaseName name = PostgresDatabaseName.valueOf(in.readUTF());
													int postgresServer = in.readCompressedInt();
													int datdba = in.readCompressedInt();
													int encoding = in.readCompressedInt();
													boolean enable_postgis = source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_27)>=0?in.readBoolean():false;
													process.setCommand(
														AOSHCommand.ADD_POSTGRES_DATABASE,
														name,
														postgresServer,
														datdba,
														encoding,
														enable_postgis
													);
													int pkey = PostgresHandler.addPostgresDatabase(
														conn,
														source,
														invalidateList,
														name,
														postgresServer,
														datdba,
														encoding,
														enable_postgis
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case POSTGRES_SERVER_USERS :
												{
													PostgresUserId username = PostgresUserId.valueOf(in.readUTF());
													int postgresServer = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.ADD_POSTGRES_SERVER_USER,
														username,
														postgresServer
													);
													int pkey = PostgresHandler.addPostgresServerUser(
														conn,
														source,
														invalidateList,
														username,
														postgresServer
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case POSTGRES_USERS :
												{
													PostgresUserId username = PostgresUserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_POSTGRES_USER,
														username
													);
													PostgresHandler.addPostgresUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case SIGNUP_REQUESTS :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
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
													UserId ba_username = UserId.valueOf(in.readUTF());
													String billing_contact = in.readUTF();
													String billing_email = in.readUTF();
													boolean billing_use_monthly = in.readBoolean();
													boolean billing_pay_one_year = in.readBoolean();
													// Encrypted values
													int from;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) from = 2; // Hard-coded value from AO website key
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
														accounting,
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
														ba_username,
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
													int pkey = SignupHandler.addSignupRequest(
														conn,
														source,
														invalidateList,
														accounting,
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
														ba_username,
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
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case SPAM_EMAIL_MESSAGES :
												{
													int esr = in.readCompressedInt();
													String message = in.readUTF().trim();
													process.setCommand(
														AOSHCommand.ADD_SPAM_EMAIL_MESSAGE,
														esr,
														message
													);
													int pkey = EmailHandler.addSpamEmailMessage(
														conn,
														source,
														invalidateList,
														esr,
														message
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case TICKETS :
												{
													AccountingCode brand;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_46)>=0) {
														brand = AccountingCode.valueOf(in.readUTF());
													} else {
														brand = BusinessHandler.getRootBusiness();
													}
													AccountingCode accounting;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_126)>=0) {
														accounting = AccountingCode.valueOf(in.readNullUTF());
													} else {
														AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
														accounting = PackageHandler.getBusinessForPackage(conn, packageName);
													}
													String language = source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_44)>=0 ? in.readUTF() : Language.EN;
													int category = source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_44)>=0 ? in.readCompressedInt() : -1;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) in.readUTF(); // username
													String type = in.readUTF();
													String fromAddress;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) fromAddress = in.readNullUTF();
													else fromAddress = null;
													String summary = source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_44)>=0 ? in.readUTF() : "(No summary)";
													String details = source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_44)>=0 ? in.readNullLongUTF() : in.readUTF();
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) in.readLong(); // deadline
													String clientPriority=in.readUTF();
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) in.readUTF(); // adminPriority
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) in.readNullUTF(); // technology
													String contactEmails;
													String contactPhoneNumbers;
													if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_125)>=0) {
														if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) in.readNullUTF(); // assignedTo
														contactEmails=in.readUTF();
														contactPhoneNumbers=in.readUTF();
													} else {
														contactEmails="";
														contactPhoneNumbers="";
													}
													process.setCommand(
														"add_ticket",
														brand,
														accounting,
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
													int pkey = TicketHandler.addTicket(
														conn,
														source,
														invalidateList,
														brand,
														accounting,
														language,
														category,
														type,
														fromAddress,
														summary,
														details,
														clientPriority,
														contactEmails,
														contactPhoneNumbers
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case TRANSACTIONS :
												{
													AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
													AccountingCode sourceAccounting = AccountingCode.valueOf(in.readUTF());
													UserId business_administrator = UserId.valueOf(in.readUTF());
													String type = in.readUTF().trim();
													String description = in.readUTF().trim();
													int quantity = in.readCompressedInt();
													int rate = in.readCompressedInt();
													String paymentType = in.readNullUTF();
													String paymentInfo = in.readNullUTF();
													String processor = in.readNullUTF();
													byte payment_confirmed = in.readByte();
													process.setCommand(
														AOSHCommand.ADD_TRANSACTION,
														accounting,
														sourceAccounting,
														business_administrator,
														type,
														description,
														SQLUtility.getMilliDecimal(quantity),
														SQLUtility.getDecimal(rate),
														paymentType,
														paymentInfo,
														processor,
														payment_confirmed==Transaction.CONFIRMED?"Y"
														:payment_confirmed==Transaction.NOT_CONFIRMED?"N"
														:"W"
													);
													int pkey = TransactionHandler.addTransaction(
														conn,
														source,
														invalidateList,
														accounting,
														sourceAccounting,
														business_administrator,
														type,
														description,
														quantity,
														rate,
														paymentType,
														paymentInfo,
														processor,
														payment_confirmed
													);
													resp = Response.valueOf(
														AOServProtocol.DONE,
														pkey
													);
												}
												break;
											case USERNAMES :
												{
													AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.ADD_USERNAME,
														packageName,
														username
													);
													UsernameHandler.addUsername(
														conn,
														source,
														invalidateList,
														packageName,
														username,
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_107)<=0) in.readUTF();
											int os_version=in.readCompressedInt();
											String username=in.readUTF();
											String password=in.readUTF();
											String contact_phone=in.readUTF();
											String contact_email=in.readUTF();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_107)<=0) throw new IOException("addBackupServer call not supported for AOServ Client version <= "+AOServProtocol.VERSION_1_0_A_107+", please upgrade AOServ Client.");
											process.setCommand(
												AOSHCommand.ADD_BACKUP_SERVER,
												hostname,
												farm,
												owner,
												description,
												os_version,
												username,
												AOServProtocol.FILTERED,
												contact_phone,
												contact_email
											);
											int pkey=ServerHandler.addBackupServer(
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
											resp1=AOServProtocol.DONE;
											resp2Int=pkey;
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
											process.setCommand(AOSHCommand.BOUNCE_TICKET);
											TicketHandler.bounceTicket(
												conn,
												source,
												invalidateList,
												ticketID,
												username,
												comments
											);
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case CANCEL_BUSINESS :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											String cancelReason = in.readNullUTF();
											process.setCommand(AOSHCommand.CANCEL_BUSINESS, accounting, cancelReason);
											BusinessHandler.cancelBusiness(conn, source, invalidateList, accounting, cancelReason);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									/*case CHANGE_TICKET_ADMIN_PRIORITY :
										{
											int ticketID=in.readCompressedInt(); 
											String priority=in.readUTF().trim();
											if(priority.length()==0) priority=null;
											String username=in.readUTF().trim();
											String comments=in.readUTF().trim();
											process.setCommand(
												AOSHCommand.CHANGE_TICKET_ADMIN_PRIORITY,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case CHANGE_TICKET_CLIENT_PRIORITY :
										{
											int ticketID = in.readCompressedInt();
											String clientPriority = in.readUTF();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) {
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48) < 0) {
												resp = Response.DONE;
											} else {
												resp = Response.valueOf(
													AOServProtocol.DONE,
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
												AOSHCommand.COMPLETE_TICKET,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case COMPARE_LINUX_SERVER_ACCOUNT_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readUTF();
											process.setCommand(
												AOSHCommand.COMPARE_LINUX_SERVER_ACCOUNT_PASSWORD,
												pkey,
												AOServProtocol.FILTERED
											);
											boolean result = LinuxAccountHandler.comparePassword(
												conn,
												source,
												pkey,
												password
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
												AOSHCommand.COPY_HOME_DIRECTORY,
												from_lsa,
												to_server
											);
											long byteCount = LinuxAccountHandler.copyHomeDirectory(
												conn,
												source,
												from_lsa,
												to_server
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
												AOSHCommand.COPY_LINUX_SERVER_ACCOUNT_PASSWORD,
												from_lsa,
												to_lsa
											);
											LinuxAccountHandler.copyLinuxServerAccountPassword(
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
											int pkey = in.readCompressedInt();
											process.setCommand(
												"copy_package_definition",
												pkey
											);
											int newPKey = PackageHandler.copyPackageDefinition(
												conn,
												source,
												invalidateList,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												newPKey
											);
											sendInvalidateList = true;
										}
										break;
									case CREDIT_CARD_DECLINED :
										{
											int transid = in.readCompressedInt();
											String reason = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.DECLINE_CREDIT_CARD,
												transid,
												reason
											);
											CreditCardHandler.creditCardDeclined(
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
											int pkey = in.readCompressedInt();
											String authorizationCommunicationResult = in.readNullUTF();
											String authorizationProviderErrorCode = in.readNullUTF();
											String authorizationErrorCode = in.readNullUTF();
											String authorizationProviderErrorMessage = in.readNullUTF();
											String authorizationProviderUniqueId = in.readNullUTF();
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
												pkey,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
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
												captureTime==0 ? null : new java.util.Date(captureTime),
												capturePrincipalName,
												captureCommunicationResult,
												captureProviderErrorCode,
												captureErrorCode,
												captureProviderErrorMessage,
												captureProviderUniqueId,
												status
											);
											CreditCardHandler.creditCardTransactionSaleCompleted(
												conn,
												source,
												invalidateList,
												pkey,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
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
											int pkey = in.readCompressedInt();
											String authorizationCommunicationResult = in.readNullUTF();
											String authorizationProviderErrorCode = in.readNullUTF();
											String authorizationErrorCode = in.readNullUTF();
											String authorizationProviderErrorMessage = in.readNullUTF();
											String authorizationProviderUniqueId = in.readNullUTF();
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
												pkey,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
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
											CreditCardHandler.creditCardTransactionAuthorizeCompleted(
												conn,
												source,
												invalidateList,
												pkey,
												authorizationCommunicationResult,
												authorizationProviderErrorCode,
												authorizationErrorCode,
												authorizationProviderErrorMessage,
												authorizationProviderUniqueId,
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
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											int disableLog = in.readCompressedInt();
											switch(tableID) {
												case BUSINESSES :
													{
														AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_BUSINESS,
															disableLog,
															accounting
														);
														BusinessHandler.disableBusiness(
															conn,
															source,
															invalidateList,
															disableLog,
															accounting
														);
													}
													break;
												case BUSINESS_ADMINISTRATORS :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_BUSINESS_ADMINISTRATOR,
															disableLog,
															username
														);
														BusinessHandler.disableBusinessAdministrator(
															conn,
															source,
															invalidateList,
															disableLog,
															username
														);
													}
													break;
												case CVS_REPOSITORIES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_CVS_REPOSITORY,
															disableLog,
															pkey
														);
														CvsHandler.disableCvsRepository(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case EMAIL_LISTS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_EMAIL_LIST,
															disableLog,
															pkey
														);
														EmailHandler.disableEmailList(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case EMAIL_PIPES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_EMAIL_PIPE,
															disableLog,
															pkey
														);
														EmailHandler.disableEmailPipe(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case EMAIL_SMTP_RELAYS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_EMAIL_SMTP_RELAY,
															disableLog,
															pkey
														);
														EmailHandler.disableEmailSmtpRelay(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case HTTPD_SHARED_TOMCATS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_HTTPD_SHARED_TOMCAT,
															disableLog,
															pkey
														);
														HttpdHandler.disableHttpdSharedTomcat(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case HTTPD_SITES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_HTTPD_SITE,
															disableLog,
															pkey
														);
														HttpdHandler.disableHttpdSite(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case HTTPD_SITE_BINDS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_HTTPD_SITE_BIND,
															disableLog,
															pkey
														);
														HttpdHandler.disableHttpdSiteBind(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case LINUX_ACCOUNTS :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_LINUX_ACCOUNT,
															disableLog,
															username
														);
														LinuxAccountHandler.disableLinuxAccount(
															conn,
															source,
															invalidateList,
															disableLog,
															username
														);
													}
													break;
												case LINUX_SERVER_ACCOUNTS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_LINUX_SERVER_ACCOUNT,
															disableLog,
															pkey
														);
														LinuxAccountHandler.disableLinuxServerAccount(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case MYSQL_SERVER_USERS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_MYSQL_SERVER_USER,
															disableLog,
															pkey
														);
														MySQLHandler.disableMySQLServerUser(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case MYSQL_USERS :
													{
														MySQLUserId username = MySQLUserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_MYSQL_USER,
															disableLog,
															username
														);
														MySQLHandler.disableMySQLUser(
															conn,
															source,
															invalidateList,
															disableLog,
															username
														);
													}
													break;
												case PACKAGES :
													{
														AccountingCode name = AccountingCode.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_PACKAGE,
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
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.DISABLE_POSTGRES_SERVER_USER,
															disableLog,
															pkey
														);
														PostgresHandler.disablePostgresServerUser(
															conn,
															source,
															invalidateList,
															disableLog,
															pkey
														);
													}
													break;
												case POSTGRES_USERS :
													{
														PostgresUserId username = PostgresUserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_POSTGRES_USER,
															disableLog,
															username
														);
														PostgresHandler.disablePostgresUser(
															conn,
															source,
															invalidateList,
															disableLog,
															username
														);
													}
													break;
												case USERNAMES :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.DISABLE_USERNAME,
															disableLog,
															username
														);
														UsernameHandler.disableUsername(
															conn,
															source,
															invalidateList,
															disableLog,
															username
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

											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.DUMP_MYSQL_DATABASE,
												pkey
											);
											MySQLHandler.dumpMySQLDatabase(
												conn,
												source,
												out,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case DUMP_POSTGRES_DATABASE :
										{
											process.setPriority(Thread.NORM_PRIORITY-1);
											currentThread.setPriority(Thread.NORM_PRIORITY-1);

											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.DUMP_POSTGRES_DATABASE,
												pkey
											);
											PostgresHandler.dumpPostgresDatabase(
												conn,
												source,
												out,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case ENABLE :
										{
											int clientTableID = in.readCompressedInt();
											SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											switch(tableID) {
												case BUSINESSES :
													{
														AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_BUSINESS,
															accounting
														);
														BusinessHandler.enableBusiness(
															conn,
															source,
															invalidateList,
															accounting
														);
													}
													break;
												case BUSINESS_ADMINISTRATORS :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_BUSINESS_ADMINISTRATOR,
															username
														);
														BusinessHandler.enableBusinessAdministrator(
															conn,
															source,
															invalidateList,
															username
														);
													}
													break;
												case CVS_REPOSITORIES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_CVS_REPOSITORY,
															pkey
														);
														CvsHandler.enableCvsRepository(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case EMAIL_LISTS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_EMAIL_LIST,
															pkey
														);
														EmailHandler.enableEmailList(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case EMAIL_PIPES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_EMAIL_PIPE,
															pkey
														);
														EmailHandler.enableEmailPipe(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case EMAIL_SMTP_RELAYS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_EMAIL_SMTP_RELAY,
															pkey
														);
														EmailHandler.enableEmailSmtpRelay(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case HTTPD_SHARED_TOMCATS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_HTTPD_SHARED_TOMCAT,
															pkey
														);
														HttpdHandler.enableHttpdSharedTomcat(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case HTTPD_SITES :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_HTTPD_SITE,
															pkey
														);
														HttpdHandler.enableHttpdSite(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case HTTPD_SITE_BINDS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_HTTPD_SITE_BIND,
															pkey
														);
														HttpdHandler.enableHttpdSiteBind(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case LINUX_ACCOUNTS :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_LINUX_ACCOUNT,
															username
														);
														LinuxAccountHandler.enableLinuxAccount(
															conn,
															source,
															invalidateList,
															username
														);
													}
													break;
												case LINUX_SERVER_ACCOUNTS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_LINUX_SERVER_ACCOUNT,
															pkey
														);
														LinuxAccountHandler.enableLinuxServerAccount(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case MYSQL_SERVER_USERS :
													{
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_MYSQL_SERVER_USER,
															pkey
														);
														MySQLHandler.enableMySQLServerUser(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case MYSQL_USERS :
													{
														MySQLUserId username = MySQLUserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_MYSQL_USER,
															username
														);
														MySQLHandler.enableMySQLUser(
															conn,
															source,
															invalidateList,
															username
														);
													}
													break;
												case PACKAGES :
													{
														AccountingCode name = AccountingCode.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_PACKAGE,
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
														int pkey = in.readCompressedInt();
														process.setCommand(
															AOSHCommand.ENABLE_POSTGRES_SERVER_USER,
															pkey
														);
														PostgresHandler.enablePostgresServerUser(
															conn,
															source,
															invalidateList,
															pkey
														);
													}
													break;
												case POSTGRES_USERS :
													{
														PostgresUserId username = PostgresUserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_POSTGRES_USER,
															username
														);
														PostgresHandler.enablePostgresUser(
															conn,
															source,
															invalidateList,
															username
														);
													}
													break;
												case USERNAMES :
													{
														UserId username = UserId.valueOf(in.readUTF());
														process.setCommand(
															AOSHCommand.ENABLE_USERNAME,
															username
														);
														UsernameHandler.enableUsername(
															conn,
															source,
															invalidateList,
															username
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
											AccountingCode template = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.GENERATE_ACCOUNTING,
												template
											);
											AccountingCode accounting = BusinessHandler.generateAccountingCode(
												conn,
												template
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												accounting
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_MYSQL_DATABASE_NAME :
										{
											String template_base = in.readUTF().trim();
											String template_added = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.GENERATE_MYSQL_DATABASE_NAME,
												template_base,
												template_added
											);
											MySQLDatabaseName name = MySQLHandler.generateMySQLDatabaseName(
												conn,
												template_base,
												template_added
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_PACKAGE_NAME :
										{
											AccountingCode template = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.GENERATE_PACKAGE_NAME,
												template
											);
											AccountingCode name = PackageHandler.generatePackageName(
												conn,
												template
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
												AOSHCommand.GENERATE_POSTGRES_DATABASE_NAME,
												template_base,
												template_added
											);
											PostgresDatabaseName name = PostgresHandler.generatePostgresDatabaseName(
												conn,
												template_base,
												template_added
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_SHARED_TOMCAT_NAME :
										{
											String template = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.GENERATE_SHARED_TOMCAT_NAME,
												template
											);
											String name = HttpdHandler.generateSharedTomcatName(
												conn,
												template
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GENERATE_SITE_NAME :
										{
											String template = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.GENERATE_SITE_NAME,
												template
											);
											String name = HttpdHandler.generateSiteName(
												conn,
												template
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												name
											);
											sendInvalidateList = false;
										}
										break;
									case GET_ACCOUNT_BALANCE :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												"get_account_balance",
												accounting
											);
											TransactionHandler.getAccountBalance(
												conn,
												source,
												out,
												accounting
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_ACCOUNT_BALANCE_BEFORE :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											long before = in.readLong();
											process.setCommand(
												"get_account_balance_before",
												accounting,
												new java.util.Date(before)
											);
											TransactionHandler.getAccountBalanceBefore(
												conn,
												source,
												out,
												accounting,
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
											BankAccountHandler.getBankTransactionsAccount(
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
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												"get_confirmed_account_balance",
												accounting
											);
											TransactionHandler.getConfirmedAccountBalance(
												conn,
												source,
												out,
												accounting
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_CONFIRMED_ACCOUNT_BALANCE_BEFORE :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											long before = in.readLong();
											process.setCommand(
												"get_confirmed_account_balance_before",
												accounting,
												new java.util.Date(before)
											);
											TransactionHandler.getConfirmedAccountBalanceBefore(
												conn,
												source,
												out,
												accounting,
												before
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_AUTORESPONDER_CONTENT :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_AUTORESPONDER_CONTENT,
												pkey
											);
											String content = LinuxAccountHandler.getAutoresponderContent(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												content
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AWSTATS_FILE :
										{
											int pkey = in.readCompressedInt();
											String path = in.readUTF();
											String queryString = in.readUTF();
											process.setCommand(
												AOSHCommand.GET_AWSTATS_FILE,
												pkey,
												path,
												queryString
											);
											HttpdHandler.getAWStatsFile(
												conn,
												source,
												pkey,
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
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_BACKUP_PARTITION_TOTAL_SIZE,
												pkey
											);
											long size = BackupHandler.getBackupPartitionTotalSize(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												size
											);
											sendInvalidateList = false;
										}
										break;
									case GET_BACKUP_PARTITION_DISK_USED_SIZE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_BACKUP_PARTITION_USED_SIZE,
												pkey
											);
											long size = BackupHandler.getBackupPartitionUsedSize(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												size
											);
											sendInvalidateList = false;
										}
										break;
									case GET_CACHED_ROW_COUNT :
										{
											int clientTableID = in.readCompressedInt();
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
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
											resp = Response.valueOf(
												AOServProtocol.DONE,
												count
											);
											sendInvalidateList = false;
										}
										break;
									case GET_CRON_TABLE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_CRON_TABLE,
												pkey
											);
											String cronTable = LinuxAccountHandler.getCronTable(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												cronTable
											);
											sendInvalidateList = false;
										}
										break;
									case GET_EMAIL_LIST_ADDRESS_LIST :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_EMAIL_LIST,
												pkey
											);
											String emailList = EmailHandler.getEmailListAddressList(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												emailList
											);
											sendInvalidateList = false;
										}
										break;
									case GET_FAILOVER_FILE_LOGS_FOR_REPLICATION :
										{
											int replication = in.readCompressedInt();
											int maxRows = in.readCompressedInt();
											FailoverHandler.getFailoverFileLogs(
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
												AOSHCommand.GET_FAILOVER_FILE_REPLICATION_ACTIVITY,
												replication
											);
											Tuple2<Long,String> activity = FailoverHandler.getFailoverFileReplicationActivity(
												conn,
												source,
												replication
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												activity.getElement1(),
												activity.getElement2()
											);
											sendInvalidateList = false;
										}
										break;
									case GET_IMAP_FOLDER_SIZES :
										{
											int pkey = in.readCompressedInt();
											int numFolders = in.readCompressedInt();
											String[] folderNames = new String[numFolders];
											for(int c=0;c<numFolders;c++) folderNames[c] = in.readUTF();
											process.setCommand(
												AOSHCommand.GET_IMAP_FOLDER_SIZES,
												pkey,
												numFolders,
												folderNames
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												EmailHandler.getImapFolderSizes(
													conn,
													source,
													pkey,
													folderNames
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_INBOX_ATTRIBUTES :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_INBOX_ATTRIBUTES,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												EmailHandler.getInboxAttributes(
													conn,
													source,
													pkey
												)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MAJORDOMO_INFO_FILE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_MAJORDOMO_INFO_FILE,
												pkey
											);
											String file = EmailHandler.getMajordomoInfoFile(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												file
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MAJORDOMO_INTRO_FILE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_MAJORDOMO_INTRO_FILE,
												pkey
											);
											String file = EmailHandler.getMajordomoIntroFile(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												file
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
											out.writeByte(AOServProtocol.DONE);
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
											resp = Response.valueOf(
												AOServProtocol.DONE,
												RandomHandler.getMasterEntropyNeeded(conn, source)
											);
											sendInvalidateList = false;
										}
										break;
									case GET_MRTG_FILE :
										{
											int aoServer = in.readCompressedInt();
											String filename = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.GET_MRTG_FILE,
												aoServer,
												filename
											);
											AOServerHandler.getMrtgFile(
												conn,
												source,
												aoServer,
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
											MySQLHandler.getMasterStatus(
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
											MySQLHandler.getSlaveStatus(
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_60)>=0) {
												mysqlSlave = in.readCompressedInt();
											} else {
												mysqlSlave = -1;
											}
											process.setCommand(
												"get_mysql_table_status",
												mysqlDatabase,
												mysqlSlave==-1 ? null : mysqlSlave
											);
											MySQLHandler.getTableStatus(
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_60)>=0) {
												mysqlSlave = in.readCompressedInt();
											} else {
												mysqlSlave = -1;
											}
											int numTables = in.readCompressedInt();
											List<MySQLTableName> tableNames = new ArrayList<>(numTables);
											for(int c=0;c<numTables;c++) {
												tableNames.add(MySQLTableName.valueOf(in.readUTF()));
											}
											process.setCommand(
												"check_mysql_tables",
												mysqlDatabase,
												mysqlSlave==-1 ? null : mysqlSlave,
												tableNames
											);
											MySQLHandler.checkTables(
												conn,
												source,
												mysqlDatabase,
												mysqlSlave,
												tableNames,
												out
											);
											resp = null;
											sendInvalidateList = false;
										}
										break;
									case GET_NET_DEVICE_BONDING_REPORT :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_net_device_bonding_report",
												pkey
											);
											String report = NetDeviceHandler.getNetDeviceBondingReport(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_NET_DEVICE_STATISTICS_REPORT :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_net_device_statistics_report",
												pkey
											);
											String report = NetDeviceHandler.getNetDeviceStatisticsReport(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_3WARE_RAID_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_3ware_raid_report",
												aoServer
											);
											String report = AOServerHandler.get3wareRaidReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MD_STAT_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_md_stat_report",
												aoServer
											);
											String report = AOServerHandler.getMdStatReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MD_MISMATCH_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_md_mismatch_report",
												aoServer
											);
											String report = AOServerHandler.getMdMismatchReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_DRBD_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_drbd_report",
												aoServer
											);
											String report = AOServerHandler.getDrbdReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_LVM_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_lvm_report",
												aoServer
											);
											String[] report = AOServerHandler.getLvmReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report[0],
												report[1],
												report[2]
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_HDD_TEMP_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_hdd_temp_report",
												aoServer
											);
											String report = AOServerHandler.getHddTempReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_HDD_MODEL_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_hdd_model_report",
												aoServer
											);
											String report = AOServerHandler.getHddModelReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_FILESYSTEMS_CSV_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_filesystems_csv_report",
												aoServer
											);
											String report = AOServerHandler.getFilesystemsCsvReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_LOADAVG_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_loadavg_report",
												aoServer
											);
											String report = AOServerHandler.getLoadAvgReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_MEMINFO_REPORT :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_meminfo_report",
												aoServer
											);
											String report = AOServerHandler.getMemInfoReport(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												report
											);
											sendInvalidateList = false;
										}
										break;
									case AO_SERVER_CHECK_PORT :
										{
											int aoServer = in.readCompressedInt();
											InetAddress ipAddress = InetAddress.valueOf(in.readUTF());
											Port port = Port.valueOf(
												in.readCompressedInt(),
												Protocol.valueOf(in.readUTF().toUpperCase(Locale.ROOT))
											);
											String appProtocol = in.readUTF();
											String monitoringParameters = in.readUTF();
											process.setCommand(
												"ao_server_check_port",
												aoServer,
												ipAddress,
												port,
												appProtocol,
												monitoringParameters
											);
											// Do not log any IO exception
											logIOException = false;
											String result = AOServerHandler.checkPort(
												conn,
												source,
												aoServer,
												ipAddress,
												port,
												appProtocol,
												monitoringParameters
											);
											logIOException = true;
											resp = Response.valueOf(
												AOServProtocol.DONE,
												result
											);
											sendInvalidateList = false;
										}
										break;
									case AO_SERVER_CHECK_SMTP_BLACKLIST :
										{
											int aoServer = in.readCompressedInt();
											InetAddress sourceIp = InetAddress.valueOf(in.readUTF());
											InetAddress connectIp = InetAddress.valueOf(in.readUTF());
											process.setCommand(
												"ao_server_check_smtp_blacklist",
												aoServer,
												sourceIp,
												connectIp
											);
											// Do not log any IO exception
											logIOException = false;
											String result = AOServerHandler.checkSmtpBlacklist(
												conn,
												source,
												aoServer,
												sourceIp,
												connectIp
											);
											logIOException = true;
											resp = Response.valueOf(
												AOServProtocol.DONE,
												result
											);
											sendInvalidateList = false;
										}
										break;
									case GET_AO_SERVER_SYSTEM_TIME_MILLIS :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												"get_ao_server_system_time_millis",
												aoServer
											);
											long systemTime = AOServerHandler.getSystemTimeMillis(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												systemTime
											);
											sendInvalidateList = false;
										}
										break;
									case GET_UPS_STATUS :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_UPS_STATUS,
												aoServer
											);
											String status = AOServerHandler.getUpsStatus(
												conn,
												source,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												status
											);
											sendInvalidateList = false;
										}
										break;
									case GET_OBJECT :
										{
											int clientTableID = in.readCompressedInt();
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
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
									case GET_PENDING_PAYMENTS :
										{
											boolean provideProgress = in.readBoolean();
											process.setCommand(
												"get_pending_payments",
												provideProgress
											);
											TransactionHandler.getPendingPayments(
												conn,
												source,
												out,
												provideProgress
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_ROOT_BUSINESS :
										{
											process.setCommand(AOSHCommand.GET_ROOT_BUSINESS);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												BusinessHandler.getRootBusiness()
											);
											sendInvalidateList = false;
										}
										break;
									case GET_ROW_COUNT :
										{
											int clientTableID = in.readCompressedInt();
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
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
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
											EmailHandler.getSpamEmailMessagesForEmailSmtpRelay(
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
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) {
												writeObjects(source, out, provideProgress, new ArrayList<>());
											} else {
												if(
													tableID == SchemaTable.TableID.DISTRO_FILES
												) {
													process.setPriority(Thread.NORM_PRIORITY-1);
													currentThread.setPriority(Thread.NORM_PRIORITY-1);
												}
												process.setCommand(
													AOSHCommand.SELECT,
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
													in,
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
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_details",
												pkey
											);
											String details = TicketHandler.getTicketDetails(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												details
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_RAW_EMAIL :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_raw_email",
												pkey
											);
											String rawEmail = TicketHandler.getTicketRawEmail(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												rawEmail
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_INTERNAL_NOTES :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_internal_notes",
												pkey
											);
											String internalNotes = TicketHandler.getTicketInternalNotes(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfLongString(
												AOServProtocol.DONE,
												internalNotes
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_OLD_VALUE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_old_value",
												pkey
											);
											String oldValue = TicketHandler.getTicketActionOldValue(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												oldValue
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_NEW_VALUE :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_new_value",
												pkey
											);
											String newValue = TicketHandler.getTicketActionNewValue(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												newValue
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_DETAILS :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_details",
												pkey
											);
											String details = TicketHandler.getTicketActionDetails(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												details
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TICKET_ACTION_RAW_EMAIL :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_ticket_action_raw_email",
												pkey
											);
											String rawEmail = TicketHandler.getTicketActionRawEmail(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullLongString(
												AOServProtocol.DONE,
												rawEmail
											);
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_BUSINESS :
										{
											boolean provideProgress = in.readBoolean();
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												"get_transactions_business",
												provideProgress,
												accounting
											);
											TransactionHandler.getTransactionsBusiness(
												conn,
												source,
												out,
												provideProgress,
												accounting
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_BUSINESS_ADMINISTRATOR :
										{
											boolean provideProgress = in.readBoolean();
											UserId username = UserId.valueOf(in.readUTF());
											process.setCommand(
												"get_transactions_business_administrator",
												provideProgress,
												username
											);
											TransactionHandler.getTransactionsBusinessAdministrator(
												conn,
												source,
												out,
												provideProgress,
												username
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case GET_TRANSACTIONS_SEARCH :
										{
											boolean provideProgress = in.readBoolean();
											TransactionSearchCriteria criteria = new TransactionSearchCriteria();
											criteria.read(in);
											process.setCommand(
												"get_transactions_search",
												provideProgress,
												"..."
											);
											TransactionHandler.getTransactionsSearch(
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
											int pkey = in.readCompressedInt();
											process.setCommand(
												"get_whois_history_whois_output",
												pkey
											);
											String whoisOutput = DNSHandler.getWhoisHistoryOutput(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												whoisOutput
											);
											sendInvalidateList = false;
										}
										break;
									/*case HOLD_TICKET :
										{
											int ticketID=in.readCompressedInt();
											String comments=in.readUTF().trim();
											process.setCommand(
												AOSHCommand.HOLD_TICKET,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									/*case INITIALIZE_HTTPD_SITE_PASSWD_FILE :
										{
											int sitePKey=in.readCompressedInt();
											String username=in.readUTF().trim();
											String encPassword=in.readUTF();
											process.setCommand(
												AOSHCommand.INITIALIZE_HTTPD_SITE_PASSWD_FILE,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=false;
										}
										break;*/
									case IS_ACCOUNTING_AVAILABLE :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_ACCOUNTING_AVAILABLE,
												accounting
											);
											boolean isAvailable = BusinessHandler.isAccountingAvailable(
												conn,
												accounting
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET :
										{
											UserId username = UserId.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET,
												username
											);
											boolean isAvailable = BusinessHandler.isBusinessAdministratorPasswordSet(
												conn,
												source,
												username
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_DNS_ZONE_AVAILABLE :
										{
											String zone = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.IS_DNS_ZONE_AVAILABLE,
												zone
											);
											boolean isAvailable = DNSHandler.isDNSZoneAvailable(
												conn,
												zone
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_EMAIL_DOMAIN_AVAILABLE :
										{
											int aoServer = in.readCompressedInt();
											DomainName domain = DomainName.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_EMAIL_DOMAIN_AVAILABLE,
												aoServer,
												domain
											);
											boolean isAvailable = EmailHandler.isEmailDomainAvailable(
												conn,
												source,
												aoServer,
												domain
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_GROUP_NAME_AVAILABLE :
										{
											GroupId name = GroupId.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_LINUX_GROUP_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = LinuxAccountHandler.isLinuxGroupNameAvailable(
												conn,
												name
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET,
												pkey
											);
											boolean isAvailable = LinuxAccountHandler.isLinuxServerAccountPasswordSet(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL,
												pkey
											);
											int isManual = LinuxAccountHandler.isLinuxServerAccountProcmailManual(
												conn,
												source,
												pkey
											);
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_6)>=0) {
												resp = Response.valueOf(
													AOServProtocol.DONE,
													isManual
												);
											} else {
												if(isManual==AOServProtocol.FALSE) {
													resp = Response.valueOf(
														AOServProtocol.DONE,
														false
													);
												} else if(isManual==AOServProtocol.TRUE) {
													resp = Response.valueOf(
														AOServProtocol.DONE,
														true
													);
												} else {
													throw new IOException("Unsupported value for AOServClient protocol < "+AOServProtocol.Version.VERSION_1_6);
												}
											}
											sendInvalidateList = false;
										}
										break;
									case IS_MYSQL_DATABASE_NAME_AVAILABLE :
										{
											MySQLDatabaseName name = MySQLDatabaseName.valueOf(in.readUTF());
											int mysqlServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_MYSQL_DATABASE_NAME_AVAILABLE,
												name,
												mysqlServer
											);
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)<0) throw new IOException(AOSHCommand.IS_MYSQL_DATABASE_NAME_AVAILABLE+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_4+", please upgrade AOServ Client.");
											boolean isAvailable = MySQLHandler.isMySQLDatabaseNameAvailable(
												conn,
												source,
												name,
												mysqlServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_MYSQL_SERVER_USER_PASSWORD_SET :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_MYSQL_SERVER_USER_PASSWORD_SET,
												pkey
											);
											boolean isSet = MySQLHandler.isMySQLServerUserPasswordSet(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isSet
											);
											sendInvalidateList = false;
										}
										break;
									case IS_PACKAGE_NAME_AVAILABLE :
										{
											AccountingCode name = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_PACKAGE_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = PackageHandler.isPackageNameAvailable(
												conn,
												name
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_DATABASE_NAME_AVAILABLE :
										{
											PostgresDatabaseName name = PostgresDatabaseName.valueOf(in.readUTF());
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_POSTGRES_DATABASE_NAME_AVAILABLE,
												name,
												postgresServer
											);
											boolean isAvailable = PostgresHandler.isPostgresDatabaseNameAvailable(
												conn,
												source,
												name,
												postgresServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_SERVER_USER_PASSWORD_SET :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_POSTGRES_SERVER_USER_PASSWORD_SET,
												pkey
											);
											boolean isAvailable = PostgresHandler.isPostgresServerUserPasswordSet(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_POSTGRES_SERVER_NAME_AVAILABLE :
										{
											PostgresServerName name = PostgresServerName.valueOf(in.readUTF());
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.IS_POSTGRES_SERVER_NAME_AVAILABLE,
												name,
												aoServer
											);
											boolean isAvailable = PostgresHandler.isPostgresServerNameAvailable(
												conn,
												source,
												name,
												aoServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_SHARED_TOMCAT_NAME_AVAILABLE :
										{
											String name = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.IS_SHARED_TOMCAT_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = HttpdHandler.isSharedTomcatNameAvailable(
												conn,
												name
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_USERNAME_AVAILABLE :
										{
											UserId username = UserId.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.IS_USERNAME_AVAILABLE,
												username
											);
											boolean isAvailable = UsernameHandler.isUsernameAvailable(
												conn,
												username
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												isAvailable
											);
											sendInvalidateList = false;
										}
										break;
									case IS_SITE_NAME_AVAILABLE :
										{
											String name = in.readUTF().trim();
											process.setCommand(
												AOSHCommand.IS_SITE_NAME_AVAILABLE,
												name
											);
											boolean isAvailable = HttpdHandler.isSiteNameAvailable(
												conn,
												name
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
												AOSHCommand.KILL_TICKET,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case MOVE_IP_ADDRESS :
										{
											int ipAddress = in.readCompressedInt();
											int toServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.MOVE_IP_ADDRESS,
												ipAddress,
												toServer
											);
											IPAddressHandler.moveIPAddress(
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
												AOSHCommand.REACTIVATE_TICKET,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case REFRESH_EMAIL_SMTP_RELAY :
										{
											process.setPriority(Thread.NORM_PRIORITY + 1);
											currentThread.setPriority(Thread.NORM_PRIORITY + 1);

											int pkey = in.readCompressedInt();
											long min_duration = in.readLong();
											process.setCommand(
												AOSHCommand.REFRESH_EMAIL_SMTP_RELAY,
												pkey,
												min_duration
											);
											EmailHandler.refreshEmailSmtpRelay(
												conn,
												source,
												invalidateList,
												pkey,
												min_duration
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case REMOVE : {
										int clientTableID = in.readCompressedInt();
										SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
										if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
										switch(tableID) {
											case BLACKHOLE_EMAIL_ADDRESSES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_BLACKHOLE_EMAIL_ADDRESS,
														pkey
													);
													EmailHandler.removeBlackholeEmailAddress(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case BUSINESS_ADMINISTRATORS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_BUSINESS_ADMINISTRATOR,
														username
													);
													BusinessHandler.removeBusinessAdministrator(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case BUSINESS_SERVERS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_BUSINESS_SERVER,
														pkey
													);
													BusinessHandler.removeBusinessServer(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case CREDIT_CARDS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_CREDIT_CARD,
														pkey
													);
													CreditCardHandler.removeCreditCard(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case CVS_REPOSITORIES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_CVS_REPOSITORY,
														pkey
													);
													CvsHandler.removeCvsRepository(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case DNS_RECORDS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_DNS_RECORD,
														pkey
													);
													DNSHandler.removeDNSRecord(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case DNS_ZONES :
												{
													String zone = in.readUTF().trim();
													process.setCommand(
														AOSHCommand.REMOVE_DNS_ZONE,
														zone
													);
													DNSHandler.removeDNSZone(
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
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_ADDRESS,
														pkey
													);
													EmailHandler.removeEmailAddress(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_DOMAINS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_DOMAIN,
														pkey
													);
													EmailHandler.removeEmailDomain(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_FORWARDING :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_FORWARDING,
														pkey
													);
													EmailHandler.removeEmailForwarding(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_LIST_ADDRESSES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_LIST_ADDRESS,
														pkey
													);
													EmailHandler.removeEmailListAddress(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_LISTS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_LIST,
														pkey
													);
													EmailHandler.removeEmailList(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_PIPE_ADDRESSES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_PIPE_ADDRESS,
														pkey
													);
													EmailHandler.removeEmailPipeAddress(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_PIPES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_PIPE,
														pkey
													);
													EmailHandler.removeEmailPipe(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case EMAIL_SMTP_RELAYS :
												{
													process.setPriority(Thread.NORM_PRIORITY + 1);
													currentThread.setPriority(Thread.NORM_PRIORITY + 1);

													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_EMAIL_SMTP_RELAY,
														pkey
													);
													EmailHandler.removeEmailSmtpRelay(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case FILE_BACKUP_SETTINGS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_FILE_BACKUP_SETTING,
														pkey
													);
													BackupHandler.removeFileBackupSetting(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case FTP_GUEST_USERS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_FTP_GUEST_USER,
														username
													);
													FTPHandler.removeFTPGuestUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SHARED_TOMCATS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_SHARED_TOMCAT,
														pkey
													);
													HttpdHandler.removeHttpdSharedTomcat(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														"remove_httpd_site_authenticated_location",
														pkey
													);
													HttpdHandler.removeHttpdSiteAuthenticatedLocation(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_SITE,
														pkey
													);
													HttpdHandler.removeHttpdSite(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_SITE_URLS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_SITE_URL,
														pkey
													);
													HttpdHandler.removeHttpdSiteURL(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_CONTEXTS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_TOMCAT_CONTEXT,
														pkey
													);
													HttpdHandler.removeHttpdTomcatContext(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_DATA_SOURCES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_TOMCAT_DATA_SOURCE,
														pkey
													);
													HttpdHandler.removeHttpdTomcatDataSource(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case HTTPD_TOMCAT_PARAMETERS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_HTTPD_TOMCAT_PARAMETER,
														pkey
													);
													HttpdHandler.removeHttpdTomcatParameter(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_ACC_ADDRESSES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_ACC_ADDRESS,
														pkey
													);
													EmailHandler.removeLinuxAccAddress(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_ACCOUNTS :
												{
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_ACCOUNT,
														username
													);
													LinuxAccountHandler.removeLinuxAccount(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_GROUP_ACCOUNTS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_GROUP_ACCOUNT,
														pkey
													);
													LinuxAccountHandler.removeLinuxGroupAccount(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_GROUPS :
												{
													GroupId name = GroupId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_GROUP,
														name
													);
													LinuxAccountHandler.removeLinuxGroup(
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
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_SERVER_ACCOUNT,
														pkey
													);
													LinuxAccountHandler.removeLinuxServerAccount(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case LINUX_SERVER_GROUPS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_LINUX_SERVER_GROUP,
														pkey
													);
													LinuxAccountHandler.removeLinuxServerGroup(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case MAJORDOMO_SERVERS :
												{
													int domain = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_MAJORDOMO_SERVER,
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
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_MYSQL_DATABASE,
														pkey
													);
													MySQLHandler.removeMySQLDatabase(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_DB_USERS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_MYSQL_DB_USER,
														pkey
													);
													MySQLHandler.removeMySQLDBUser(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_SERVER_USERS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_MYSQL_SERVER_USER,
														pkey
													);
													MySQLHandler.removeMySQLServerUser(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case MYSQL_USERS :
												{
													MySQLUserId username = MySQLUserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_MYSQL_USER,
														username
													);
													MySQLHandler.removeMySQLUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case NET_BINDS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_NET_BIND,
														pkey
													);
													NetBindHandler.removeNetBind(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case PACKAGE_DEFINITIONS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														"remove_package_definition",
														pkey
													);
													PackageHandler.removePackageDefinition(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_DATABASES :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_POSTGRES_DATABASE,
														pkey
													);
													PostgresHandler.removePostgresDatabase(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_SERVER_USERS :
												{
													int pkey = in.readCompressedInt();
													process.setCommand(
														AOSHCommand.REMOVE_POSTGRES_SERVER_USER,
														pkey
													);
													PostgresHandler.removePostgresServerUser(
														conn,
														source,
														invalidateList,
														pkey
													);
													resp = Response.DONE;
												}
												break;
											case POSTGRES_USERS :
												{
													PostgresUserId username = PostgresUserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_POSTGRES_USER,
														username
													);
													PostgresHandler.removePostgresUser(
														conn,
														source,
														invalidateList,
														username
													);
													resp = Response.DONE;
												}
												break;
											case USERNAMES :
												{
													UserId username = UserId.valueOf(in.readUTF());
													process.setCommand(
														AOSHCommand.REMOVE_USERNAME,
														username
													);
													UsernameHandler.removeUsername(
														conn,
														source,
														invalidateList,
														username
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
											int pkey = in.readCompressedInt();
											process.setCommand(
												"request_replication_daemon_access",
												pkey
											);
											AOServer.DaemonAccess daemonAccess = FailoverHandler.requestReplicationDaemonAccess(
												conn,
												source,
												pkey
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.RESTART_APACHE,
												aoServer
											);
											HttpdHandler.restartApache(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_CRON :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.RESTART_CRON,
												aoServer
											);
											AOServerHandler.restartCron(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_MYSQL :
										{
											int mysqlServer = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)<0) throw new IOException("addBackupServer call not supported for AOServ Client version < "+AOServProtocol.Version.VERSION_1_4+", please upgrade AOServ Client.");
											process.setCommand(
												AOSHCommand.RESTART_MYSQL,
												mysqlServer
											);
											MySQLHandler.restartMySQL(
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
												AOSHCommand.RESTART_POSTGRESQL,
												postgresServer
											);
											PostgresHandler.restartPostgreSQL(
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
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.RESTART_XFS,
												aoServer
											);
											AOServerHandler.restartXfs(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case RESTART_XVFB :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.RESTART_XVFB,
												aoServer
											);
											AOServerHandler.restartXvfb(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_AUTORESPONDER :
										{
											int pkey = in.readCompressedInt();
											int from = in.readCompressedInt();
											String subject = in.readNullUTF();
											String content = in.readNullUTF();
											boolean enabled = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_AUTORESPONDER,
												pkey,
												from==-1?null:from,
												subject,
												content,
												enabled
											);
											LinuxAccountHandler.setAutoresponder(
												conn,
												source,
												invalidateList,
												pkey,
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
											AccountingCode oldAccounting = AccountingCode.valueOf(in.readUTF());
											AccountingCode newAccounting = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_BUSINESS_ACCOUNTING,
												oldAccounting,
												newAccounting
											);
											BusinessHandler.setBusinessAccounting(
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
											UserId username = UserId.valueOf(in.readUTF());
											String password = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_BUSINESS_ADMINISTRATOR_PASSWORD,
												username,
												AOServProtocol.FILTERED
											);
											BusinessHandler.setBusinessAdministratorPassword(
												conn,
												source,
												invalidateList,
												username,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_BUSINESS_ADMINISTRATOR_PROFILE :
										{
											UserId username = UserId.valueOf(in.readUTF());
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
												AOSHCommand.SET_BUSINESS_ADMINISTRATOR_PROFILE,
												username,
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
											BusinessHandler.setBusinessAdministratorProfile(
												conn,
												source,
												invalidateList,
												username,
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
											int pkey = in.readCompressedInt();
											String crontab = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_CRON_TABLE,
												pkey,
												crontab
											);
											LinuxAccountHandler.setCronTable(
												conn,
												source,
												pkey,
												crontab
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_CVS_REPOSITORY_MODE :
										{
											int pkey = in.readCompressedInt();
											long mode = in.readLong();
											process.setCommand(
												AOSHCommand.SET_CVS_REPOSITORY_MODE,
												pkey,
												Long.toOctalString(mode)
											);
											CvsHandler.setMode(
												conn,
												source,
												invalidateList,
												pkey,
												mode
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_DEFAULT_BUSINESS_SERVER :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SET_DEFAULT_BUSINESS_SERVER,
												pkey
											);
											BusinessHandler.setDefaultBusinessServer(
												conn,
												source,
												invalidateList,
												pkey
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
												AOSHCommand.SET_DNS_ZONE_TTL,
												zone,
												ttl
											);
											DNSHandler.setDNSZoneTTL(
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
											int pkey = in.readCompressedInt();
											String list = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_EMAIL_LIST,
												pkey,
												list
											);
											EmailHandler.setEmailListAddressList(
												conn,
												source,
												pkey,
												list
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_FILE_BACKUP_SETTINGS :
										{
											int pkey = in.readCompressedInt();
											String path = in.readUTF();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
												in.readCompressedInt(); // package
											}
											boolean backupEnabled;
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
												short backupLevel=in.readShort();
												in.readShort(); // backup_retention
												in.readBoolean(); // recurse
												backupEnabled = backupLevel>0;
											} else {
												backupEnabled = in.readBoolean();
											}
											boolean required;
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_62)>=0) {
												required = in.readBoolean();
											} else {
												required = false;
											}
											process.setCommand(
												AOSHCommand.SET_FILE_BACKUP_SETTING,
												pkey,
												path,
												backupEnabled,
												required
											);
											BackupHandler.setFileBackupSettings(
												conn,
												source,
												invalidateList,
												pkey,
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
												if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_62)>=0) {
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
											FailoverHandler.setFileBackupSettingsAllAtOnce(
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
											int pkey = in.readCompressedInt();
											boolean is_manual = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_HTTPD_SHARED_TOMCAT_IS_MANUAL,
												pkey,
												is_manual
											);
											HttpdHandler.setHttpdSharedTomcatIsManual(
												conn,
												source,
												invalidateList,
												pkey,
												is_manual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_AUTHENTICATED_LOCATION_ATTRIBUTES :
										{
											int pkey = in.readCompressedInt();
											String path = in.readUTF().trim();
											boolean isRegularExpression = in.readBoolean();
											String authName = in.readUTF().trim();
											UnixPath authGroupFile;
											{
												String s = in.readUTF().trim();
												authGroupFile = s.isEmpty() ? null : UnixPath.valueOf(s);
											}
											UnixPath authUserFile;
											{
												String s = in.readUTF().trim();
												authUserFile = s.isEmpty() ? null : UnixPath.valueOf(s);
											}
											String require = in.readUTF().trim();
											process.setCommand(
												"set_httpd_site_authenticated_location_attributes",
												pkey,
												path,
												isRegularExpression,
												authName,
												authGroupFile,
												authUserFile,
												require
											);
											HttpdHandler.setHttpdSiteAuthenticatedLocationAttributes(
												conn,
												source,
												invalidateList,
												pkey,
												path,
												isRegularExpression,
												authName,
												authGroupFile,
												authUserFile,
												require
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_IS_MANUAL :
										{
											int pkey = in.readCompressedInt();
											boolean is_manual = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_HTTPD_SITE_BIND_IS_MANUAL,
												pkey,
												is_manual
											);
											HttpdHandler.setHttpdSiteBindIsManual(
												conn,
												source,
												invalidateList,
												pkey,
												is_manual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME :
										{
											int pkey = in.readCompressedInt();
											boolean redirect_to_primary_hostname = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME,
												pkey,
												redirect_to_primary_hostname
											);
											HttpdHandler.setHttpdSiteBindRedirectToPrimaryHostname(
												conn,
												source,
												invalidateList,
												pkey,
												redirect_to_primary_hostname
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_IS_MANUAL :
										{
											int pkey = in.readCompressedInt();
											boolean is_manual = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_HTTPD_SITE_IS_MANUAL,
												pkey,
												is_manual
											);
											HttpdHandler.setHttpdSiteIsManual(
												conn,
												source,
												invalidateList,
												pkey,
												is_manual
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_SERVER_ADMIN :
										{
											int pkey = in.readCompressedInt();
											Email emailAddress = Email.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_HTTPD_SITE_SERVER_ADMIN,
												pkey,
												emailAddress
											);
											HttpdHandler.setHttpdSiteServerAdmin(
												conn,
												source,
												invalidateList,
												pkey,
												emailAddress
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_SITE_BIND_PREDISABLE_CONFIG :
										{
											int pkey = in.readCompressedInt();
											String config = in.readNullUTF();
											process.setCommand(
												"set_httpd_site_bind_predisable_config",
												pkey,
												AOServProtocol.FILTERED
											);
											HttpdHandler.setHttpdSiteBindPredisableConfig(
												conn,
												source,
												invalidateList,
												pkey,
												config
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES :
										{
											int pkey = in.readCompressedInt();
											String className = in.readNullUTF();
											boolean cookies = in.readBoolean();
											boolean crossContext = in.readBoolean();
											UnixPath docBase = UnixPath.valueOf(in.readUTF());
											boolean override = in.readBoolean();
											String path = in.readUTF().trim();
											boolean privileged = in.readBoolean();
											boolean reloadable = in.readBoolean();
											boolean useNaming = in.readBoolean();
											String wrapperClass = in.readNullUTF();
											int debug = in.readCompressedInt();
											UnixPath workDir = UnixPath.valueOf(in.readNullUTF());
											process.setCommand(
												AOSHCommand.SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES,
												pkey,
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
												workDir
											);
											HttpdHandler.setHttpdTomcatContextAttributes(
												conn,
												source,
												invalidateList,
												pkey,
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
												workDir
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_IMAP_FOLDER_SUBSCRIBED :
										{
											int pkey = in.readCompressedInt();
											String folderName = in.readUTF();
											boolean subscribed = in.readBoolean();
											process.setCommand(
												"set_imap_folder_subscribed",
												pkey,
												folderName,
												subscribed
											);
											EmailHandler.setImapFolderSubscribed(
												conn,
												source,
												pkey,
												folderName,
												subscribed
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_IP_ADDRESS_DHCP_ADDRESS :
										{
											int ipAddress = in.readCompressedInt();
											InetAddress dhcpAddress = InetAddress.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_IP_ADDRESS_DHCP_ADDRESS,
												ipAddress,
												dhcpAddress
											);
											IPAddressHandler.setIPAddressDHCPAddress(
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
												AOSHCommand.SET_IP_ADDRESS_HOSTNAME,
												ipAddress,
												hostname
											);
											IPAddressHandler.setIPAddressHostname(
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
									case SET_IP_ADDRESS_PACKAGE :
										{
											int ipAddress = in.readCompressedInt();
											AccountingCode packageName = AccountingCode.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_IP_ADDRESS_PACKAGE,
												ipAddress,
												packageName
											);
											IPAddressHandler.setIPAddressPackage(
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
												AOSHCommand.ADD_IP_REPUTATION,
												ipReputationSet,
												size
											);
											IpReputationSetHandler.addIpReputation(
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

											int ao_server = in.readCompressedInt();
											long time = in.readLong();
											process.setCommand(
												"set_last_distro_time",
												ao_server,
												new java.util.Date(time)
											);
											AOServerHandler.setLastDistroTime(
												conn,
												source,
												invalidateList,
												ao_server,
												time
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_HOME_PHONE :
										{
											UserId username = UserId.valueOf(in.readUTF());
											Gecos phone;
											{
												String s = in.readUTF();
												phone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_HOME_PHONE,
												username,
												phone
											);
											LinuxAccountHandler.setLinuxAccountHomePhone(
												conn,
												source,
												invalidateList,
												username,
												phone
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_NAME :
										{
											UserId username = UserId.valueOf(in.readUTF());
											Gecos fullName = Gecos.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_NAME,
												username,
												fullName
											);
											LinuxAccountHandler.setLinuxAccountName(
												conn,
												source,
												invalidateList,
												username,
												fullName
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_OFFICE_LOCATION :
										{
											UserId username = UserId.valueOf(in.readUTF());
											Gecos location;
											{
												String s = in.readUTF();
												location = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_OFFICE_LOCATION,
												username,
												location
											);
											LinuxAccountHandler.setLinuxAccountOfficeLocation(
												conn,
												source,
												invalidateList,
												username,
												location
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_OFFICE_PHONE :
										{
											UserId username = UserId.valueOf(in.readUTF());
											Gecos phone;
											{
												String s = in.readUTF();
												phone = s.isEmpty() ? null : Gecos.valueOf(s);
											}
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_OFFICE_PHONE,
												username,
												phone
											);
											LinuxAccountHandler.setLinuxAccountOfficePhone(
												conn,
												source,
												invalidateList,
												username,
												phone
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_ACCOUNT_SHELL :
										{
											UserId username = UserId.valueOf(in.readUTF());
											UnixPath shell = UnixPath.valueOf(in.readUTF());
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_SHELL,
												username,
												shell
											);
											LinuxAccountHandler.setLinuxAccountShell(
												conn,
												source,
												invalidateList,
												username,
												shell
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION :
										{
											int pkey = in.readCompressedInt();
											int days = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION,
												pkey,
												days
											);
											LinuxAccountHandler.setLinuxServerAccountJunkEmailRetention(
												conn,
												source,
												invalidateList,
												pkey,
												days
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_LINUX_ACCOUNT_PASSWORD,
												pkey,
												AOServProtocol.FILTERED
											);
											LinuxAccountHandler.setLinuxServerAccountPassword(
												conn,
												source,
												invalidateList,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_PREDISABLE_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_linux_server_account_predisable_password",
												pkey,
												AOServProtocol.FILTERED
											);
											LinuxAccountHandler.setLinuxServerAccountPredisablePassword(
												conn,
												source,
												invalidateList,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_EMAIL_SPAMASSASSIN_INTEGRATION_MODE:
										{
											int pkey = in.readCompressedInt();
											String mode = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_INTEGRATION_MODE,
												pkey,
												mode
											);
											LinuxAccountHandler.setLinuxServerAccountSpamAssassinIntegrationMode(
												conn,
												source,
												invalidateList,
												pkey,
												mode
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE:
										{
											int pkey = in.readCompressedInt();
											float required_score = in.readFloat();
											process.setCommand(
												AOSHCommand.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE,
												pkey,
												required_score
											);
											LinuxAccountHandler.setLinuxServerAccountSpamAssassinRequiredScore(
												conn,
												source,
												invalidateList,
												pkey,
												required_score
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_DISCARD_SCORE:
										{
											int pkey = in.readCompressedInt();
											int discard_score = in.readCompressedInt();
											process.setCommand(
												"set_linux_server_account_spamassassin_discard_score",
												pkey,
												discard_score==-1 ? "\"\"" : Integer.toString(discard_score)
											);
											LinuxAccountHandler.setLinuxServerAccountSpamAssassinDiscardScore(
												conn,
												source,
												invalidateList,
												pkey,
												discard_score
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION :
										{
											int pkey = in.readCompressedInt();
											int days = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION,
												pkey,
												days
											);
											LinuxAccountHandler.setLinuxServerAccountTrashEmailRetention(
												conn,
												source,
												invalidateList,
												pkey,
												days
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_LINUX_SERVER_ACCOUNT_USE_INBOX :
										{
											int pkey = in.readCompressedInt();
											boolean useInbox = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_LINUX_SERVER_ACCOUNT_USE_INBOX,
												pkey,
												useInbox
											);
											LinuxAccountHandler.setLinuxServerAccountUseInbox(
												conn,
												source,
												invalidateList,
												pkey,
												useInbox
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_MAJORDOMO_INFO_FILE :
										{
											int pkey = in.readCompressedInt();
											String file = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_MAJORDOMO_INFO_FILE,
												pkey,
												file
											);
											EmailHandler.setMajordomoInfoFile(
												conn,
												source,
												pkey,
												file
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MAJORDOMO_INTRO_FILE :
										{
											int pkey = in.readCompressedInt();
											String file = in.readUTF();
											process.setCommand(
												AOSHCommand.SET_MAJORDOMO_INTRO_FILE,
												pkey,
												file
											);
											EmailHandler.setMajordomoIntroFile(
												conn,
												source,
												pkey,
												file
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MYSQL_SERVER_USER_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												AOSHCommand.SET_MYSQL_SERVER_USER_PASSWORD,
												pkey,
												AOServProtocol.FILTERED
											);
											MySQLHandler.setMySQLServerUserPassword(
												conn,
												source,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_MYSQL_SERVER_USER_PREDISABLE_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_mysql_server_user_predisable_password",
												pkey,
												AOServProtocol.FILTERED
											);
											MySQLHandler.setMySQLServerUserPredisablePassword(
												conn,
												source,
												invalidateList,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_NET_BIND_MONITORING :
										{
											int pkey = in.readCompressedInt();
											boolean enabled = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_NET_BIND_MONITORING_ENABLED,
												pkey,
												enabled
											);
											NetBindHandler.setNetBindMonitoringEnabled(
												conn,
												source,
												invalidateList,
												pkey,
												enabled
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_NET_BIND_OPEN_FIREWALL :
										{
											int pkey = in.readCompressedInt();
											boolean open_firewall = in.readBoolean();
											process.setCommand(
												AOSHCommand.SET_NET_BIND_OPEN_FIREWALL,
												pkey,
												open_firewall
											);
											NetBindHandler.setNetBindOpenFirewall(
												conn,
												source,
												invalidateList,
												pkey,
												open_firewall
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PACKAGE_DEFINITION_ACTIVE :
										{
											int pkey = in.readCompressedInt();
											boolean is_active = in.readBoolean();
											process.setCommand(
												"set_package_definition_active",
												pkey,
												is_active
											);
											PackageHandler.setPackageDefinitionActive(
												conn,
												source,
												invalidateList,
												pkey,
												is_active
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PACKAGE_DEFINITION_LIMITS :
										{
											int pkey = in.readCompressedInt();
											int count = in.readCompressedInt();
											String[] resources = new String[count];
											int[] soft_limits = new int[count];
											int[] hard_limits = new int[count];
											int[] additional_rates = new int[count];
											String[] additional_transaction_types = new String[count];
											for(int c=0;c<count;c++) {
												resources[c] = in.readUTF().trim();
												soft_limits[c] = in.readCompressedInt();
												hard_limits[c] = in.readCompressedInt();
												additional_rates[c] = in.readCompressedInt();
												additional_transaction_types[c] = in.readNullUTF();
											}
											process.setCommand(
												"set_package_definition_limits",
												pkey,
												count,
												resources,
												soft_limits,
												hard_limits,
												additional_rates,
												additional_transaction_types
											);
											PackageHandler.setPackageDefinitionLimits(
												conn,
												source,
												invalidateList,
												pkey,
												resources,
												soft_limits,
												hard_limits,
												additional_rates,
												additional_transaction_types
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_POSTGRES_SERVER_USER_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												AOSHCommand.SET_POSTGRES_SERVER_USER_PASSWORD,
												pkey,
												password
											);
											PostgresHandler.setPostgresServerUserPassword(
												conn,
												source,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case SET_POSTGRES_SERVER_USER_PREDISABLE_PASSWORD :
										{
											int pkey = in.readCompressedInt();
											String password = in.readNullUTF();
											process.setCommand(
												"set_postgres_server_user_predisable_password",
												pkey,
												AOServProtocol.FILTERED
											);
											PostgresHandler.setPostgresServerUserPredisablePassword(
												conn,
												source,
												invalidateList,
												pkey,
												password
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PRIMARY_HTTPD_SITE_URL :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SET_PRIMARY_HTTPD_SITE_URL,
												pkey
											);
											HttpdHandler.setPrimaryHttpdSiteURL(
												conn,
												source,
												invalidateList,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_PRIMARY_LINUX_GROUP_ACCOUNT :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SET_PRIMARY_LINUX_GROUP_ACCOUNT,
												pkey
											);
											LinuxAccountHandler.setPrimaryLinuxGroupAccount(
												conn,
												source,
												invalidateList,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									/*case SET_TICKET_ASSIGNED_TO :
										{
											int ticketID=in.readCompressedInt(); 
											String assignedTo=in.readUTF().trim();
											if(assignedTo.length()==0) assignedTo=null;
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case SET_TICKET_CONTACT_EMAILS :
										{
											int ticketID = in.readCompressedInt();
											String contactEmails = in.readUTF();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
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
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
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
											AccountingCode oldAccounting;
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) {
												// Added old accounting to behave like atomic variable
												String oldAccountingS = in.readUTF();
												oldAccounting = oldAccountingS.length()==0 ? null : AccountingCode.valueOf(oldAccountingS);
											} else {
												oldAccounting = null;
											}
											String newAccountingS = in.readUTF();
											AccountingCode newAccounting = newAccountingS.length()==0 ? null : AccountingCode.valueOf(newAccountingS);
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
												String username = in.readUTF();
												String comments = in.readUTF();
											}
											process.setCommand(
												"set_ticket_business",
												ticketID,
												oldAccounting,
												newAccounting
											);
											boolean updated = TicketHandler.setTicketBusiness(
												conn,
												source,
												invalidateList,
												ticketID,
												oldAccounting,
												newAccounting
											);
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)<0) {
												resp = Response.DONE;
											} else {
												// Added boolean updated response
												resp = Response.valueOf(
													AOServProtocol.DONE,
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
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
											resp = Response.valueOf(
												AOServProtocol.DONE,
												updated
											);
											sendInvalidateList = true;
										}
										break;
									case START_APACHE :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_APACHE,
												aoServer
											);
											HttpdHandler.startApache(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_CRON :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_CRON,
												aoServer
											);
											AOServerHandler.startCron(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_DISTRO :
										{
											process.setPriority(Thread.MIN_PRIORITY+1);
											currentThread.setPriority(Thread.MIN_PRIORITY+1);

											int ao_server = in.readCompressedInt();
											boolean includeUser = in.readBoolean();
											process.setCommand(
												AOSHCommand.START_DISTRO,
												ao_server,
												includeUser
											);
											AOServerHandler.startDistro(
												conn,
												source,
												ao_server,
												includeUser
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_JVM :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_JVM,
												pkey
											);
											String message = HttpdHandler.startJVM(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullString(
												AOServProtocol.DONE,
												message
											);
											sendInvalidateList = false;
										}
										break;
									case START_MYSQL :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_MYSQL,
												aoServer
											);
											MySQLHandler.startMySQL(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_POSTGRESQL :
										{
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_POSTGRESQL,
												postgresServer
											);
											PostgresHandler.startPostgreSQL(
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
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_XFS,
												aoServer
											);
											AOServerHandler.startXfs(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case START_XVFB :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.START_XVFB,
												aoServer
											);
											AOServerHandler.startXvfb(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_APACHE :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_APACHE,
												aoServer
											);
											HttpdHandler.stopApache(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_CRON :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_CRON,
												aoServer
											);
											AOServerHandler.stopCron(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_JVM :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_JVM,
												pkey
											);
											String message = HttpdHandler.stopJVM(
												conn,
												source,
												pkey
											);
											resp = Response.valueOfNullString(
												AOServProtocol.DONE,
												message
											);
											sendInvalidateList = false;
										}
										break;
									case STOP_MYSQL :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_MYSQL,
												aoServer
											);
											MySQLHandler.stopMySQL(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_POSTGRESQL :
										{
											int postgresServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_POSTGRESQL,
												postgresServer
											);
											PostgresHandler.stopPostgreSQL(
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
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_XFS,
												aoServer
											);
											AOServerHandler.stopXfs(
												conn,
												source,
												aoServer
											);
											resp = Response.DONE;
											sendInvalidateList = false;
										}
										break;
									case STOP_XVFB :
										{
											int aoServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.STOP_XVFB,
												aoServer
											);
											AOServerHandler.stopXvfb(
												conn,
												source,
												aoServer
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
												AOSHCommand.ADD_TICKET_WORK,
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
											resp1=AOServProtocol.DONE;
											sendInvalidateList=true;
										}
										break;*/
									case TRANSACTION_APPROVED :
										{
											int transid = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_28)<=0) {
												String paymentType=in.readUTF();
												String paymentInfo=in.readNullUTF();
												String merchant=in.readNullUTF();
												String apr_num;
												if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_128)<0) apr_num=Integer.toString(in.readCompressedInt());
												else apr_num=in.readUTF();
												throw new SQLException("approve_transaction for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
											}
											int creditCardTransaction = in.readCompressedInt();
											process.setCommand(
												"approve_transaction",
												transid,
												creditCardTransaction
											);
											TransactionHandler.transactionApproved(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case TRANSACTION_DECLINED :
										{
											int transid = in.readCompressedInt();
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_28)<=0) {
												String paymentType=in.readUTF().trim();
												String paymentInfo=in.readNullUTF();
												String merchant=in.readNullUTF();
												throw new SQLException("decline_transaction for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
											}
											int creditCardTransaction = in.readCompressedInt();
											process.setCommand(
												"decline_transaction",
												transid,
												creditCardTransaction
											);
											TransactionHandler.transactionDeclined(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case TRANSACTION_HELD :
										{
											int transid = in.readCompressedInt();
											int creditCardTransaction = in.readCompressedInt();
											process.setCommand(
												"hold_transaction",
												transid,
												creditCardTransaction
											);
											TransactionHandler.transactionHeld(
												conn,
												source,
												invalidateList,
												transid,
												creditCardTransaction
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case REACTIVATE_CREDIT_CARD :
										{
											int pkey = in.readCompressedInt();
											process.setCommand(
												"reactivate_credit_card",
												pkey
											);
											CreditCardHandler.reactivateCreditCard(
												conn,
												source,
												invalidateList,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_CREDIT_CARD_USE_MONTHLY :
										{
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											int pkey = in.readCompressedInt();
											process.setCommand(
												"set_credit_card_use_monthly",
												accounting,
												pkey
											);
											CreditCardHandler.setCreditCardUseMonthly(
												conn,
												source,
												invalidateList,
												accounting,
												pkey
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case SET_FAILOVER_FILE_REPLICATION_BIT_RATE :
										{
											int pkey = in.readCompressedInt();
											final Long bitRate;
											if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_61)<=0) {
												int bitRateInt = in.readCompressedInt();
												bitRate = bitRateInt==-1 ? null : (long)bitRateInt;
											} else {
												long bitRateLong = in.readLong();
												bitRate = bitRateLong==-1 ? null : bitRateLong;
											}
											process.setCommand(
												"set_failover_file_replication_bit_rate",
												pkey,
												bitRate==null ? "unlimited" : bitRate.toString()
											);
											FailoverHandler.setFailoverFileReplicationBitRate(
												conn,
												source,
												invalidateList,
												pkey,
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
											FailoverHandler.setFailoverFileSchedules(
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
											int pkey = in.readCompressedInt();
											String firstName=in.readUTF().trim();
											String lastName=in.readUTF().trim();
											String companyName=in.readUTF().trim();
											if(companyName.length()==0) companyName=null;
											String email=in.readUTF().trim();
											if(email.length()==0) email=null;
											String phone=in.readUTF().trim();
											if(phone.length()==0) phone=null;
											String fax=in.readUTF().trim();
											if(fax.length()==0) fax=null;
											String customerTaxId=in.readUTF().trim();
											if(customerTaxId.length()==0) customerTaxId=null;
											String streetAddress1=in.readUTF().trim();
											String streetAddress2=in.readUTF().trim();
											if(streetAddress2.length()==0) streetAddress2=null;
											String city=in.readUTF().trim();
											String state=in.readUTF().trim();
											if(state.length()==0) state=null;
											String postalCode=in.readUTF().trim();
											if(postalCode.length()==0) postalCode=null;
											String countryCode=in.readUTF().trim();
											String description=in.readUTF().trim();
											if(description.length()==0) description=null;
											process.setCommand(
												"update_credit_card",
												pkey,
												firstName,
												lastName,
												companyName,
												email,
												phone,
												fax,
												customerTaxId,
												streetAddress1,
												streetAddress2,
												city,
												state,
												postalCode,
												countryCode,
												description
											);
											CreditCardHandler.updateCreditCard(
												conn,
												source,
												invalidateList,
												pkey,
												firstName,
												lastName,
												companyName,
												email,
												phone,
												fax,
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
											int pkey = in.readCompressedInt();
											String maskedCardNumber = in.readUTF();
											String encryptedCardNumber = in.readNullUTF();
											String encryptedExpiration = in.readNullUTF();
											int encryptionFrom = in.readCompressedInt();
											int encryptionRecipient = in.readCompressedInt();
											process.setCommand(
												"update_credit_card_number_and_expiration",
												pkey,
												maskedCardNumber,
												encryptedCardNumber==null ? null : AOServProtocol.FILTERED,
												encryptedExpiration==null ? null : AOServProtocol.FILTERED,
												encryptionFrom==-1 ? null : encryptionFrom,
												encryptionRecipient==-1 ? null : encryptionRecipient
											);
											CreditCardHandler.updateCreditCardNumberAndExpiration(
												conn,
												source,
												invalidateList,
												pkey,
												maskedCardNumber,
												encryptedCardNumber,
												encryptedExpiration,
												encryptionFrom,
												encryptionRecipient
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_CREDIT_CARD_EXPIRATION :
										{
											int pkey = in.readCompressedInt();
											String encryptedExpiration = in.readUTF();
											int encryptionFrom = in.readCompressedInt();
											int encryptionRecipient = in.readCompressedInt();
											process.setCommand(
												"update_credit_card_expiration",
												pkey,
												AOServProtocol.FILTERED,
												encryptionFrom,
												encryptionRecipient
											);
											CreditCardHandler.updateCreditCardExpiration(
												conn,
												source,
												invalidateList,
												pkey,
												encryptedExpiration,
												encryptionFrom,
												encryptionRecipient
											);
											resp = Response.DONE;
											sendInvalidateList = true;
										}
										break;
									case UPDATE_HTTPD_TOMCAT_DATA_SOURCE:
										{
											int pkey = in.readCompressedInt();
											String name=in.readUTF();
											String driverClassName=in.readUTF();
											String url=in.readUTF();
											String username=in.readUTF();
											String password=in.readUTF();
											int maxActive=in.readCompressedInt();
											int maxIdle=in.readCompressedInt();
											int maxWait=in.readCompressedInt();
											String validationQuery=in.readUTF();
											if(validationQuery.length()==0) validationQuery=null;
											process.setCommand(
												AOSHCommand.UPDATE_HTTPD_TOMCAT_DATA_SOURCE,
												pkey,
												name,
												driverClassName,
												url,
												username,
												AOServProtocol.FILTERED,
												maxActive,
												maxIdle,
												maxWait,
												validationQuery
											);
											HttpdHandler.updateHttpdTomcatDataSource(
												conn,
												source,
												invalidateList,
												pkey,
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
											int pkey = in.readCompressedInt();
											String name=in.readUTF();
											String value=in.readUTF();
											boolean override=in.readBoolean();
											String description=in.readUTF();
											if(description.length()==0) description=null;
											process.setCommand(
												AOSHCommand.UPDATE_HTTPD_TOMCAT_PARAMETER,
												pkey,
												name,
												value,
												override,
												description
											);
											HttpdHandler.updateHttpdTomcatParameter(
												conn,
												source,
												invalidateList,
												pkey,
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
											int pkey = in.readCompressedInt();
											AccountingCode accounting = AccountingCode.valueOf(in.readUTF());
											String category=in.readUTF();
											String name=in.readUTF().trim();
											String version=in.readUTF().trim();
											String display=in.readUTF().trim();
											String description=in.readUTF().trim();
											int setupFee=in.readCompressedInt();
											String setupFeeTransactionType=in.readNullUTF();
											int monthlyRate=in.readCompressedInt();
											String monthlyRateTransactionType=in.readUTF();
											process.setCommand(
												"update_package_definition",
												pkey,
												accounting,
												category,
												name,
												version,
												display,
												description,
												SQLUtility.getDecimal(setupFee),
												setupFeeTransactionType,
												SQLUtility.getDecimal(monthlyRate),
												monthlyRateTransactionType
											);
											PackageHandler.updatePackageDefinition(
												conn,
												source,
												invalidateList,
												pkey,
												accounting,
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
											SchemaTable.TableID tableID = TableHandler.convertFromClientTableID(conn, source, clientTableID);
											if(tableID == null) throw new IOException("Client table not supported: #" + clientTableID);
											int aoServer = in.readCompressedInt();
											switch(tableID) {
												case HTTPD_SITES :
													process.setCommand(
														AOSHCommand.WAIT_FOR_HTTPD_SITE_REBUILD,
														aoServer
													);
													HttpdHandler.waitForHttpdSiteRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case LINUX_ACCOUNTS :
													process.setCommand(
														AOSHCommand.WAIT_FOR_LINUX_ACCOUNT_REBUILD,
														aoServer
													);
													LinuxAccountHandler.waitForLinuxAccountRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case MYSQL_DATABASES :
													process.setCommand(
														AOSHCommand.WAIT_FOR_MYSQL_DATABASE_REBUILD,
														aoServer
													);
													MySQLHandler.waitForMySQLDatabaseRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case MYSQL_DB_USERS :
													process.setCommand(
														AOSHCommand.WAIT_FOR_MYSQL_DB_USER_REBUILD,
														aoServer
													);
													MySQLHandler.waitForMySQLDBUserRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case MYSQL_USERS :
													process.setCommand(
														AOSHCommand.WAIT_FOR_MYSQL_USER_REBUILD,
														aoServer
													);
													MySQLHandler.waitForMySQLUserRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case POSTGRES_DATABASES :
													process.setCommand(
														AOSHCommand.WAIT_FOR_POSTGRES_DATABASE_REBUILD,
														aoServer
													);
													PostgresHandler.waitForPostgresDatabaseRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case POSTGRES_SERVERS :
													process.setCommand(
														AOSHCommand.WAIT_FOR_POSTGRES_SERVER_REBUILD,
														aoServer
													);
													PostgresHandler.waitForPostgresServerRebuild(
														conn,
														source,
														aoServer
													);
													break;
												case POSTGRES_USERS :
													process.setCommand(
														AOSHCommand.WAIT_FOR_POSTGRES_USER_REBUILD,
														aoServer
													);
													PostgresHandler.waitForPostgresUserRebuild(
														conn,
														source,
														aoServer
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
											AOServer.DaemonAccess daemonAccess = VirtualServerHandler.requestVncConsoleDaemonAccess(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
												AOSHCommand.VERIFY_VIRTUAL_DISK,
												virtualDisk
											);
											long lastVerified = VirtualServerHandler.verifyVirtualDisk(
												conn,
												source,
												virtualDisk
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												lastVerified
											);
											sendInvalidateList = false;
										}
										break;
									case CREATE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.CREATE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.createVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case REBOOT_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.REBOOT_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.rebootVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case SHUTDOWN_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.SHUTDOWN_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.shutdownVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case DESTROY_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.DESTROY_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.destroyVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case PAUSE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.PAUSE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.pauseVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case UNPAUSE_VIRTUAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.UNPAUSE_VIRTUAL_SERVER,
												virtualServer
											);
											String output = VirtualServerHandler.unpauseVirtualServer(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												output
											);
											sendInvalidateList = false;
										}
										break;
									case GET_VIRTUAL_SERVER_STATUS :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_VIRTUAL_SERVER_STATUS,
												virtualServer
											);
											int status = VirtualServerHandler.getVirtualServerStatus(
												conn,
												source,
												virtualServer
											);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												status
											);
											sendInvalidateList = false;
										}
										break;
									case GET_PRIMARY_PHYSICAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_PRIMARY_PHYSICAL_SERVER,
												virtualServer
											);
											int physicalServer = ClusterHandler.getPrimaryPhysicalServer(conn, source, virtualServer);
											resp = Response.valueOf(
												AOServProtocol.DONE,
												physicalServer
											);
											sendInvalidateList = false;
										}
										break;
									case GET_SECONDARY_PHYSICAL_SERVER :
										{
											int virtualServer = in.readCompressedInt();
											process.setCommand(
												AOSHCommand.GET_SECONDARY_PHYSICAL_SERVER,
												virtualServer
											);
											int physicalServer = ClusterHandler.getSecondaryPhysicalServer(conn, source, virtualServer);
											resp = Response.valueOf(
												AOServProtocol.DONE,
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
									for(SchemaTable.TableID tableID : tableIDs) {
										if(invalidateList.isInvalid(tableID)) clientInvalidateList.add(TableHandler.convertToClientTableID(conn, source, tableID));
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
						logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AOServProtocol.SQL_EXCEPTION);
						out.writeUTF(message==null?"":message);
					} catch(ValidationException err) {
						logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AOServProtocol.IO_EXCEPTION);
						out.writeUTF(message==null?"":message);
						keepOpen = false; // Close on ValidationException
					} catch(IOException err) {
						if(logIOException) logger.log(Level.SEVERE, null, err);
						String message=err.getMessage();
						out.writeByte(AOServProtocol.IO_EXCEPTION);
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
	 */
	public static void invalidateTables(
		InvalidateList invalidateList,
		RequestSource invalidateSource
	) throws IOException, SQLException {
		// Invalidate the internally cached data first
		invalidateList.invalidateMasterCaches();

		// Values used inside the loops
		long invalidateSourceConnectorID=invalidateSource==null?-1:invalidateSource.getConnectorID();

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
				if(invalidateSourceConnectorID!=source.getConnectorID()) {
					tableList.clear();
					// Build the list with a connection, but don't send until the connection is released
					try {
						try {
							for(SchemaTable.TableID tableID : tableIDs) {
								int clientTableID=TableHandler.convertToClientTableID(conn, source, tableID);
								if(clientTableID!=-1) {
									List<AccountingCode> affectedBusinesses = invalidateList.getAffectedBusinesses(tableID);
									List<Integer> affectedServers = invalidateList.getAffectedServers(tableID);
									if(
										affectedBusinesses!=null
										&& affectedServers!=null
									) {
										boolean businessMatches;
										int size=affectedBusinesses.size();
										if(size==0) businessMatches=true;
										else {
											businessMatches=false;
											for(int c=0;c<size;c++) {
												if(BusinessHandler.canAccessBusiness(conn, source, affectedBusinesses.get(c))) {
													businessMatches=true;
													break;
												}
											}
										}

										// Filter by server
										boolean serverMatches;
										size=affectedServers.size();
										if(size==0) serverMatches=true;
										else {
											serverMatches=false;
											for(int c=0;c<size;c++) {
												int server=affectedServers.get(c);
												if(ServerHandler.canAccessServer(conn, source, server)) {
													serverMatches=true;
													break;
												}
												if(
													tableID==SchemaTable.TableID.AO_SERVERS
													|| tableID==SchemaTable.TableID.IP_ADDRESSES
													|| tableID==SchemaTable.TableID.LINUX_ACCOUNTS
													|| tableID==SchemaTable.TableID.LINUX_SERVER_ACCOUNTS
													|| tableID==SchemaTable.TableID.NET_DEVICES
													|| tableID==SchemaTable.TableID.SERVERS
													|| tableID==SchemaTable.TableID.USERNAMES
												) {
													// These tables invalidations are also sent to the servers failover parent
													int failoverServer=ServerHandler.getFailoverServer(conn, server);
													if(failoverServer!=-1 && ServerHandler.canAccessServer(conn, source, failoverServer)) {
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

	/**
	 * Runs all of the configured protocols of <code>MasterServer</code>
	 * processes as configured in <code>com/aoindustries/aoserv/master/aoserv-master.properties</code>.
	 */
	public static void main(String[] args) {
		// Not profiled because the profiler is enabled here
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

			AccountCleaner.start();
			ClusterHandler.start();
			CreditCardHandler.start();
			DNSHandler.start();
			FailoverHandler.start();
			SignupHandler.start();
			TicketHandler.start();
		} catch (IOException | IllegalArgumentException err) {
			logger.log(Level.SEVERE, null, err);
		}
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
	 */
	public static <T extends AOServObject<?,?>> void writeObjects(
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		T obj,
		ResultSet results
	) throws IOException, SQLException {
		AOServProtocol.Version version=source.getProtocolVersion();

		// Make one pass counting the rows if providing progress information
		if(provideProgress) {
			int rowCount = 0;
			while (results.next()) rowCount++;
			results.beforeFirst();
			out.writeByte(AOServProtocol.NEXT);
			out.writeCompressedInt(rowCount);
		}
		int writeCount = 0;
		while(results.next()) {
			obj.init(results);
			out.writeByte(AOServProtocol.NEXT);
			obj.write(out, version);
			writeCount++;
		}
		if(writeCount > TableHandler.RESULT_SET_BATCH_SIZE) {
			logger.log(Level.WARNING, null, new SQLWarning("Warning: provideProgress==true caused non-cursor select with more than "+TableHandler.RESULT_SET_BATCH_SIZE+" rows: "+writeCount));
		}
	}

	/**
	 * Writes all rows of a results set.
	 */
	public static void writeObjects(RequestSource source, CompressedDataOutputStream out, boolean provideProgress, List<? extends AOServObject<?,?>> objs) throws IOException {
		AOServProtocol.Version version=source.getProtocolVersion();

		int size=objs.size();
		if (provideProgress) {
			out.writeByte(AOServProtocol.NEXT);
			out.writeCompressedInt(size);
		}
		for(int c=0;c<size;c++) {
			out.writeByte(AOServProtocol.NEXT);
			objs.get(c).write(out, version);
		}
	}

	/**
	 * Writes all rows of a results set.
	 */
	public static void writeObjectsSynced(
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		List<? extends AOServObject<?,?>> objs
	) throws IOException {
		AOServProtocol.Version version=source.getProtocolVersion();

		int size=objs.size();
		if (provideProgress) {
			out.writeByte(AOServProtocol.NEXT);
			out.writeCompressedInt(size);
		}
		for(int c=0;c<size;c++) {
			out.writeByte(AOServProtocol.NEXT);
			AOServObject<?,?> obj=objs.get(c);
			synchronized(obj) {
				obj.write(out, version);
			}
		}
	}

	public static String authenticate(
		DatabaseConnection conn,
		String remoteHost, 
		UserId connectAs, 
		UserId authenticateAs, 
		String password
	) throws IOException, SQLException {
		if(connectAs == null) return "Connection attempted with empty connect username";
		if(authenticateAs == null) return "Connection attempted with empty authentication username";

		if(!BusinessHandler.isBusinessAdministrator(conn, authenticateAs)) return "Unable to find BusinessAdministrator: "+authenticateAs;

		if(BusinessHandler.isBusinessAdministratorDisabled(conn, authenticateAs)) return "BusinessAdministrator disabled: "+authenticateAs;

		if (!isHostAllowed(conn, authenticateAs, remoteHost)) return "Connection from "+remoteHost+" as "+authenticateAs+" not allowed.";

		// Authenticate the client first
		if(password.length()==0) return "Connection attempted with empty password";

		HashedPassword correctCrypted=BusinessHandler.getBusinessAdministrator(conn, authenticateAs).getPassword();
		if(
			correctCrypted==null
			|| !correctCrypted.passwordMatches(password)
		) return "Connection attempted with invalid password";

		// If connectAs is not authenticateAs, must be authenticated with switch user permissions
		if(!connectAs.equals(authenticateAs)) {
			// Must have can_switch_users permissions and must be switching to a subaccount user
			if(!BusinessHandler.canSwitchUser(conn, authenticateAs, connectAs)) return "Not allowed to switch users from "+authenticateAs+" to "+connectAs;
		}

		// Let them in
		return null;
	}

	public static String trim(String inStr) {
		return (inStr==null?null:inStr.trim());
	}

	private static void addStat(
		List<MasterServerStat> objs, 
		String name, 
		String value, 
		String description
	) {
		name=trim(name);
		value=trim(value);
		description=trim(description);
		objs.add(new MasterServerStat(name, value, description));
	}

	public static void writeStats(RequestSource source, CompressedDataOutputStream out, boolean provideProgress) throws IOException {
		try {
			// Create the list of objects first
			List<MasterServerStat> objs=new ArrayList<>();
			addStat(objs, MasterServerStat.BYTE_ARRAY_CACHE_CREATES, Long.toString(BufferManager.getByteBufferCreates()), "Number of byte[] buffers created");
			addStat(objs, MasterServerStat.BYTE_ARRAY_CACHE_USES, Long.toString(BufferManager.getByteBufferUses()), "Total number of byte[] buffers allocated");

			addStat(objs, MasterServerStat.CHAR_ARRAY_CACHE_CREATES, Long.toString(BufferManager.getCharBufferCreates()), "Number of char[] buffers created");
			addStat(objs, MasterServerStat.CHAR_ARRAY_CACHE_USES, Long.toString(BufferManager.getCharBufferUses()), "Total number of char[] buffers allocated");

			addStat(objs, MasterServerStat.DAEMON_CONCURRENCY, Integer.toString(DaemonHandler.getDaemonConcurrency()), "Number of active daemon connections");
			addStat(objs, MasterServerStat.DAEMON_CONNECTIONS, Integer.toString(DaemonHandler.getDaemonConnections()), "Current number of daemon connections");
			addStat(objs, MasterServerStat.DAEMON_CONNECTS, Integer.toString(DaemonHandler.getDaemonConnects()), "Number of times connecting to daemons");
			addStat(objs, MasterServerStat.DAEMON_COUNT, Integer.toString(DaemonHandler.getDaemonCount()), "Number of daemons that have been accessed");
			addStat(objs, MasterServerStat.DAEMON_DOWN_COUNT, Integer.toString(DaemonHandler.getDownDaemonCount()), "Number of daemons that are currently unavailable");
			addStat(objs, MasterServerStat.DAEMON_MAX_CONCURRENCY, Integer.toString(DaemonHandler.getDaemonMaxConcurrency()), "Peak number of active daemon connections");
			addStat(objs, MasterServerStat.DAEMON_POOL_SIZE, Integer.toString(DaemonHandler.getDaemonPoolSize()), "Maximum number of daemon connections");
			addStat(objs, MasterServerStat.DAEMON_TOTAL_TIME, StringUtility.getDecimalTimeLengthString(DaemonHandler.getDaemonTotalTime()), "Total time spent accessing daemons");
			addStat(objs, MasterServerStat.DAEMON_TRANSACTIONS, Long.toString(DaemonHandler.getDaemonTransactions()), "Number of transactions processed by daemons");

			AOConnectionPool dbPool=MasterDatabase.getDatabase().getConnectionPool();
			addStat(objs, MasterServerStat.DB_CONCURRENCY, Integer.toString(dbPool.getConcurrency()), "Number of active database connections");
			addStat(objs, MasterServerStat.DB_CONNECTIONS, Integer.toString(dbPool.getConnectionCount()), "Current number of database connections");
			addStat(objs, MasterServerStat.DB_CONNECTS, Long.toString(dbPool.getConnects()), "Number of times connecting to the database");
			addStat(objs, MasterServerStat.DB_MAX_CONCURRENCY, Integer.toString(dbPool.getMaxConcurrency()), "Peak number of active database connections");
			addStat(objs, MasterServerStat.DB_POOL_SIZE, Integer.toString(dbPool.getPoolSize()), "Maximum number of database connections");
			addStat(objs, MasterServerStat.DB_TOTAL_TIME, StringUtility.getDecimalTimeLengthString(dbPool.getTotalTime()), "Total time spent accessing the database");
			addStat(objs, MasterServerStat.DB_TRANSACTIONS, Long.toString(dbPool.getTransactionCount()), "Number of transactions committed by the database");

			FifoFile entropyFile=RandomHandler.getFifoFile();
			addStat(objs, MasterServerStat.ENTROPY_AVAIL, Long.toString(entropyFile.getLength()), "Number of bytes of entropy currently available");
			addStat(objs, MasterServerStat.ENTROPY_POOLSIZE, Long.toString(entropyFile.getMaximumFifoLength()), "Maximum number of bytes of entropy");
			FifoFileInputStream entropyIn=entropyFile.getInputStream();
			addStat(objs, MasterServerStat.ENTROPY_READ_BYTES, Long.toString(entropyIn.getReadBytes()), "Number of bytes read from the entropy pool");
			addStat(objs, MasterServerStat.ENTROPY_READ_COUNT, Long.toString(entropyIn.getReadCount()), "Number of reads from the entropy pool");
			FifoFileOutputStream entropyOut=entropyFile.getOutputStream();
			addStat(objs, MasterServerStat.ENTROPY_WRITE_BYTES, Long.toString(entropyOut.getWriteBytes()), "Number of bytes written to the entropy pool");
			addStat(objs, MasterServerStat.ENTROPY_WRITE_COUNT, Long.toString(entropyOut.getWriteCount()), "Number of writes to the entropy pool");

			addStat(objs, MasterServerStat.MEMORY_FREE, Long.toString(Runtime.getRuntime().freeMemory()), "Free virtual machine memory in bytes");
			addStat(objs, MasterServerStat.MEMORY_TOTAL, Long.toString(Runtime.getRuntime().totalMemory()), "Total virtual machine memory in bytes");

			addStat(objs, MasterServerStat.PROTOCOL_VERSION, StringUtility.join(AOServProtocol.Version.values(), ", "), "Supported AOServProtocol version numbers");

			addStat(objs, MasterServerStat.REQUEST_CONCURRENCY, Integer.toString(getRequestConcurrency()), "Current number of client requests being processed");
			addStat(objs, MasterServerStat.REQUEST_CONNECTIONS, Long.toString(getRequestConnections()), "Number of connections received from clients");
			addStat(objs, MasterServerStat.REQUEST_MAX_CONCURRENCY, Integer.toString(getRequestMaxConcurrency()), "Peak number of client requests being processed");
			addStat(objs, MasterServerStat.REQUEST_TOTAL_TIME, StringUtility.getDecimalTimeLengthString(getRequestTotalTime()), "Total time spent processing client requests");
			addStat(objs, MasterServerStat.REQUEST_TRANSACTIONS, Long.toString(getRequestTransactions()), "Number of client requests processed");

			addStat(objs, MasterServerStat.THREAD_COUNT, Integer.toString(ThreadUtility.getThreadCount()), "Current number of virtual machine threads");

			addStat(objs, MasterServerStat.UPTIME, StringUtility.getDecimalTimeLengthString(System.currentTimeMillis()-getStartTime()), "Amount of time the master server has been running");

			writeObjects(source, out, provideProgress, objs);
		} catch(IOException err) {
			logger.log(Level.SEVERE, null, err);
			out.writeByte(AOServProtocol.IO_EXCEPTION);
			String message=err.getMessage();
			out.writeUTF(message==null?"":message);
		}
	}

	/**
	 * @see  #checkAccessHostname(MasterDatabaseConnection,RequestSource,String,String,String[])
	 */
	public static void checkAccessHostname(DatabaseConnection conn, RequestSource source, String action, String hostname) throws IOException, SQLException {
		checkAccessHostname(conn, source, action, hostname, DNSHandler.getDNSTLDs(conn));
	}

	/**
	 * Determines if this hostname may be used by the source.  The dns_forbidden_zones,
	 * dns_zones, httpd_site_urls, and email_domains tables are searched, in this order,
	 * for a match.  If a match is found with an owner of this source, then access is
	 * granted.  If the source is not restricted by either server or business, then
	 * access is granted and the previous checks are avoided.
	 *
	 * TODO: What about ending '.' on zones vs DomainName objects here?
	 */
	public static void checkAccessHostname(DatabaseConnection conn, RequestSource source, String action, String hostname, List<DomainName> tlds) throws IOException, SQLException {
		String zone = DNSZoneTable.getDNSZoneForHostname(hostname, tlds);

		if(conn.executeBooleanQuery(
			"select (select zone from dns_forbidden_zones where zone=?) is not null",
			zone
		)) throw new SQLException("Access to this hostname forbidden: Exists in dns_forbidden_zones: "+hostname);

		UserId username = source.getUsername();

		String existingZone=conn.executeStringQuery(
			Connection.TRANSACTION_READ_COMMITTED,
			true,
			false,
			"select zone from dns_zones where zone=?",
			zone
		);
		if(existingZone!=null && !DNSHandler.canAccessDNSZone(conn, source, existingZone)) throw new SQLException("Access to this hostname forbidden: Exists in dns_zones: "+hostname);

		String domain = zone.substring(0, zone.length()-1);

		IntList httpdSites=conn.executeIntListQuery(
			"select\n"
			+ "  hsb.httpd_site\n"
			+ "from\n"
			+ "  httpd_site_urls hsu,\n"
			+ "  httpd_site_binds hsb\n"
			+ "where\n"
			+ "  (hsu.hostname=? or hsu.hostname like ?)\n"
			+ "  and hsu.httpd_site_bind=hsb.pkey",
			domain,
			"%."+domain
		);
		// Must be able to access all of the sites
		for(int httpdSite : httpdSites) if(!HttpdHandler.canAccessHttpdSite(conn, source, httpdSite)) throw new SQLException("Access to this hostname forbidden: Exists in httpd_site_urls: "+hostname);

		IntList emailDomains=conn.executeIntListQuery(
			"select pkey from email_domains where (domain=? or domain like ?)",
			domain,
			"%."+domain
		);
		// Must be able to access all of the domains
		for(int emailDomain : emailDomains) if(!EmailHandler.canAccessEmailDomain(conn, source, emailDomain)) throw new SQLException("Access to this hostname forbidden: Exists in email_domains: "+hostname);
	}

	public static com.aoindustries.aoserv.client.MasterServer[] getMasterServers(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		synchronized(masterServersLock) {
			if(masterServers==null) masterServers=new HashMap<>();
			com.aoindustries.aoserv.client.MasterServer[] mss=masterServers.get(username);
			if(mss!=null) return mss;
			try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement("select ms.* from master_users mu, master_servers ms where mu.is_active and mu.username=? and mu.username=ms.username")) {
				try {
					List<com.aoindustries.aoserv.client.MasterServer> v=new ArrayList<>();
					pstmt.setString(1, username.toString());
					try (ResultSet results = pstmt.executeQuery()) {
						while(results.next()) {
							com.aoindustries.aoserv.client.MasterServer ms=new com.aoindustries.aoserv.client.MasterServer();
							ms.init(results);
							v.add(ms);
						}
					}
					mss=new com.aoindustries.aoserv.client.MasterServer[v.size()];
					v.toArray(mss);
					masterServers.put(username, mss);
					return mss;
				} catch(SQLException e) {
					throw new WrappedSQLException(e, pstmt);
				}
			}
		}
	}

	public static Map<UserId,MasterUser> getMasterUsers(DatabaseConnection conn) throws IOException, SQLException {
		synchronized(masterUsersLock) {
			if(masterUsers==null) {
				try (Statement stmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement()) {
					Map<UserId,MasterUser> table=new HashMap<>();
					ResultSet results=stmt.executeQuery("select * from master_users where is_active");
					while(results.next()) {
						MasterUser mu=new MasterUser();
						mu.init(results);
						table.put(mu.getKey(), mu);
					}
					masterUsers = Collections.unmodifiableMap(table);
				}
			}
			return masterUsers;
		}
	}

	public static MasterUser getMasterUser(DatabaseConnection conn, UserId username) throws IOException, SQLException {
		return getMasterUsers(conn).get(username);
	}

	/**
	 * Gets the hosts that are allowed for the provided username.
	 */
	public static boolean isHostAllowed(DatabaseConnection conn, UserId username, String host) throws IOException, SQLException {
		Map<UserId,List<HostAddress>> myMasterHosts;
		synchronized(masterHostsLock) {
			if(masterHosts==null) {
				try (Statement stmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement()) {
					Map<UserId,List<HostAddress>> table=new HashMap<>();
					ResultSet results=stmt.executeQuery("select mh.username, mh.host from master_hosts mh, master_users mu where mh.username=mu.username and mu.is_active");
					while(results.next()) {
						UserId un;
						HostAddress ho;
						try {
							un=UserId.valueOf(results.getString(1));
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
		if(getMasterUser(conn, username)!=null) {
			List<HostAddress> hosts=myMasterHosts.get(username);
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
			return BusinessHandler.getBusinessAdministrator(conn, username)!=null;
		}
	}

	public static void writeObject(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		AOServProtocol.Version version = source.getProtocolVersion();
		Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
		try (PreparedStatement pstmt = dbConn.prepareStatement(sql)) {
			try {
				DatabaseConnection.setParams(dbConn, pstmt, params);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						obj.init(results);
						out.writeByte(AOServProtocol.NEXT);
						obj.write(out, version);
					} else out.writeByte(AOServProtocol.DONE);
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	public static void fetchObjects(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		AOServProtocol.Version version=source.getProtocolVersion();

		Connection dbConn=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false);
		try (PreparedStatement pstmt = dbConn.prepareStatement("declare fetch_objects cursor for "+sql)) {
			try {
				DatabaseConnection.setParams(dbConn, pstmt, params);
				pstmt.executeUpdate();
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}

		String sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from fetch_objects";
		Statement stmt = dbConn.createStatement();
		try {
			while(true) {
				int batchSize=0;
				try (ResultSet results = stmt.executeQuery(sqlString)) {
					while(results.next()) {
						obj.init(results);
						out.writeByte(AOServProtocol.NEXT);
						obj.write(out, version);
						batchSize++;
					}
				}
				if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
			}
		} catch(SQLException err) {
			throw new WrappedSQLException(err, sqlString);
		} finally {
			stmt.executeUpdate("close fetch_objects");
			stmt.close();
		}
	}

	public static void writeObjects(
		DatabaseConnection conn,
		RequestSource source,
		CompressedDataOutputStream out,
		boolean provideProgress,
		AOServObject<?,?> obj,
		String sql,
		Object ... params
	) throws IOException, SQLException {
		if(!provideProgress) fetchObjects(conn, source, out, obj, sql, params);
		else {
			Connection dbConn = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true);
			try (PreparedStatement pstmt = dbConn.prepareStatement(sql)) {
				try {
					DatabaseConnection.setParams(dbConn, pstmt, params);
					try (ResultSet results = pstmt.executeQuery()) {
						writeObjects(source, out, provideProgress, obj, results);
					}
				} catch(SQLException err) {
					throw new WrappedSQLException(err, pstmt);
				}
			}
		}
	}

	public static void writePenniesCheckBusiness(
		DatabaseConnection conn,
		RequestSource source,
		String action,
		AccountingCode accounting,
		CompressedDataOutputStream out,
		String sql,
		String param1
	) throws IOException, SQLException {
		BusinessHandler.checkAccessBusiness(conn, source, action, accounting);
		try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql)) {
			try {
				pstmt.setString(1, param1);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						out.writeByte(AOServProtocol.DONE);
						out.writeCompressedInt(SQLUtility.getPennies(results.getString(1)));
					} else throw new NoRowException();
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	public static void writePenniesCheckBusiness(
		DatabaseConnection conn,
		RequestSource source,
		String action,
		AccountingCode accounting,
		CompressedDataOutputStream out,
		String sql,
		String param1,
		Timestamp param2
	) throws IOException, SQLException {
		BusinessHandler.checkAccessBusiness(conn, source, action, accounting);
		try (PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql)) {
			try {
				pstmt.setString(1, param1);
				pstmt.setTimestamp(2, param2);
				try (ResultSet results = pstmt.executeQuery()) {
					if(results.next()) {
						out.writeByte(AOServProtocol.DONE);
						out.writeCompressedInt(SQLUtility.getPennies(results.getString(1)));
					} else throw new NoRowException();
				}
			} catch(SQLException err) {
				throw new WrappedSQLException(err, pstmt);
			}
		}
	}

	public static void invalidateTable(SchemaTable.TableID tableID) {
		if(tableID==SchemaTable.TableID.MASTER_HOSTS) {
			synchronized(masterHostsLock) {
				masterHosts=null;
			}
		} else if(tableID==SchemaTable.TableID.MASTER_SERVERS) {
			synchronized(masterHostsLock) {
				masterHosts=null;
			}
			synchronized(masterServersLock) {
				masterServers=null;
			}
		} else if(tableID==SchemaTable.TableID.MASTER_USERS) {
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

	public static void updateAOServProtocolLastUsed(DatabaseConnection conn, AOServProtocol.Version protocolVersion) throws IOException, SQLException {
		conn.executeUpdate("update aoserv_protocols set last_used=now()::date where version=? and (last_used is null or last_used<now()::date)", protocolVersion.getVersion());
	}
}
