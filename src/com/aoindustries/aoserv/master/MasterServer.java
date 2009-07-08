package com.aoindustries.aoserv.master;

/*
 * Copyright 2000-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOSHCommand;
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.DNSRecord;
import com.aoindustries.aoserv.client.DNSZoneTable;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.Language;
import com.aoindustries.aoserv.client.MasterHistory;
import com.aoindustries.aoserv.client.MasterProcess;
import com.aoindustries.aoserv.client.MasterServerStat;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.Transaction;
import com.aoindustries.aoserv.client.TransactionSearchCriteria;
import com.aoindustries.io.BitRateProvider;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.io.FifoFile;
import com.aoindustries.io.FifoFileInputStream;
import com.aoindustries.io.FifoFileOutputStream;
import com.aoindustries.profiler.Profiler;
import com.aoindustries.sql.AOConnectionPool;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.util.BufferManager;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.IntArrayList;
import com.aoindustries.util.IntList;
import com.aoindustries.util.SortedArrayList;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.ThreadUtility;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private static Map<String,MasterUser> masterUsers;
    private static Map<String,List<String>> masterHosts;
    private static Map<String,com.aoindustries.aoserv.client.MasterServer[]> masterServers;

    /**
     * The time the system started up
     */
    private static final long startTime=System.currentTimeMillis();

    /**
     * The central list of all objects that are notified of
     * cache updates.
     */
    private static final List<RequestSource> cacheListeners=new ArrayList<RequestSource>();

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
    private static long lastID=-1;

    private static int concurrency=0;
    private static int maxConcurrency=0;

    private static long requestCount=0;
    private static long totalTime=0;

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
    }

    /**
     * Gets the interface address this server is listening on.
     */
    final public String getBindAddress() {
        return serverBind;
    }

    public static long getNextConnectorID() {
        synchronized(MasterServer.class) {
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
        return concurrency;
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
        return maxConcurrency;
    }

    public static long getRequestTotalTime() {
        synchronized(MasterServer.class) {
            return totalTime;
        }
    }

    public static long getRequestTransactions() {
        synchronized(MasterServer.class) {
            return requestCount;
        }
    }

    public static long getStartTime() {
        return startTime;
    }

    /** Used to avoid cloning of array for each access. */
    private static final AOServProtocol.CommandID[] commandIDs = AOServProtocol.CommandID.values();

    /** Copy used to avoid copying for each access. */
    private static final SchemaTable.TableID[] tableIDs = SchemaTable.TableID.values();

    /**
     * Handles a single request and then returns.
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
        synchronized(MasterServer.class) {
            int c=++concurrency;
            if(c>maxConcurrency) maxConcurrency=c;
            requestCount++;
        }
        long requestStartTime=System.currentTimeMillis();
        try {
            if(taskCodeOrdinal==-1) {
                // EOF
                process.setCommand("quit");
                synchronized(MasterServer.class) {
                    addTime=false;
                    concurrency--;
                }
                return false;
            } else {
                final boolean done;
                AOServProtocol.CommandID taskCode=commandIDs[taskCodeOrdinal];
                switch(taskCode) {
                    case LISTEN_CACHES :
                        process.setCommand("listen_caches");
                        synchronized(MasterServer.class) {
                            addTime=false;
                            concurrency--;
                        }
                        // This method normally never leaves for this command
                        try {
                            addCacheListener(source);
                            final DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
                            try {
                                AOServProtocol.Version protocolVersion = source.getProtocolVersion();
                                while(!BusinessHandler.isBusinessAdministratorDisabled(conn, source.getUsername())) {
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
                                            || protocolVersion.compareTo(AOServProtocol.Version.VERSION_1_47)<0
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
                        synchronized(MasterServer.class) {
                            addTime=false;
                            concurrency--;
                        }
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
                    Thread currentThread=Thread.currentThread();
                    try {
                        InvalidateList invalidateList=new InvalidateList();
                        IntArrayList clientInvalidateList=null;
                        byte resp1=-1;

                        int resp2Int=-1;
                        boolean hasResp2Int=false;

                        long resp2Long=-1;
                        boolean hasResp2Long=false;

                        short resp2Short=-1;
                        boolean hasResp2Short=false;

                        String resp2String=null;

                        String resp2NullLongString = null;
                        boolean hasResp2NullLongString = false;

                        String resp2LongString = null;

                        boolean resp2Boolean=false;
                        boolean hasResp2Boolean=false;

                        InboxAttributes resp2InboxAttributes=null;
                        boolean hasResp2InboxAttributes=false;

                        long[] resp2LongArray=null;
                        boolean hasResp2LongArray=false;

                        String resp3String=null;
                        String resp4String=null;

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
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                            int server;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
                                                String hostname=in.readBoolean()?in.readUTF():null;
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
                                                server==-1 ? null : Integer.valueOf(server)
                                            );
                                            TableHandler.invalidate(
                                                conn,
                                                source,
                                                invalidateList,
                                                tableID,
                                                server
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case ADD : {
                                        int clientTableID=in.readCompressedInt();
                                        SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                        if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                        switch(tableID) {
                                            case BUSINESS_ADMINISTRATORS :
                                                {
                                                    String username=in.readUTF().trim();
                                                    String name=in.readUTF().trim();
                                                    String title=in.readBoolean()?in.readUTF().trim():null;
                                                    long birthday=in.readLong();
                                                    boolean isPrivate=in.readBoolean();
                                                    String workPhone=in.readUTF().trim();
                                                    String homePhone=in.readBoolean()?in.readUTF().trim():null;
                                                    String cellPhone=in.readBoolean()?in.readUTF().trim():null;
                                                    String fax=in.readBoolean()?in.readUTF().trim():null;
                                                    String email=in.readUTF().trim();
                                                    String address1=in.readBoolean()?in.readUTF().trim():null;
                                                    String address2=in.readBoolean()?in.readUTF().trim():null;
                                                    String city=in.readBoolean()?in.readUTF().trim():null;
                                                    String state=in.readBoolean()?in.readUTF().trim():null;
                                                    String country=in.readBoolean()?in.readUTF().trim():null;
                                                    String zip=in.readBoolean()?in.readUTF().trim():null;
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
                                                        birthday==BusinessAdministrator.NONE?null:new java.util.Date(birthday),
                                                        isPrivate?Boolean.TRUE:Boolean.FALSE,
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case BUSINESS_PROFILES :
                                                {
                                                    String accounting=in.readUTF();
                                                    String name=in.readUTF().trim();
                                                    boolean isPrivate=in.readBoolean();
                                                    String phone=in.readUTF().trim();
                                                    String fax=in.readBoolean()?in.readUTF().trim():null;
                                                    String address1=in.readUTF().trim();
                                                    String address2=in.readBoolean()?in.readUTF().trim():null;
                                                    String city=in.readUTF().trim();
                                                    String state=in.readBoolean()?in.readUTF().trim():null;
                                                    String country=in.readUTF();
                                                    String zip=in.readBoolean()?in.readUTF().trim():null;
                                                    boolean sendInvoice=in.readBoolean();
                                                    String billingContact=in.readUTF().trim();
                                                    String billingEmail=in.readUTF().trim();
                                                    String technicalContact=in.readUTF().trim();
                                                    String technicalEmail=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_BUSINESS_PROFILE,
                                                        accounting,
                                                        name,
                                                        isPrivate?Boolean.TRUE:Boolean.FALSE,
                                                        phone,
                                                        fax,
                                                        address1,
                                                        address2,
                                                        city,
                                                        state,
                                                        country,
                                                        zip,
                                                        sendInvoice?Boolean.TRUE:Boolean.FALSE,
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case BUSINESS_SERVERS :
                                                {
                                                    String accounting=in.readUTF();
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
                                                        Integer.valueOf(server)
                                                    );
                                                    int pkey=BusinessHandler.addBusinessServer(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        accounting,
                                                        server
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case BUSINESSES :
                                                {
                                                    String accounting=in.readUTF().trim();
                                                    String contractVersion=in.readBoolean()?in.readUTF().trim():null;
                                                    int defaultServer;
                                                    String hostname;
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_30)<=0) {
                                                        defaultServer = -1;
                                                        hostname = in.readUTF();
                                                    } else {
                                                        defaultServer = in.readCompressedInt();
                                                        hostname = null;
                                                    }
                                                    String parent=in.readUTF();
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
                                                        can_add_backup_servers?Boolean.TRUE:Boolean.FALSE,
                                                        can_add_businesses?Boolean.TRUE:Boolean.FALSE,
                                                        can_see_prices?Boolean.TRUE:Boolean.FALSE,
                                                        billParent?Boolean.TRUE:Boolean.FALSE
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
                                                    resp1=AOServProtocol.DONE;
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
                                                        String description=in.readBoolean()?in.readUTF().trim():null;
                                                        throw new SQLException("add_credit_card for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
                                                    }
                                                    String processorName = in.readUTF();
                                                    String accounting = in.readUTF();
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
                                                        encryptionFrom==-1 ? null : Integer.valueOf(encryptionFrom),
                                                        encryptionRecipient==-1 ? null : Integer.valueOf(encryptionRecipient)
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case CREDIT_CARD_TRANSACTIONS :
                                                {
                                                    String processor = in.readUTF();
                                                    String accounting = in.readUTF();
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
                                                    String creditCardCreatedBy = in.readUTF();
                                                    String creditCardPrincipalName = in.readNullUTF();
                                                    String creditCardAccounting = in.readUTF();
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case CVS_REPOSITORIES :
                                                {
                                                    int aoServer=in.readCompressedInt();
                                                    String path=in.readUTF();
                                                    int lsa=in.readCompressedInt();
                                                    int lsg=in.readCompressedInt();
                                                    long mode=in.readLong();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_CVS_REPOSITORY,
                                                        Integer.valueOf(aoServer),
                                                        path,
                                                        Integer.valueOf(lsa),
                                                        Integer.valueOf(lsg),
                                                        Long.toOctalString(mode)
                                                    );
                                                    int pkey=CvsHandler.addCvsRepository(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        aoServer,
                                                        path,
                                                        lsa,
                                                        lsg,
                                                        mode
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case DISABLE_LOG :
                                                {
                                                    String accounting=in.readUTF();
                                                    String disableReason=in.readBoolean()?in.readUTF().trim():null;
                                                    process.setCommand(
                                                        "add_disable_log",
                                                        accounting,
                                                        disableReason
                                                    );
                                                    int pkey=BusinessHandler.addDisableLog(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        accounting,
                                                        disableReason
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case DNS_RECORDS :
                                                {
                                                    String zone=in.readUTF();
                                                    String domain=in.readUTF().trim();
                                                    String type=in.readUTF();
                                                    int mx_priority=in.readCompressedInt();
                                                    String destination=in.readUTF().trim();
                                                    int ttl=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_DNS_RECORD,
                                                        zone,
                                                        domain,
                                                        type,
                                                        mx_priority==DNSRecord.NO_MX_PRIORITY?null:Integer.valueOf(mx_priority),
                                                        destination,
                                                        ttl==DNSRecord.NO_TTL?null:Integer.valueOf(ttl)
                                                    );
                                                    int pkey=DNSHandler.addDNSRecord(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        zone,
                                                        domain,
                                                        type,
                                                        mx_priority,
                                                        destination,
                                                        ttl
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case DNS_ZONES :
                                                {
                                                    String packageName=in.readUTF();
                                                    String zone=in.readUTF().trim();
                                                    String ip=in.readUTF().trim();
                                                    int ttl=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_DNS_ZONE,
                                                        packageName,
                                                        zone,
                                                        ip,
                                                        Integer.valueOf(ttl)
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_ADDRESSES :
                                                {
                                                    String address=in.readUTF().trim();
                                                    int domain=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_ADDRESS,
                                                        address,
                                                        Integer.valueOf(domain)
                                                    );
                                                    int pkey=EmailHandler.addEmailAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        address,
                                                        domain
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_DOMAINS :
                                                {
                                                    String domain=in.readUTF().trim();
                                                    int aoServer=in.readCompressedInt();
                                                    String packageName=in.readUTF();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_DOMAIN,
                                                        domain,
                                                        Integer.valueOf(aoServer),
                                                        packageName
                                                    );
                                                    int pkey=EmailHandler.addEmailDomain(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        domain,
                                                        aoServer,
                                                        packageName
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_FORWARDING :
                                                {
                                                    int address=in.readCompressedInt();
                                                    String destination=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_FORWARDING,
                                                        Integer.valueOf(address),
                                                        destination
                                                    );
                                                    int pkey=EmailHandler.addEmailForwarding(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        address,
                                                        destination
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_LIST_ADDRESSES :
                                                {
                                                    int address=in.readCompressedInt();
                                                    int email_list=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_LIST_ADDRESS,
                                                        Integer.valueOf(address),
                                                        Integer.valueOf(email_list)
                                                    );
                                                    int pkey=EmailHandler.addEmailListAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        address,
                                                        email_list
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_LISTS :
                                                {
                                                    String path=in.readUTF();
                                                    int linuxServerAccount=in.readCompressedInt();
                                                    int linuxServerGroup=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_LIST,
                                                        path,
                                                        Integer.valueOf(linuxServerAccount),
                                                        Integer.valueOf(linuxServerGroup)
                                                    );
                                                    int pkey=EmailHandler.addEmailList(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        path,
                                                        linuxServerAccount,
                                                        linuxServerGroup
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_PIPE_ADDRESSES :
                                                {
                                                    int address=in.readCompressedInt();
                                                    int pipe=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_PIPE_ADDRESS,
                                                        Integer.valueOf(address),
                                                        Integer.valueOf(pipe)
                                                    );
                                                    int pkey=EmailHandler.addEmailPipeAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        address,
                                                        pipe
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_PIPES :
                                                {
                                                    int ao_server=in.readCompressedInt();
                                                    String path=in.readUTF();
                                                    String packageName=in.readUTF();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_PIPE,
                                                        Integer.valueOf(ao_server),
                                                        path,
                                                        packageName
                                                    );
                                                    int pkey=EmailHandler.addEmailPipe(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        ao_server,
                                                        path,
                                                        packageName
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case EMAIL_SMTP_RELAYS :
                                                {
                                                    process.setPriority(Thread.NORM_PRIORITY+1);
                                                    currentThread.setPriority(Thread.NORM_PRIORITY+1);

                                                    String packageName=in.readUTF();
                                                    int aoServer=in.readCompressedInt();
                                                    String host=in.readUTF().trim();
                                                    String type=in.readUTF();
                                                    long duration=in.readLong();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_EMAIL_SMTP_RELAY,
                                                        packageName,
                                                        aoServer==-1?null:Integer.valueOf(aoServer),
                                                        host,
                                                        type,
                                                        Long.valueOf(duration)
                                                    );
                                                    int pkey=EmailHandler.addEmailSmtpRelay(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        packageName,
                                                        aoServer,
                                                        host,
                                                        type,
                                                        duration
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
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
                                                        Integer.valueOf(replication),
                                                        new java.util.Date(fflStartTime),
                                                        new java.util.Date(endTime),
                                                        Integer.valueOf(scanned),
                                                        Integer.valueOf(updated),
                                                        Long.valueOf(bytes),
                                                        isSuccessful?Boolean.TRUE:Boolean.FALSE
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
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
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_31)<0) {
                                                        throw new IOException(AOSHCommand.ADD_FILE_BACKUP_SETTING+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
                                                    }
                                                    process.setCommand(
                                                        AOSHCommand.ADD_FILE_BACKUP_SETTING,
                                                        Integer.valueOf(replication),
                                                        path,
                                                        backupEnabled?Boolean.TRUE:Boolean.FALSE
                                                    );
                                                    int pkey=BackupHandler.addFileBackupSetting(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        replication,
                                                        path,
                                                        backupEnabled
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case FTP_GUEST_USERS :
                                                {
                                                    String username=in.readUTF();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_SHARED_TOMCATS :
                                                {
                                                    String name=in.readUTF().trim();
                                                    int aoServer=in.readCompressedInt();
                                                    int version=in.readCompressedInt();
                                                    String linuxServerAccount=in.readUTF();
                                                    String linuxServerGroup=in.readUTF();
                                                    boolean isSecure=in.readBoolean();
                                                    boolean isOverflow=in.readBoolean();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_SHARED_TOMCAT,
                                                        name,
                                                        Integer.valueOf(aoServer),
                                                        Integer.valueOf(version),
                                                        linuxServerAccount,
                                                        linuxServerGroup,
                                                        isSecure?Boolean.TRUE:Boolean.FALSE,
                                                        isOverflow?Boolean.TRUE:Boolean.FALSE
                                                    );
                                                    int pkey=HttpdHandler.addHttpdSharedTomcat(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case HTTPD_JBOSS_SITES :
                                                {
                                                    int aoServer=in.readCompressedInt();
                                                    String siteName=in.readUTF().trim();
                                                    String packageName=in.readUTF();
                                                    String username=in.readUTF();
                                                    String group=in.readUTF();
                                                    String serverAdmin=in.readUTF().trim();
                                                    boolean useApache=in.readBoolean();
                                                    int ipAddress=in.readCompressedInt();
                                                    String primaryHttpHostname=in.readUTF().trim();
                                                    int len=in.readCompressedInt();
                                                    String[] altHttpHostnames=new String[len];
                                                    for(int c=0;c<len;c++) altHttpHostnames[c]=in.readUTF().trim();
                                                    int jBossVersion=in.readCompressedInt();
                                                    String contentSrc=in.readBoolean()?in.readUTF():null;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_JBOSS_SITE,
                                                        Integer.valueOf(aoServer),
                                                        siteName,
                                                        packageName,
                                                        username,
                                                        group,
                                                        serverAdmin,
                                                        useApache?Boolean.TRUE:Boolean.FALSE,
                                                        ipAddress==-1?null:Integer.valueOf(ipAddress),
                                                        primaryHttpHostname,
                                                        altHttpHostnames,
                                                        Integer.valueOf(jBossVersion),
                                                        contentSrc
                                                    );

                                                    int pkey=HttpdHandler.addHttpdJBossSite(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
                                                {
                                                    int httpd_site=in.readCompressedInt();
                                                    String path = in.readUTF();
                                                    boolean isRegularExpression = in.readBoolean();
                                                    String authName = in.readUTF();
                                                    String authGroupFile = in.readUTF();
                                                    String authUserFile = in.readUTF();
                                                    String require = in.readUTF();
                                                    process.setCommand(
                                                        "add_httpd_site_authenticated_location",
                                                        Integer.valueOf(httpd_site),
                                                        path,
                                                        isRegularExpression?Boolean.TRUE:Boolean.FALSE,
                                                        authName,
                                                        authGroupFile,
                                                        authUserFile,
                                                        require
                                                    );
                                                    int pkey=HttpdHandler.addHttpdSiteAuthenticatedLocation(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;							
                                            case HTTPD_SITE_URLS :
                                                {
                                                    int hsb_pkey=in.readCompressedInt();
                                                    String hostname=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_SITE_URL,
                                                        Integer.valueOf(hsb_pkey),
                                                        hostname
                                                    );
                                                    int pkey=HttpdHandler.addHttpdSiteURL(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        hsb_pkey,
                                                        hostname
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case HTTPD_TOMCAT_CONTEXTS :
                                                {
                                                    int tomcat_site=in.readCompressedInt();
                                                    String className=in.readBoolean()?in.readUTF().trim():null;
                                                    boolean cookies=in.readBoolean();
                                                    boolean crossContext=in.readBoolean();
                                                    String docBase=in.readUTF().trim();
                                                    boolean override=in.readBoolean();
                                                    String path=in.readUTF().trim();
                                                    boolean privileged=in.readBoolean();
                                                    boolean reloadable=in.readBoolean();
                                                    boolean useNaming=in.readBoolean();
                                                    String wrapperClass=in.readBoolean()?in.readUTF().trim():null;
                                                    int debug=in.readCompressedInt();
                                                    String workDir=in.readBoolean()?in.readUTF().trim():null;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_TOMCAT_CONTEXT,
                                                        Integer.valueOf(tomcat_site),
                                                        className,
                                                        cookies?Boolean.TRUE:Boolean.FALSE,
                                                        crossContext?Boolean.TRUE:Boolean.FALSE,
                                                        docBase,
                                                        override?Boolean.TRUE:Boolean.FALSE,
                                                        path,
                                                        privileged?Boolean.TRUE:Boolean.FALSE,
                                                        reloadable?Boolean.TRUE:Boolean.FALSE,
                                                        useNaming?Boolean.TRUE:Boolean.FALSE,
                                                        wrapperClass,
                                                        Integer.valueOf(debug),
                                                        workDir
                                                    );
                                                    int pkey=HttpdHandler.addHttpdTomcatContext(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
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
                                                    int pkey=HttpdHandler.addHttpdTomcatDataSource(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
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
                                                    int pkey=HttpdHandler.addHttpdTomcatParameter(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        tomcat_context,
                                                        name,
                                                        value,
                                                        override,
                                                        description
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;							
                                            case HTTPD_TOMCAT_SHARED_SITES :
                                                {
                                                    int aoServer=in.readCompressedInt();
                                                    String siteName=in.readUTF().trim();
                                                    String packageName=in.readUTF().trim();
                                                    String username=in.readUTF().trim();
                                                    String group=in.readUTF().trim();
                                                    String serverAdmin=in.readUTF().trim();
                                                    boolean useApache=in.readBoolean();
                                                    int ipAddress=in.readCompressedInt();
                                                    String primaryHttpHostname=in.readUTF().trim();
                                                    int len=in.readCompressedInt();
                                                    String[] altHttpHostnames=new String[len];
                                                    for(int c=0;c<len;c++) altHttpHostnames[c]=in.readUTF().trim();
                                                    String sharedTomcatName=in.readBoolean()?in.readUTF().trim():null;
                                                    int version=in.readCompressedInt();
                                                    String contentSrc=in.readBoolean()?in.readUTF().trim():null;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_TOMCAT_SHARED_SITE,
                                                        Integer.valueOf(aoServer),
                                                        siteName,
                                                        packageName,
                                                        username,
                                                        group,
                                                        serverAdmin,
                                                        useApache?Boolean.TRUE:Boolean.FALSE,
                                                        ipAddress==-1?null:Integer.valueOf(ipAddress),
                                                        primaryHttpHostname,
                                                        altHttpHostnames,
                                                        sharedTomcatName,
                                                        version==-1?null:Integer.valueOf(version),
                                                        contentSrc
                                                    );

                                                    int pkey=HttpdHandler.addHttpdTomcatSharedSite(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case HTTPD_TOMCAT_STD_SITES :
                                                {
                                                    int aoServer=in.readCompressedInt();
                                                    String siteName=in.readUTF().trim();
                                                    String packageName=in.readUTF().trim();
                                                    String username=in.readUTF().trim();
                                                    String group=in.readUTF().trim();
                                                    String serverAdmin=in.readUTF().trim();
                                                    boolean useApache=in.readBoolean();
                                                    int ipAddress=in.readCompressedInt();
                                                    String primaryHttpHostname=in.readUTF().trim();
                                                    int len=in.readCompressedInt();
                                                    String[] altHttpHostnames=new String[len];
                                                    for(int c=0;c<len;c++) altHttpHostnames[c]=in.readUTF().trim();
                                                    int tomcatVersion=in.readCompressedInt();
                                                    String contentSrc=in.readBoolean()?in.readUTF().trim():null;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_HTTPD_TOMCAT_STD_SITE,
                                                        Integer.valueOf(aoServer),
                                                        siteName,
                                                        packageName,
                                                        username,
                                                        group,
                                                        serverAdmin,
                                                        useApache?Boolean.TRUE:Boolean.FALSE,
                                                        ipAddress==-1?null:Integer.valueOf(ipAddress),
                                                        primaryHttpHostname,
                                                        altHttpHostnames,
                                                        Integer.valueOf(tomcatVersion),
                                                        contentSrc
                                                    );
                                                    int pkey=HttpdHandler.addHttpdTomcatStdSite(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case LINUX_ACC_ADDRESSES :
                                                {
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_31)<0) {
                                                        in.readCompressedInt(); // address
                                                        in.readUTF().trim(); // username
                                                        throw new IOException(AOSHCommand.ADD_LINUX_ACC_ADDRESS+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_31+", please upgrade AOServ Client.");
                                                    }
                                                    int address=in.readCompressedInt();
                                                    int lsa = in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_LINUX_ACC_ADDRESS,
                                                        Integer.valueOf(address),
                                                        lsa
                                                    );
                                                    int pkey=EmailHandler.addLinuxAccAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        address,
                                                        lsa
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case LINUX_ACCOUNTS :
                                                {
                                                    String username=in.readUTF().trim();
                                                    String primary_group=in.readUTF().trim();
                                                    String name=in.readUTF().trim();
                                                    String office_location=in.readBoolean()?in.readUTF().trim():null;
                                                    String office_phone=in.readBoolean()?in.readUTF().trim():null;
                                                    String home_phone=in.readBoolean()?in.readUTF().trim():null;
                                                    String type=in.readUTF().trim();
                                                    String shell=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_GROUP_ACCOUNTS :
                                                {
                                                    String groupName=in.readUTF().trim();
                                                    String username=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_LINUX_GROUP_ACCOUNT,
                                                        groupName,
                                                        username
                                                    );
                                                    int pkey=LinuxAccountHandler.addLinuxGroupAccount(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        groupName,
                                                        username,
                                                        false,
                                                        false
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case LINUX_GROUPS :
                                                {
                                                    String groupName=in.readUTF().trim();
                                                    String packageName=in.readUTF().trim();
                                                    String type=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_SERVER_ACCOUNTS :
                                                {
                                                    String username=in.readUTF().trim();
                                                    int aoServer=in.readCompressedInt();
                                                    String home=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_LINUX_SERVER_ACCOUNT,
                                                        username,
                                                        Integer.valueOf(aoServer),
                                                        home
                                                    );
                                                    int pkey=LinuxAccountHandler.addLinuxServerAccount(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        username,
                                                        aoServer,
                                                        home,
                                                        false
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case LINUX_SERVER_GROUPS :
                                                {
                                                    String groupName=in.readUTF().trim();
                                                    int aoServer=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_LINUX_SERVER_GROUP,
                                                        groupName,
                                                        Integer.valueOf(aoServer)
                                                    );
                                                    int pkey=LinuxAccountHandler.addLinuxServerGroup(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        groupName,
                                                        aoServer,
                                                        false
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case MAJORDOMO_LISTS :
                                                {
                                                    int majordomoServer=in.readCompressedInt();
                                                    String listName=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_MAJORDOMO_LIST,
                                                        Integer.valueOf(majordomoServer),
                                                        listName
                                                    );
                                                    int pkey=EmailHandler.addMajordomoList(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        majordomoServer,
                                                        listName
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case MAJORDOMO_SERVERS :
                                                {
                                                    int emailDomain=in.readCompressedInt();
                                                    int lsa=in.readCompressedInt();
                                                    int lsg=in.readCompressedInt();
                                                    String version=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_MAJORDOMO_SERVER,
                                                        Integer.valueOf(emailDomain),
                                                        Integer.valueOf(lsa),
                                                        Integer.valueOf(lsg),
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MYSQL_DATABASES :
                                                {
                                                    String name=in.readUTF().trim();
                                                    int mysqlServer=in.readCompressedInt();
                                                    String packageName=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_MYSQL_DATABASE,
                                                        name,
                                                        Integer.valueOf(mysqlServer),
                                                        packageName
                                                    );
                                                    int pkey=MySQLHandler.addMySQLDatabase(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        name,
                                                        mysqlServer,
                                                        packageName
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case MYSQL_DB_USERS :
                                                {
                                                    int mysql_database=in.readCompressedInt();
                                                    int mysql_server_user=in.readCompressedInt();
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
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)>=0) {
                                                        canCreateView=in.readBoolean();
                                                        canShowView=in.readBoolean();
                                                        canCreateRoutine=in.readBoolean();
                                                        canAlterRoutine=in.readBoolean();
                                                        canExecute=in.readBoolean();
                                                    } else {
                                                        canCreateView=false;
                                                        canShowView=false;
                                                        canCreateRoutine=false;
                                                        canAlterRoutine=false;
                                                        canExecute=false;
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
                                                        canExecute
                                                    );
                                                    int pkey=MySQLHandler.addMySQLDBUser(
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
                                                        canExecute
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case MYSQL_SERVER_USERS :
                                                {
                                                    String username=in.readUTF().trim();
                                                    int mysqlServer=in.readCompressedInt();
                                                    String host=in.readBoolean()?in.readUTF().trim():null;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_MYSQL_SERVER_USER,
                                                        username,
                                                        Integer.valueOf(mysqlServer),
                                                        host
                                                    );
                                                    int pkey=MySQLHandler.addMySQLServerUser(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        username,
                                                        mysqlServer,
                                                        host
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case MYSQL_USERS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case NET_BINDS :
                                                {
                                                    int server=in.readCompressedInt();
                                                    String packageName=in.readUTF().trim();
                                                    int ipAddress=in.readCompressedInt();
                                                    int port=in.readCompressedInt();
                                                    String netProtocol=in.readUTF().trim();
                                                    String appProtocol=in.readUTF().trim();
                                                    boolean openFirewall=in.readBoolean();
                                                    boolean monitoringEnabled;
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_103)<=0) {
                                                        monitoringEnabled=in.readCompressedInt()!=-1;
                                                        if(in.readBoolean()) in.readUTF();
                                                        if(in.readBoolean()) in.readUTF();
                                                        if(in.readBoolean()) in.readUTF();
                                                    } else monitoringEnabled=in.readBoolean();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_NET_BIND,
                                                        Integer.valueOf(server),
                                                        packageName,
                                                        Integer.valueOf(ipAddress),
                                                        Integer.valueOf(port),
                                                        netProtocol,
                                                        appProtocol,
                                                        openFirewall?Boolean.TRUE:Boolean.FALSE,
                                                        monitoringEnabled?Boolean.TRUE:Boolean.FALSE
                                                    );
                                                    int pkey=NetBindHandler.addNetBind(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        server,
                                                        packageName,
                                                        ipAddress,
                                                        port,
                                                        netProtocol,
                                                        appProtocol,
                                                        openFirewall,
                                                        monitoringEnabled
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case NOTICE_LOG :
                                                {
                                                    String accounting=in.readUTF().trim();
                                                    String billingContact=in.readUTF().trim();
                                                    String emailAddress=in.readUTF().trim();
                                                    int balance=in.readCompressedInt();
                                                    String type=in.readUTF().trim();
                                                    int transid=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_NOTICE_LOG,
                                                        accounting,
                                                        billingContact,
                                                        emailAddress,
                                                        SQLUtility.getDecimal(balance),
                                                        type,
                                                        Integer.valueOf(transid)
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case PACKAGES :
                                                {
                                                    String packageName=in.readUTF().trim();
                                                    String accounting=in.readUTF().trim();
                                                    int packageDefinition;
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_122)<=0) {
                                                        // Try to find a package definition owned by the source accounting with matching rates and limits
                                                        String level=in.readUTF().trim();
                                                        int rate=in.readCompressedInt();
                                                        int userLimit=in.readCompressedInt();
                                                        int additionalUserRate=in.readCompressedInt();
                                                        int popLimit=in.readCompressedInt();
                                                        int additionalPopRate=in.readCompressedInt();
                                                        String baAccounting=UsernameHandler.getBusinessForUsername(conn, source.getUsername());
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
                                                        packageDefinition=in.readCompressedInt();
                                                    }
                                                    process.setCommand(
                                                        AOSHCommand.ADD_PACKAGE,
                                                        packageName,
                                                        accounting,
                                                        Integer.valueOf(packageDefinition)
                                                    );
                                                    int pkey=PackageHandler.addPackage(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        packageName,
                                                        accounting,
                                                        packageDefinition
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case PACKAGE_DEFINITIONS :
                                                {
                                                    String accounting=in.readUTF().trim();
                                                    String category=in.readUTF().trim();
                                                    String name=in.readUTF().trim();
                                                    String version=in.readUTF().trim();
                                                    String display=in.readUTF().trim();
                                                    String description=in.readUTF().trim();
                                                    int setupFee=in.readCompressedInt();
                                                    String setupFeeTransactionType=in.readBoolean()?in.readUTF():null;
                                                    int monthlyRate=in.readCompressedInt();
                                                    String monthlyRateTransactionType=in.readUTF();
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
                                                    int pkey=PackageHandler.addPackageDefinition(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case POSTGRES_DATABASES :
                                                {
                                                    String name=in.readUTF().trim();
                                                    int postgresServer=in.readCompressedInt();
                                                    int datdba=in.readCompressedInt();
                                                    int encoding=in.readCompressedInt();
                                                    boolean enable_postgis=source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_27)>=0?in.readBoolean():false;
                                                    process.setCommand(
                                                        AOSHCommand.ADD_POSTGRES_DATABASE,
                                                        name,
                                                        Integer.valueOf(postgresServer),
                                                        Integer.valueOf(datdba),
                                                        Integer.valueOf(encoding),
                                                        enable_postgis
                                                    );
                                                    int pkey=PostgresHandler.addPostgresDatabase(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        name,
                                                        postgresServer,
                                                        datdba,
                                                        encoding,
                                                        enable_postgis
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case POSTGRES_SERVER_USERS :
                                                {
                                                    String username=in.readUTF().trim();
                                                    int postgresServer=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_POSTGRES_SERVER_USER,
                                                        username,
                                                        Integer.valueOf(postgresServer)
                                                    );
                                                    int pkey=PostgresHandler.addPostgresServerUser(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        username,
                                                        postgresServer
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case POSTGRES_USERS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case SIGNUP_REQUESTS :
                                                {
                                                    String accounting = in.readUTF();
                                                    String ip_address = in.readUTF();
                                                    int package_definition = in.readCompressedInt();
                                                    String business_name = in.readUTF();
                                                    String business_phone = in.readUTF();
                                                    String business_fax = in.readBoolean() ? in.readUTF() : null;
                                                    String business_address1 = in.readUTF();
                                                    String business_address2 = in.readBoolean() ? in.readUTF() : null;
                                                    String business_city = in.readUTF();
                                                    String business_state = in.readBoolean() ? in.readUTF() : null;
                                                    String business_country = in.readUTF();
                                                    String business_zip = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_name = in.readUTF();
                                                    String ba_title = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_work_phone = in.readUTF();
                                                    String ba_cell_phone = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_home_phone = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_fax = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_email = in.readUTF();
                                                    String ba_address1 = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_address2 = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_city = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_state = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_country = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_zip = in.readBoolean() ? in.readUTF() : null;
                                                    String ba_username = in.readUTF();
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
                                                    Map<String,String> options = new HashMap<String,String>(numOptions * 4 / 3 + 1);
                                                    for(int c=0;c<numOptions;c++) {
                                                        String name = in.readUTF();
                                                        String value = in.readBoolean() ? in.readUTF() : null;
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
                                                    int pkey=SignupHandler.addSignupRequest(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case SPAM_EMAIL_MESSAGES :
                                                {
                                                    int esr=in.readCompressedInt();
                                                    String message=in.readUTF().trim();
                                                    process.setCommand(
                                                        AOSHCommand.ADD_SPAM_EMAIL_MESSAGE,
                                                        Integer.valueOf(esr),
                                                        message
                                                    );
                                                    int pkey=EmailHandler.addSpamEmailMessage(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        esr,
                                                        message
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case TICKETS :
                                                {
                                                    String brand;
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_46)>=0) {
                                                        brand = in.readUTF();
                                                    } else {
                                                        brand = BusinessHandler.getRootBusiness();
                                                    }
                                                    String accounting;
                                                    if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_126)>=0) {
                                                        accounting=in.readBoolean()?in.readUTF().trim():null;
                                                    } else {
                                                        String packageName=in.readUTF().trim();
                                                        accounting=PackageHandler.getBusinessForPackage(conn, packageName);
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
                                                        details,
                                                        clientPriority,
                                                        contactEmails,
                                                        contactPhoneNumbers
                                                    );
                                                    int pkey=TicketHandler.addTicket(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case TRANSACTIONS :
                                                {
                                                    String accounting=in.readUTF().trim();
                                                    String sourceAccounting=in.readUTF().trim();
                                                    String business_administrator=in.readUTF().trim();
                                                    String type=in.readUTF().trim();
                                                    String description=in.readUTF().trim();
                                                    int quantity=in.readCompressedInt();
                                                    int rate=in.readCompressedInt();
                                                    String paymentType = in.readNullUTF();
                                                    String paymentInfo = in.readNullUTF();
                                                    String processor = in.readNullUTF();
                                                    byte payment_confirmed=in.readByte();
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
                                                    int pkey=TransactionHandler.addTransaction(
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
                                                    resp1=AOServProtocol.DONE;
                                                    resp2Int=pkey;
                                                    hasResp2Int=true;
                                                }
                                                break;
                                            case USERNAMES :
                                                {
                                                    String packageName=in.readUTF().trim();
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
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
                                                Integer.valueOf(owner),
                                                description,
                                                Integer.valueOf(os_version),
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
                                                Integer.valueOf(numBytes)
                                            );
                                            RandomHandler.addMasterEntropy(conn, source, entropy);
                                            resp1=AOServProtocol.DONE;
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
                                            String accounting=in.readUTF().trim();
                                            String cancelReason=in.readBoolean()?in.readUTF().trim():null;
                                            process.setCommand(AOSHCommand.CANCEL_BUSINESS, accounting, cancelReason);
                                            BusinessHandler.cancelBusiness(conn, source, invalidateList, accounting, cancelReason);
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(ticketID),
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
                                            int ticketID=in.readCompressedInt();
                                            String clientPriority = in.readUTF();
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
                                                String username = in.readUTF();
                                                String comments = in.readUTF();
                                            }
                                            process.setCommand(
                                                "change_ticket_client_priority",
                                                Integer.valueOf(ticketID),
                                                clientPriority
                                            );
                                            TicketHandler.changeTicketClientPriority(
                                                conn,
                                                source,
                                                invalidateList,
                                                ticketID,
                                                clientPriority
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_TICKET_SUMMARY :
                                        {
                                            int ticketID = in.readCompressedInt();
                                            String summary = in.readUTF();
                                            process.setCommand(
                                                "set_ticket_summary",
                                                Integer.valueOf(ticketID),
                                                summary
                                            );
                                            TicketHandler.setTicketSummary(
                                                conn,
                                                source,
                                                invalidateList,
                                                ticketID,
                                                summary
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case ADD_TICKET_ANNOTATION :
                                        {
                                            int ticketID = in.readCompressedInt();
                                            String summary = in.readUTF();
                                            String details = in.readNullLongUTF();
                                            process.setCommand(
                                                "add_ticket_annotation",
                                                Integer.valueOf(ticketID),
                                                summary,
                                                details
                                            );
                                            TicketHandler.addTicketAnnotation(
                                                conn,
                                                source,
                                                invalidateList,
                                                ticketID,
                                                summary,
                                                details
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(ticketID),
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
                                            resp1=AOServProtocol.DONE;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) {
                                                resp2Boolean = updated;
                                                hasResp2Boolean = true;
                                            }
                                            sendInvalidateList=true;
                                            break;
                                        }
                                    /*case COMPLETE_TICKET :
                                        {
                                            int ticketID=in.readCompressedInt();
                                            String username=in.readUTF().trim();
                                            String comments=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.COMPLETE_TICKET,
                                                Integer.valueOf(ticketID),
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
                                            int pkey=in.readCompressedInt();
                                            String password=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.COMPARE_LINUX_SERVER_ACCOUNT_PASSWORD,
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            boolean result=LinuxAccountHandler.comparePassword(
                                                conn,
                                                source,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=result;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case COPY_HOME_DIRECTORY :
                                        {
                                            int from_lsa=in.readCompressedInt();
                                            int to_server=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.COPY_HOME_DIRECTORY,
                                                Integer.valueOf(from_lsa),
                                                Integer.valueOf(to_server)
                                            );
                                            long byteCount=LinuxAccountHandler.copyHomeDirectory(
                                                conn,
                                                source,
                                                from_lsa,
                                                to_server
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Long=byteCount;
                                            hasResp2Long=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case COPY_LINUX_SERVER_ACCOUNT_PASSWORD :
                                        {
                                            int from_lsa=in.readCompressedInt();
                                            int to_lsa=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.COPY_LINUX_SERVER_ACCOUNT_PASSWORD,
                                                Integer.valueOf(from_lsa),
                                                Integer.valueOf(to_lsa)
                                            );
                                            LinuxAccountHandler.copyLinuxServerAccountPassword(
                                                conn,
                                                source,
                                                invalidateList,
                                                from_lsa,
                                                to_lsa
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case COPY_PACKAGE_DEFINITION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "copy_package_definition",
                                                Integer.valueOf(pkey)
                                            );
                                            int newPKey=PackageHandler.copyPackageDefinition(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Int=newPKey;
                                            hasResp2Int=true;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case CREDIT_CARD_DECLINED :
                                        {
                                            int transid=in.readCompressedInt();
                                            String reason=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.DECLINE_CREDIT_CARD,
                                                Integer.valueOf(transid),
                                                reason
                                            );
                                            CreditCardHandler.creditCardDeclined(
                                                conn,
                                                source,
                                                invalidateList,
                                                transid,
                                                reason
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case DISABLE :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                            int disableLog=in.readCompressedInt();
                                            Integer dlObj=Integer.valueOf(disableLog);
                                            switch(tableID) {
                                                case BUSINESSES :
                                                    {
                                                        String accounting=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_BUSINESS,
                                                            dlObj,
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
                                                        String username=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_BUSINESS_ADMINISTRATOR,
                                                            dlObj,
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_CVS_REPOSITORY,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_EMAIL_LIST,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_EMAIL_PIPE,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_EMAIL_SMTP_RELAY,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_HTTPD_SHARED_TOMCAT,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_HTTPD_SITE,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_HTTPD_SITE_BIND,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_LINUX_ACCOUNT,
                                                            dlObj,
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_LINUX_SERVER_ACCOUNT,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_MYSQL_SERVER_USER,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_MYSQL_USER,
                                                            dlObj,
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
                                                        String name=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_PACKAGE,
                                                            dlObj,
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_POSTGRES_SERVER_USER,
                                                            dlObj,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_POSTGRES_USER,
                                                            dlObj,
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
                                                        String username=in.readUTF().trim();
                                                        process.setCommand(
                                                            AOSHCommand.DISABLE_USERNAME,
                                                            dlObj,
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case DUMP_MYSQL_DATABASE :
                                        {
                                            process.setPriority(Thread.NORM_PRIORITY-1);
                                            currentThread.setPriority(Thread.NORM_PRIORITY-1);

                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.DUMP_MYSQL_DATABASE,
                                                Integer.valueOf(pkey)
                                            );
                                            MySQLHandler.dumpMySQLDatabase(
                                                conn,
                                                source,
                                                out,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case DUMP_POSTGRES_DATABASE :
                                        {
                                            process.setPriority(Thread.NORM_PRIORITY-1);
                                            currentThread.setPriority(Thread.NORM_PRIORITY-1);

                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.DUMP_POSTGRES_DATABASE,
                                                Integer.valueOf(pkey)
                                            );
                                            PostgresHandler.dumpPostgresDatabase(
                                                conn,
                                                source,
                                                out,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case ENABLE :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                            switch(tableID) {
                                                case BUSINESSES :
                                                    {
                                                        String accounting=in.readUTF().trim();
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
                                                        String username=in.readUTF().trim();
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_CVS_REPOSITORY,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_EMAIL_LIST,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_EMAIL_PIPE,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_EMAIL_SMTP_RELAY,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_HTTPD_SHARED_TOMCAT,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_HTTPD_SITE,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_HTTPD_SITE_BIND,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_LINUX_SERVER_ACCOUNT,
                                                            Integer.valueOf(pkey)
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_MYSQL_SERVER_USER,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
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
                                                        String name=in.readUTF().trim();
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
                                                        int pkey=in.readCompressedInt();
                                                        process.setCommand(
                                                            AOSHCommand.ENABLE_POSTGRES_SERVER_USER,
                                                            Integer.valueOf(pkey)
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
                                                        String username=in.readUTF().trim();
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
                                                        String username=in.readUTF().trim();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case GENERATE_ACCOUNTING_CODE :
                                        {
                                            String template=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_ACCOUNTING,
                                                template
                                            );
                                            String accounting=BusinessHandler.generateAccountingCode(
                                                conn,
                                                template
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=accounting;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GENERATE_MYSQL_DATABASE_NAME :
                                        {
                                            String template_base=in.readUTF().trim();
                                            String template_added=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_MYSQL_DATABASE_NAME,
                                                template_base,
                                                template_added
                                            );
                                            String name=MySQLHandler.generateMySQLDatabaseName(
                                                conn,
                                                template_base,
                                                template_added
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=name;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GENERATE_PACKAGE_NAME :
                                        {
                                            String template=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_PACKAGE_NAME,
                                                template
                                            );
                                            String name=PackageHandler.generatePackageName(
                                                conn,
                                                template
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=name;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GENERATE_POSTGRES_DATABASE_NAME :
                                        {
                                            String template_base=in.readUTF().trim();
                                            String template_added=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_POSTGRES_DATABASE_NAME,
                                                template_base,
                                                template_added
                                            );
                                            String name=PostgresHandler.generatePostgresDatabaseName(
                                                conn,
                                                template_base,
                                                template_added
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=name;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GENERATE_SHARED_TOMCAT_NAME :
                                        {
                                            String template=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_SHARED_TOMCAT_NAME,
                                                template
                                            );
                                            String name=HttpdHandler.generateSharedTomcatName(
                                                conn,
                                                template
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=name;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GENERATE_SITE_NAME :
                                        {
                                            String template=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GENERATE_SITE_NAME,
                                                template
                                            );
                                            String name=HttpdHandler.generateSiteName(
                                                conn,
                                                template
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=name;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_ACCOUNT_BALANCE :
                                        {
                                            String accounting=in.readUTF().trim();
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
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_ACCOUNT_BALANCE_BEFORE :
                                        {
                                            String accounting=in.readUTF().trim();
                                            long before=in.readLong();
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
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_BANK_TRANSACTIONS_ACCOUNT :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            String accounting=in.readUTF().trim();
                                            process.setCommand(
                                                "get_bank_transactions_account",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE,
                                                accounting
                                            );
                                            BankAccountHandler.getBankTransactionsAccount(
                                                conn,
                                                source,
                                                out,
                                                provideProgress,
                                                accounting
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_CONFIRMED_ACCOUNT_BALANCE :
                                        {
                                            String accounting=in.readUTF().trim();
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
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_CONFIRMED_ACCOUNT_BALANCE_BEFORE :
                                        {
                                            String accounting=in.readUTF().trim();
                                            long before=in.readLong();
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
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AUTORESPONDER_CONTENT :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_AUTORESPONDER_CONTENT,
                                                Integer.valueOf(pkey)
                                            );
                                            String content=LinuxAccountHandler.getAutoresponderContent(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=content;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AWSTATS_FILE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String path=in.readUTF();
                                            String queryString=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.GET_AWSTATS_FILE,
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_BACKUP_PARTITION_DISK_TOTAL_SIZE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_BACKUP_PARTITION_TOTAL_SIZE,
                                                Integer.valueOf(pkey)
                                            );
                                            long size=BackupHandler.getBackupPartitionTotalSize(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Long=size;
                                            hasResp2Long=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_BACKUP_PARTITION_DISK_USED_SIZE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_BACKUP_PARTITION_USED_SIZE,
                                                Integer.valueOf(pkey)
                                            );
                                            long size=BackupHandler.getBackupPartitionUsedSize(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Long=size;
                                            hasResp2Long=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_CACHED_ROW_COUNT :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                            process.setCommand(
                                                "get_cached_row_count",
                                                TableHandler.getTableName(
                                                    conn,
                                                    tableID
                                                )
                                            );
                                            int count=TableHandler.getCachedRowCount(
                                                conn,
                                                source,
                                                tableID
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Int=count;
                                            hasResp2Int=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_CRON_TABLE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_CRON_TABLE,
                                                Integer.valueOf(pkey)
                                            );
                                            String cronTable=LinuxAccountHandler.getCronTable(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=cronTable;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_EMAIL_LIST_ADDRESS_LIST :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_EMAIL_LIST,
                                                Integer.valueOf(pkey)
                                            );
                                            String emailList=EmailHandler.getEmailListAddressList(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=emailList;
                                            sendInvalidateList=false;
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_IMAP_FOLDER_SIZES :
                                        {
                                            int pkey=in.readCompressedInt();
                                            int numFolders=in.readCompressedInt();
                                            String[] folderNames=new String[numFolders];
                                            for(int c=0;c<numFolders;c++) folderNames[c]=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.GET_IMAP_FOLDER_SIZES,
                                                Integer.valueOf(pkey),
                                                Integer.valueOf(numFolders),
                                                folderNames
                                            );
                                            resp2LongArray=EmailHandler.getImapFolderSizes(
                                                conn,
                                                source,
                                                pkey,
                                                folderNames
                                            );
                                            hasResp2LongArray=true;
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_INBOX_ATTRIBUTES :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_INBOX_ATTRIBUTES,
                                                Integer.valueOf(pkey)
                                            );
                                            resp2InboxAttributes=EmailHandler.getInboxAttributes(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            hasResp2InboxAttributes=true;
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MAJORDOMO_INFO_FILE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_MAJORDOMO_INFO_FILE,
                                                Integer.valueOf(pkey)
                                            );
                                            String file=EmailHandler.getMajordomoInfoFile(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=file;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MAJORDOMO_INTRO_FILE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.GET_MAJORDOMO_INTRO_FILE,
                                                Integer.valueOf(pkey)
                                            );
                                            String file=EmailHandler.getMajordomoIntroFile(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=file;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MASTER_ENTROPY :
                                        {
                                            int numBytes=in.readCompressedInt();
                                            process.setCommand(
                                                "get_master_entropy",
                                                Integer.valueOf(numBytes)
                                            );
                                            byte[] bytes=RandomHandler.getMasterEntropy(conn, source, numBytes);
                                            out.writeByte(AOServProtocol.DONE);
                                            out.writeCompressedInt(bytes.length);
                                            for(int c=0;c<bytes.length;c++) out.writeByte(bytes[c]);
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MASTER_ENTROPY_NEEDED :
                                        {
                                            process.setCommand(
                                                "get_master_entropy_needed"
                                            );
                                            long needed=RandomHandler.getMasterEntropyNeeded(conn, source);
                                            resp1=AOServProtocol.DONE;
                                            resp2Long=needed;
                                            hasResp2Long=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MRTG_FILE :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            String filename=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.GET_MRTG_FILE,
                                                Integer.valueOf(aoServer),
                                                filename
                                            );
                                            AOServerHandler.getMrtgFile(
                                                conn,
                                                source,
                                                aoServer,
                                                filename,
                                                out
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MYSQL_MASTER_STATUS :
                                        {
                                            int mysqlServer=in.readCompressedInt();
                                            process.setCommand(
                                                "get_mysql_master_status",
                                                Integer.valueOf(mysqlServer)
                                            );
                                            MySQLHandler.getMasterStatus(
                                                conn,
                                                source,
                                                mysqlServer,
                                                out
                                            );
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_MYSQL_SLAVE_STATUS :
                                        {
                                            int failoverMySQLReplication=in.readCompressedInt();
                                            process.setCommand(
                                                "get_mysql_slave_status",
                                                Integer.valueOf(failoverMySQLReplication)
                                            );
                                            MySQLHandler.getSlaveStatus(
                                                conn,
                                                source,
                                                failoverMySQLReplication,
                                                out
                                            );
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_NET_DEVICE_BONDING_REPORT :
                                        {
                                            int pkey = in.readCompressedInt();
                                            process.setCommand(
                                                "get_net_device_bonding_report",
                                                Integer.valueOf(pkey)
                                            );
                                            String report = NetDeviceHandler.getNetDeviceBondingReport(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_NET_DEVICE_STATISTICS_REPORT :
                                        {
                                            int pkey = in.readCompressedInt();
                                            process.setCommand(
                                                "get_net_device_statistics_report",
                                                Integer.valueOf(pkey)
                                            );
                                            String report = NetDeviceHandler.getNetDeviceStatisticsReport(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_3WARE_RAID_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_3ware_raid_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.get3wareRaidReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_MD_RAID_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_md_raid_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getMdRaidReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_DRBD_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_drbd_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getDrbdReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_LVM_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_lvm_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String[] report = AOServerHandler.getLvmReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report[0];
                                            resp3String=report[1];
                                            resp4String=report[2];
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_HDD_TEMP_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_hdd_temp_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getHddTempReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_HDD_MODEL_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_hdd_model_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getHddModelReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_FILESYSTEMS_CSV_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_filesystems_csv_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getFilesystemsCsvReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_LOADAVG_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_loadavg_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getLoadAvgReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_MEMINFO_REPORT :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_meminfo_report",
                                                Integer.valueOf(aoServer)
                                            );
                                            String report = AOServerHandler.getMemInfoReport(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=report;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_AO_SERVER_SYSTEM_TIME_MILLIS :
                                        {
                                            int aoServer = in.readCompressedInt();
                                            process.setCommand(
                                                "get_ao_server_system_time_millis",
                                                Integer.valueOf(aoServer)
                                            );
                                            long systemTime = AOServerHandler.getSystemTimeMillis(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1 = AOServProtocol.DONE;
                                            resp2Long = systemTime;
                                            hasResp2Long = true;
                                            sendInvalidateList = false;
                                        }
                                        break;
                                    case GET_OBJECT :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
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
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_PENDING_PAYMENTS :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            process.setCommand(
                                                "get_pending_payments",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE
                                            );
                                            TransactionHandler.getPendingPayments(
                                                conn,
                                                source,
                                                out,
                                                provideProgress
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_ROOT_BUSINESS :
                                        {
                                            process.setCommand(AOSHCommand.GET_ROOT_BUSINESS);
                                            String bu=BusinessHandler.getRootBusiness();
                                            resp1=AOServProtocol.DONE;
                                            resp2String=bu;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_ROW_COUNT :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            int count;
                                            if(tableID==null) {
                                                logger.warning("Client table not supported: #"+clientTableID+", returning 0 from get_row_count");
                                                count = 0;
                                            } else {
                                                process.setCommand(
                                                    "get_row_count",
                                                    TableHandler.getTableName(
                                                        conn,
                                                        tableID
                                                    )
                                                );
                                                count=TableHandler.getRowCount(
                                                    conn,
                                                    source,
                                                    tableID
                                                );
                                            }
                                            resp1=AOServProtocol.DONE;
                                            resp2Int=count;
                                            hasResp2Int=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_SPAM_EMAIL_MESSAGES_FOR_EMAIL_SMTP_RELAY :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            int esr=in.readCompressedInt();
                                            process.setCommand(
                                                "get_spam_email_messages_for_email_smtp_relay",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE,
                                                Integer.valueOf(esr)
                                            );
                                            EmailHandler.getSpamEmailMessagesForEmailSmtpRelay(
                                                conn,
                                                source,
                                                out,
                                                provideProgress,
                                                esr
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TABLE :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) {
                                                writeObjects(source, out, provideProgress, new ArrayList<AOServObject>());                                                
                                            } else {
                                                if(
                                                    tableID==SchemaTable.TableID.DISTRO_FILES
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TICKET_DETAILS :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_details",
                                                Integer.valueOf(pkey)
                                            );
                                            String details=TicketHandler.getTicketDetails(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = details;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TICKET_RAW_EMAIL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_raw_email",
                                                Integer.valueOf(pkey)
                                            );
                                            String rawEmail=TicketHandler.getTicketRawEmail(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = rawEmail;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList = false;
                                        }
                                        break;
                                    case GET_TICKET_INTERNAL_NOTES :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_internal_notes",
                                                Integer.valueOf(pkey)
                                            );
                                            String internalNotes=TicketHandler.getTicketInternalNotes(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2LongString = internalNotes;
                                            //hasResp2LongString = true;
                                            sendInvalidateList = false;
                                        }
                                        break;
                                    case GET_TICKET_ACTION_OLD_VALUE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_action_old_value",
                                                Integer.valueOf(pkey)
                                            );
                                            String oldValue=TicketHandler.getTicketActionOldValue(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = oldValue;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TICKET_ACTION_NEW_VALUE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_action_new_value",
                                                Integer.valueOf(pkey)
                                            );
                                            String newValue=TicketHandler.getTicketActionNewValue(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = newValue;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TICKET_ACTION_DETAILS :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_action_details",
                                                Integer.valueOf(pkey)
                                            );
                                            String details=TicketHandler.getTicketActionDetails(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = details;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TICKET_ACTION_RAW_EMAIL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_ticket_action_raw_email",
                                                Integer.valueOf(pkey)
                                            );
                                            String rawEmail=TicketHandler.getTicketActionRawEmail(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2NullLongString = rawEmail;
                                            hasResp2NullLongString = true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TRANSACTIONS_BUSINESS :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            String accounting=in.readUTF().trim();
                                            process.setCommand(
                                                "get_transactions_business",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE,
                                                accounting
                                            );
                                            TransactionHandler.getTransactionsBusiness(
                                                conn,
                                                source,
                                                out,
                                                provideProgress,
                                                accounting
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TRANSACTIONS_BUSINESS_ADMINISTRATOR :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            String username=in.readUTF().trim();
                                            process.setCommand(
                                                "get_transactions_business_administrator",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE,
                                                username
                                            );
                                            TransactionHandler.getTransactionsBusinessAdministrator(
                                                conn,
                                                source,
                                                out,
                                                provideProgress,
                                                username
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_TRANSACTIONS_SEARCH :
                                        {
                                            boolean provideProgress=in.readBoolean();
                                            TransactionSearchCriteria criteria=new TransactionSearchCriteria();
                                            criteria.read(in);
                                            process.setCommand(
                                                "get_transactions_search",
                                                provideProgress?Boolean.TRUE:Boolean.FALSE,
                                                "..."
                                            );
                                            TransactionHandler.getTransactionsSearch(
                                                conn,
                                                source,
                                                out,
                                                provideProgress,
                                                criteria
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case GET_WHOIS_HISTORY_WHOIS_OUTPUT :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "get_whois_history_whois_output",
                                                Integer.valueOf(pkey)
                                            );
                                            String whoisOutput=DNSHandler.getWhoisHistoryOutput(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2String=whoisOutput;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    /*case HOLD_TICKET :
                                        {
                                            int ticketID=in.readCompressedInt();
                                            String comments=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.HOLD_TICKET,
                                                Integer.valueOf(ticketID),
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
                                                Integer.valueOf(sitePKey),
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
                                            String accounting=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_ACCOUNTING_AVAILABLE,
                                                accounting
                                            );
                                            boolean isAvailable=BusinessHandler.isAccountingAvailable(
                                                conn,
                                                accounting
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET :
                                        {
                                            String username=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_BUSINESS_ADMINISTRATOR_PASSWORD_SET,
                                                username
                                            );
                                            boolean isAvailable=BusinessHandler.isBusinessAdministratorPasswordSet(
                                                conn,
                                                source,
                                                username
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_DNS_ZONE_AVAILABLE :
                                        {
                                            String zone=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_DNS_ZONE_AVAILABLE,
                                                zone
                                            );
                                            boolean isAvailable=DNSHandler.isDNSZoneAvailable(
                                                conn,
                                                zone
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_EMAIL_DOMAIN_AVAILABLE :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            String domain=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_EMAIL_DOMAIN_AVAILABLE,
                                                Integer.valueOf(aoServer),
                                                domain
                                            );
                                            boolean isAvailable=EmailHandler.isEmailDomainAvailable(
                                                conn,
                                                source,
                                                aoServer,
                                                domain
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_LINUX_GROUP_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_LINUX_GROUP_NAME_AVAILABLE,
                                                name
                                            );
                                            boolean isAvailable=LinuxAccountHandler.isLinuxGroupNameAvailable(
                                                conn,
                                                name
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_LINUX_SERVER_ACCOUNT_PASSWORD_SET,
                                                Integer.valueOf(pkey)
                                            );
                                            boolean isAvailable=LinuxAccountHandler.isLinuxServerAccountPasswordSet(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_LINUX_SERVER_ACCOUNT_PROCMAIL_MANUAL,
                                                Integer.valueOf(pkey)
                                            );
                                            int isManual=LinuxAccountHandler.isLinuxServerAccountProcmailManual(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_6)>=0) {
                                                resp2Int=isManual;
                                                hasResp2Int=true;
                                            } else {
                                                if(isManual==AOServProtocol.FALSE) resp2Boolean=false;
                                                else if(isManual==AOServProtocol.TRUE) resp2Boolean=true;
                                                else throw new IOException("Unsupported value for AOServClient protocol < "+AOServProtocol.Version.VERSION_1_6);
                                                hasResp2Boolean=true;
                                            }
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_MYSQL_DATABASE_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            int mysqlServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_MYSQL_DATABASE_NAME_AVAILABLE,
                                                name,
                                                Integer.valueOf(mysqlServer)
                                            );
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)<0) throw new IOException(AOSHCommand.IS_MYSQL_DATABASE_NAME_AVAILABLE+" call not supported for AOServProtocol < "+AOServProtocol.Version.VERSION_1_4+", please upgrade AOServ Client.");
                                            boolean isAvailable=MySQLHandler.isMySQLDatabaseNameAvailable(
                                                conn,
                                                source,
                                                name,
                                                mysqlServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_MYSQL_SERVER_USER_PASSWORD_SET :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_MYSQL_SERVER_USER_PASSWORD_SET,
                                                Integer.valueOf(pkey)
                                            );
                                            boolean isAvailable=MySQLHandler.isMySQLServerUserPasswordSet(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_PACKAGE_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_PACKAGE_NAME_AVAILABLE,
                                                name
                                            );
                                            boolean isAvailable=PackageHandler.isPackageNameAvailable(
                                                conn,
                                                name
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_POSTGRES_DATABASE_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            int postgresServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_POSTGRES_DATABASE_NAME_AVAILABLE,
                                                name,
                                                Integer.valueOf(postgresServer)
                                            );
                                            boolean isAvailable=PostgresHandler.isPostgresDatabaseNameAvailable(
                                                conn,
                                                source,
                                                name,
                                                postgresServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_POSTGRES_SERVER_USER_PASSWORD_SET :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_POSTGRES_SERVER_USER_PASSWORD_SET,
                                                Integer.valueOf(pkey)
                                            );
                                            boolean isAvailable=PostgresHandler.isPostgresServerUserPasswordSet(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_POSTGRES_SERVER_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.IS_POSTGRES_SERVER_NAME_AVAILABLE,
                                                name,
                                                Integer.valueOf(aoServer)
                                            );
                                            boolean isAvailable=PostgresHandler.isPostgresServerNameAvailable(
                                                conn,
                                                source,
                                                name,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_SHARED_TOMCAT_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_SHARED_TOMCAT_NAME_AVAILABLE,
                                                name
                                            );
                                            boolean isAvailable=HttpdHandler.isSharedTomcatNameAvailable(
                                                conn,
                                                name
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_USERNAME_AVAILABLE :
                                        {
                                            String username=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_USERNAME_AVAILABLE,
                                                username
                                            );
                                            boolean isAvailable=UsernameHandler.isUsernameAvailable(
                                                conn,
                                                username
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case IS_SITE_NAME_AVAILABLE :
                                        {
                                            String name=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.IS_SITE_NAME_AVAILABLE,
                                                name
                                            );
                                            boolean isAvailable=HttpdHandler.isSiteNameAvailable(
                                                conn,
                                                name
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=isAvailable;
                                            hasResp2Boolean=true;
                                            sendInvalidateList=false;
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
                                            int ipAddress=in.readCompressedInt();
                                            int toServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.MOVE_IP_ADDRESS,
                                                Integer.valueOf(ipAddress),
                                                Integer.valueOf(toServer)
                                            );
                                            IPAddressHandler.moveIPAddress(
                                                conn,
                                                source,
                                                invalidateList,
                                                ipAddress,
                                                toServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    /*case REACTIVATE_TICKET :
                                        {
                                            int ticketID=in.readCompressedInt();
                                            String username=in.readUTF().trim();
                                            String comments=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.REACTIVATE_TICKET,
                                                Integer.valueOf(ticketID),
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
                                            process.setPriority(Thread.NORM_PRIORITY+1);
                                            currentThread.setPriority(Thread.NORM_PRIORITY+1);

                                            int pkey=in.readCompressedInt();
                                            long min_duration=in.readLong();
                                            process.setCommand(
                                                AOSHCommand.REFRESH_EMAIL_SMTP_RELAY,
                                                Integer.valueOf(pkey),
                                                Long.valueOf(min_duration)
                                            );
                                            EmailHandler.refreshEmailSmtpRelay(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                min_duration
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case REMOVE : {
                                        int clientTableID=in.readCompressedInt();
                                        SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                        if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                        switch(tableID) {
                                            case BLACKHOLE_EMAIL_ADDRESSES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_BLACKHOLE_EMAIL_ADDRESS,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeBlackholeEmailAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case BUSINESS_ADMINISTRATORS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case BUSINESS_SERVERS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_BUSINESS_SERVER,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    BusinessHandler.removeBusinessServer(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case CREDIT_CARDS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_CREDIT_CARD,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    CreditCardHandler.removeCreditCard(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case CVS_REPOSITORIES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_CVS_REPOSITORY,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    CvsHandler.removeCvsRepository(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case DNS_RECORDS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_DNS_RECORD,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    DNSHandler.removeDNSRecord(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case DNS_ZONES :
                                                {
                                                    String zone=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_ADDRESSES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_ADDRESS,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_DOMAINS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_DOMAIN,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailDomain(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_FORWARDING :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_FORWARDING,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailForwarding(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_LIST_ADDRESSES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_LIST_ADDRESS,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailListAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_LISTS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_LIST,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailList(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_PIPE_ADDRESSES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_PIPE_ADDRESS,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailPipeAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_PIPES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_PIPE,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailPipe(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case EMAIL_SMTP_RELAYS :
                                                {
                                                    process.setPriority(Thread.NORM_PRIORITY+1);
                                                    currentThread.setPriority(Thread.NORM_PRIORITY+1);

                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_EMAIL_SMTP_RELAY,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeEmailSmtpRelay(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case FILE_BACKUP_SETTINGS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_FILE_BACKUP_SETTING,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    BackupHandler.removeFileBackupSetting(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case FTP_GUEST_USERS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_SHARED_TOMCATS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_SHARED_TOMCAT,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdSharedTomcat(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_SITE_AUTHENTICATED_LOCATIONS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        "remove_httpd_site_authenticated_location",
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdSiteAuthenticatedLocation(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_SITES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_SITE,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdSite(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_SITE_URLS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_SITE_URL,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdSiteURL(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_TOMCAT_CONTEXTS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_TOMCAT_CONTEXT,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdTomcatContext(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_TOMCAT_DATA_SOURCES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_TOMCAT_DATA_SOURCE,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdTomcatDataSource(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case HTTPD_TOMCAT_PARAMETERS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_HTTPD_TOMCAT_PARAMETER,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    HttpdHandler.removeHttpdTomcatParameter(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_ACC_ADDRESSES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_LINUX_ACC_ADDRESS,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    EmailHandler.removeLinuxAccAddress(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_ACCOUNTS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_GROUP_ACCOUNTS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_LINUX_GROUP_ACCOUNT,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    LinuxAccountHandler.removeLinuxGroupAccount(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_GROUPS :
                                                {
                                                    String name=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_SERVER_ACCOUNTS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_LINUX_SERVER_ACCOUNT,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    LinuxAccountHandler.removeLinuxServerAccount(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case LINUX_SERVER_GROUPS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_LINUX_SERVER_GROUP,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    LinuxAccountHandler.removeLinuxServerGroup(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MAJORDOMO_SERVERS :
                                                {
                                                    int domain=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_MAJORDOMO_SERVER,
                                                        Integer.valueOf(domain)
                                                    );
                                                    EmailHandler.removeMajordomoServer(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        domain
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MYSQL_DATABASES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_MYSQL_DATABASE,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    MySQLHandler.removeMySQLDatabase(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MYSQL_DB_USERS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_MYSQL_DB_USER,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    MySQLHandler.removeMySQLDBUser(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MYSQL_SERVER_USERS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_MYSQL_SERVER_USER,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    MySQLHandler.removeMySQLServerUser(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case MYSQL_USERS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case NET_BINDS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_NET_BIND,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    NetBindHandler.removeNetBind(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case PACKAGE_DEFINITIONS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        "remove_package_definition",
                                                        Integer.valueOf(pkey)
                                                    );
                                                    PackageHandler.removePackageDefinition(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case POSTGRES_DATABASES :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_POSTGRES_DATABASE,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    PostgresHandler.removePostgresDatabase(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case POSTGRES_SERVER_USERS :
                                                {
                                                    int pkey=in.readCompressedInt();
                                                    process.setCommand(
                                                        AOSHCommand.REMOVE_POSTGRES_SERVER_USER,
                                                        Integer.valueOf(pkey)
                                                    );
                                                    PostgresHandler.removePostgresServerUser(
                                                        conn,
                                                        source,
                                                        invalidateList,
                                                        pkey
                                                    );
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case POSTGRES_USERS :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            case USERNAMES :
                                                {
                                                    String username=in.readUTF().trim();
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
                                                    resp1=AOServProtocol.DONE;
                                                }
                                                break;
                                            default :
                                                throw new IOException("Unknown table ID for remove: clientTableID="+clientTableID+", tableID="+tableID);
                                        }
                                        sendInvalidateList=true;
                                        break;
                                    }
                                    case REQUEST_DAEMON_ACCESS :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            int daemonCommandCode=in.readCompressedInt();
                                            int param1=in.readCompressedInt();
                                            process.setCommand(
                                                "request_daemon_access",
                                                Integer.valueOf(aoServer),
                                                Integer.valueOf(daemonCommandCode),
                                                Integer.valueOf(param1)
                                            );
                                            long key=DaemonHandler.requestDaemonAccess(
                                                conn,
                                                source,
                                                aoServer,
                                                daemonCommandCode,
                                                param1
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Long=key;
                                            hasResp2Long=true;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_APACHE :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.RESTART_APACHE,
                                                Integer.valueOf(aoServer)
                                            );
                                            HttpdHandler.restartApache(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_CRON :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.RESTART_CRON,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.restartCron(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_MYSQL :
                                        {
                                            int mysqlServer=in.readCompressedInt();
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_4)<0) throw new IOException("addBackupServer call not supported for AOServ Client version < "+AOServProtocol.Version.VERSION_1_4+", please upgrade AOServ Client.");
                                            process.setCommand(
                                                AOSHCommand.RESTART_MYSQL,
                                                Integer.valueOf(mysqlServer)
                                            );
                                            MySQLHandler.restartMySQL(
                                                conn,
                                                source,
                                                mysqlServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_POSTGRESQL :
                                        {
                                            int postgresServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.RESTART_POSTGRESQL,
                                                Integer.valueOf(postgresServer)
                                            );
                                            PostgresHandler.restartPostgreSQL(
                                                conn,
                                                source,
                                                postgresServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_XFS :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.RESTART_XFS,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.restartXfs(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case RESTART_XVFB :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.RESTART_XVFB,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.restartXvfb(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_AUTORESPONDER :
                                        {
                                            int pkey=in.readCompressedInt();
                                            int from=in.readCompressedInt();
                                            String subject=in.readBoolean()?in.readUTF().trim():null;
                                            String content=in.readBoolean()?in.readUTF():null;
                                            boolean enabled=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_AUTORESPONDER,
                                                Integer.valueOf(pkey),
                                                from==-1?null:Integer.valueOf(from),
                                                subject,
                                                content,
                                                enabled?Boolean.TRUE:Boolean.FALSE
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_BUSINESS_ACCOUNTING :
                                        {
                                            String oldAccounting=in.readUTF().trim();
                                            String newAccounting=in.readUTF().trim();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_BUSINESS_ADMINISTRATOR_PASSWORD :
                                        {
                                            String username=in.readUTF().trim();
                                            String password=in.readUTF();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_BUSINESS_ADMINISTRATOR_PROFILE :
                                        {
                                            String username=in.readUTF().trim();
                                            String name=in.readUTF().trim();
                                            String title=in.readBoolean()?in.readUTF().trim():null;
                                            long birthday=in.readLong();
                                            boolean isPrivate=in.readBoolean();
                                            String workPhone=in.readUTF().trim();
                                            String homePhone=in.readBoolean()?in.readUTF().trim():null;
                                            String cellPhone=in.readBoolean()?in.readUTF().trim():null;
                                            String fax=in.readBoolean()?in.readUTF().trim():null;
                                            String email=in.readUTF().trim();
                                            String address1=in.readBoolean()?in.readUTF().trim():null;
                                            String address2=in.readBoolean()?in.readUTF().trim():null;
                                            String city=in.readBoolean()?in.readUTF().trim():null;
                                            String state=in.readBoolean()?in.readUTF().trim():null;
                                            String country=in.readBoolean()?in.readUTF().trim():null;
                                            String zip=in.readBoolean()?in.readUTF().trim():null;
                                            process.setCommand(
                                                AOSHCommand.SET_BUSINESS_ADMINISTRATOR_PROFILE,
                                                username,
                                                name,
                                                title,
                                                birthday==BusinessAdministrator.NONE?null:new java.util.Date(birthday),
                                                isPrivate?Boolean.TRUE:Boolean.FALSE,
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_CRON_TABLE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String crontab=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_CRON_TABLE,
                                                Integer.valueOf(pkey),
                                                crontab
                                            );
                                            LinuxAccountHandler.setCronTable(
                                                conn,
                                                source,
                                                pkey,
                                                crontab
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_CVS_REPOSITORY_MODE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            long mode=in.readLong();
                                            process.setCommand(
                                                AOSHCommand.SET_CVS_REPOSITORY_MODE,
                                                Integer.valueOf(pkey),
                                                Long.toOctalString(mode)
                                            );
                                            CvsHandler.setMode(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                mode
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_DEFAULT_BUSINESS_SERVER :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_DEFAULT_BUSINESS_SERVER,
                                                Integer.valueOf(pkey)
                                            );
                                            BusinessHandler.setDefaultBusinessServer(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_DNS_ZONE_TTL :
                                        {
                                            String zone=in.readUTF();
                                            int ttl=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_DNS_ZONE_TTL,
                                                zone,
                                                Integer.valueOf(ttl)
                                            );
                                            DNSHandler.setDNSZoneTTL(
                                                conn,
                                                source,
                                                invalidateList,
                                                zone,
                                                ttl
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_EMAIL_LIST_ADDRESS_LIST :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String list=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_EMAIL_LIST,
                                                Integer.valueOf(pkey),
                                                list
                                            );
                                            EmailHandler.setEmailListAddressList(
                                                conn,
                                                source,
                                                pkey,
                                                list
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_FILE_BACKUP_SETTINGS :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String path=in.readUTF();
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
                                            process.setCommand(
                                                AOSHCommand.SET_FILE_BACKUP_SETTING,
                                                Integer.valueOf(pkey),
                                                path,
                                                backupEnabled?Boolean.TRUE:Boolean.FALSE
                                            );
                                            BackupHandler.setFileBackupSettings(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                path,
                                                backupEnabled
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_FILE_BACKUP_SETTINGS_ALL_AT_ONCE :
                                        {
                                            int replication = in.readCompressedInt();
                                            int size = in.readCompressedInt();
                                            List<String> paths = new ArrayList<String>(size);
                                            List<Boolean> backupEnableds = new ArrayList<Boolean>(size);
                                            for(int c=0;c<size;c++) {
                                                paths.add(in.readUTF());
                                                backupEnableds.add(in.readBoolean());
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
                                                backupEnableds
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SHARED_TOMCAT_IS_MANUAL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean is_manual=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_SHARED_TOMCAT_IS_MANUAL,
                                                Integer.valueOf(pkey),
                                                is_manual?Boolean.TRUE:Boolean.FALSE
                                            );
                                            HttpdHandler.setHttpdSharedTomcatIsManual(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                is_manual
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_AUTHENTICATED_LOCATION_ATTRIBUTES :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String path=in.readUTF().trim();
                                            boolean isRegularExpression=in.readBoolean();
                                            String authName=in.readUTF().trim();
                                            String authGroupFile=in.readUTF().trim();
                                            String authUserFile=in.readUTF().trim();
                                            String require=in.readUTF().trim();
                                            process.setCommand(
                                                "set_httpd_site_authenticated_location_attributes",
                                                Integer.valueOf(pkey),
                                                path,
                                                isRegularExpression?Boolean.TRUE:Boolean.FALSE,
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_BIND_IS_MANUAL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean is_manual=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_SITE_BIND_IS_MANUAL,
                                                Integer.valueOf(pkey),
                                                is_manual?Boolean.TRUE:Boolean.FALSE
                                            );
                                            HttpdHandler.setHttpdSiteBindIsManual(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                is_manual
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean redirect_to_primary_hostname=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_SITE_BIND_REDIRECT_TO_PRIMARY_HOSTNAME,
                                                Integer.valueOf(pkey),
                                                redirect_to_primary_hostname?Boolean.TRUE:Boolean.FALSE
                                            );
                                            HttpdHandler.setHttpdSiteBindRedirectToPrimaryHostname(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                redirect_to_primary_hostname
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_IS_MANUAL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean is_manual=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_SITE_IS_MANUAL,
                                                Integer.valueOf(pkey),
                                                is_manual?Boolean.TRUE:Boolean.FALSE
                                            );
                                            HttpdHandler.setHttpdSiteIsManual(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                is_manual
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_SERVER_ADMIN :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String emailAddress=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_SITE_SERVER_ADMIN,
                                                Integer.valueOf(pkey),
                                                emailAddress
                                            );
                                            HttpdHandler.setHttpdSiteServerAdmin(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                emailAddress
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_SITE_BIND_PREDISABLE_CONFIG :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String config=in.readBoolean()?in.readUTF().trim():null;
                                            process.setCommand(
                                                "set_httpd_site_bind_predisable_config",
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            HttpdHandler.setHttpdSiteBindPredisableConfig(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                config
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String className=in.readBoolean()?in.readUTF().trim():null;
                                            boolean cookies=in.readBoolean();
                                            boolean crossContext=in.readBoolean();
                                            String docBase=in.readUTF().trim();
                                            boolean override=in.readBoolean();
                                            String path=in.readUTF().trim();
                                            boolean privileged=in.readBoolean();
                                            boolean reloadable=in.readBoolean();
                                            boolean useNaming=in.readBoolean();
                                            String wrapperClass=in.readBoolean()?in.readUTF().trim():null;
                                            int debug=in.readCompressedInt();
                                            String workDir=in.readBoolean()?in.readUTF().trim():null;
                                            process.setCommand(
                                                AOSHCommand.SET_HTTPD_TOMCAT_CONTEXT_ATTRIBUTES,
                                                Integer.valueOf(pkey),
                                                className,
                                                cookies?Boolean.TRUE:Boolean.FALSE,
                                                crossContext?Boolean.TRUE:Boolean.FALSE,
                                                docBase,
                                                override?Boolean.TRUE:Boolean.FALSE,
                                                path,
                                                privileged?Boolean.TRUE:Boolean.FALSE,
                                                reloadable?Boolean.TRUE:Boolean.FALSE,
                                                useNaming?Boolean.TRUE:Boolean.FALSE,
                                                wrapperClass,
                                                Integer.valueOf(debug),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_IMAP_FOLDER_SUBSCRIBED :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String folderName=in.readUTF();
                                            boolean subscribed=in.readBoolean();
                                            process.setCommand(
                                                "set_imap_folder_subscribed",
                                                Integer.valueOf(pkey),
                                                folderName,
                                                subscribed?"true":"false"
                                            );
                                            EmailHandler.setImapFolderSubscribed(
                                                conn,
                                                source,
                                                pkey,
                                                folderName,
                                                subscribed
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_IP_ADDRESS_DHCP_ADDRESS :
                                        {
                                            int ipAddress=in.readCompressedInt();
                                            String dhcpAddress=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.SET_IP_ADDRESS_DHCP_ADDRESS,
                                                Integer.valueOf(ipAddress),
                                                dhcpAddress
                                            );
                                            IPAddressHandler.setIPAddressDHCPAddress(
                                                conn,
                                                source,
                                                invalidateList,
                                                ipAddress,
                                                dhcpAddress
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_IP_ADDRESS_HOSTNAME :
                                        {
                                            int ipAddress=in.readCompressedInt();
                                            String hostname=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.SET_IP_ADDRESS_HOSTNAME,
                                                Integer.valueOf(ipAddress),
                                                hostname
                                            );
                                            IPAddressHandler.setIPAddressHostname(
                                                conn,
                                                source,
                                                invalidateList,
                                                ipAddress,
                                                hostname
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_IP_ADDRESS_PACKAGE :
                                        {
                                            int ipAddress=in.readCompressedInt();
                                            String packageName=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.SET_IP_ADDRESS_PACKAGE,
                                                Integer.valueOf(ipAddress),
                                                packageName
                                            );
                                            IPAddressHandler.setIPAddressPackage(
                                                conn,
                                                source,
                                                invalidateList,
                                                ipAddress,
                                                packageName
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LAST_DISTRO_TIME :
                                        {
                                            process.setPriority(Thread.MIN_PRIORITY+1);
                                            currentThread.setPriority(Thread.MIN_PRIORITY+1);

                                            int ao_server=in.readCompressedInt();
                                            long time=in.readLong();
                                            process.setCommand(
                                                "set_last_distro_time",
                                                Integer.valueOf(ao_server),
                                                new java.util.Date(time)
                                            );
                                            AOServerHandler.setLastDistroTime(
                                                conn,
                                                source,
                                                invalidateList,
                                                ao_server,
                                                time
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_ACCOUNT_HOME_PHONE :
                                        {
                                            String username=in.readUTF().trim();
                                            String phone=in.readUTF().trim();
                                            if(phone.length()==0) phone=null;
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_ACCOUNT_NAME :
                                        {
                                            String username=in.readUTF().trim();
                                            String fullName=in.readUTF().trim();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_ACCOUNT_OFFICE_LOCATION :
                                        {
                                            String username=in.readUTF().trim();
                                            String location=in.readUTF().trim();
                                            if(location.length()==0) location=null;
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_ACCOUNT_OFFICE_PHONE :
                                        {
                                            String username=in.readUTF().trim();
                                            String phone=in.readUTF().trim();
                                            if(phone.length()==0) phone=null;
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_ACCOUNT_SHELL :
                                        {
                                            String username=in.readUTF().trim();
                                            String shell=in.readUTF().trim();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            int days=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_SERVER_ACCOUNT_JUNK_EMAIL_RETENTION,
                                                Integer.valueOf(pkey),
                                                Integer.valueOf(days)
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountJunkEmailRetention(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                days
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_ACCOUNT_PASSWORD,
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountPassword(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_PREDISABLE_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readBoolean()?in.readUTF():null;
                                            process.setCommand(
                                                "set_linux_server_account_predisable_password",
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountPredisablePassword(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_EMAIL_SPAMASSASSIN_INTEGRATION_MODE:
                                        {
                                            int pkey=in.readCompressedInt();
                                            String mode=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_INTEGRATION_MODE,
                                                Integer.valueOf(pkey),
                                                mode
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountSpamAssassinIntegrationMode(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                mode
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE:
                                        {
                                            int pkey=in.readCompressedInt();
                                            float required_score=in.readFloat();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_REQUIRED_SCORE,
                                                Integer.valueOf(pkey),
                                                Float.valueOf(required_score)
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountSpamAssassinRequiredScore(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                required_score
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_SPAMASSASSIN_DISCARD_SCORE:
                                        {
                                            int pkey=in.readCompressedInt();
                                            int discard_score = in.readCompressedInt();
                                            process.setCommand(
                                                "set_linux_server_account_spamassassin_discard_score",
                                                Integer.valueOf(pkey),
                                                discard_score==-1 ? "\"\"" : Integer.toString(discard_score)
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountSpamAssassinDiscardScore(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                discard_score
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            int days=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_SERVER_ACCOUNT_TRASH_EMAIL_RETENTION,
                                                Integer.valueOf(pkey),
                                                Integer.valueOf(days)
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountTrashEmailRetention(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                days
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_LINUX_SERVER_ACCOUNT_USE_INBOX :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean useInbox=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_LINUX_SERVER_ACCOUNT_USE_INBOX,
                                                Integer.valueOf(pkey),
                                                useInbox?Boolean.TRUE:Boolean.FALSE
                                            );
                                            LinuxAccountHandler.setLinuxServerAccountUseInbox(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                useInbox
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_MAJORDOMO_INFO_FILE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String file=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_MAJORDOMO_INFO_FILE,
                                                Integer.valueOf(pkey),
                                                file
                                            );
                                            EmailHandler.setMajordomoInfoFile(
                                                conn,
                                                source,
                                                pkey,
                                                file
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_MAJORDOMO_INTRO_FILE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String file=in.readUTF();
                                            process.setCommand(
                                                AOSHCommand.SET_MAJORDOMO_INTRO_FILE,
                                                Integer.valueOf(pkey),
                                                file
                                            );
                                            EmailHandler.setMajordomoIntroFile(
                                                conn,
                                                source,
                                                pkey,
                                                file
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_MYSQL_SERVER_USER_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readBoolean()?in.readUTF():null;
                                            process.setCommand(
                                                AOSHCommand.SET_MYSQL_SERVER_USER_PASSWORD,
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            MySQLHandler.setMySQLServerUserPassword(
                                                conn,
                                                source,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_MYSQL_SERVER_USER_PREDISABLE_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readBoolean()?in.readUTF():null;
                                            process.setCommand(
                                                "set_mysql_server_user_predisable_password",
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            MySQLHandler.setMySQLServerUserPredisablePassword(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_NET_BIND_MONITORING :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean enabled=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_NET_BIND_MONITORING_ENABLED,
                                                Integer.valueOf(pkey),
                                                enabled?Boolean.TRUE:Boolean.FALSE
                                            );
                                            NetBindHandler.setNetBindMonitoringEnabled(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                enabled
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_NET_BIND_OPEN_FIREWALL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean open_firewall=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.SET_NET_BIND_OPEN_FIREWALL,
                                                Integer.valueOf(pkey),
                                                open_firewall?Boolean.TRUE:Boolean.FALSE
                                            );
                                            NetBindHandler.setNetBindOpenFirewall(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                open_firewall
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_PACKAGE_DEFINITION_ACTIVE :
                                        {
                                            int pkey=in.readCompressedInt();
                                            boolean is_active=in.readBoolean();
                                            process.setCommand(
                                                "set_package_definition_active",
                                                Integer.valueOf(pkey),
                                                is_active?Boolean.TRUE:Boolean.FALSE
                                            );
                                            PackageHandler.setPackageDefinitionActive(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                is_active
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_PACKAGE_DEFINITION_LIMITS :
                                        {
                                            int pkey=in.readCompressedInt();
                                            int count=in.readCompressedInt();
                                            String[] resources=new String[count];
                                            int[] soft_limits=new int[count];
                                            int[] hard_limits=new int[count];
                                            int[] additional_rates=new int[count];
                                            String[] additional_transaction_types=new String[count];
                                            for(int c=0;c<count;c++) {
                                                resources[c]=in.readUTF().trim();
                                                soft_limits[c]=in.readCompressedInt();
                                                hard_limits[c]=in.readCompressedInt();
                                                additional_rates[c]=in.readCompressedInt();
                                                additional_transaction_types[c]=in.readBoolean()?in.readUTF().trim():null;
                                            }
                                            process.setCommand(
                                                "set_package_definition_limits",
                                                Integer.valueOf(pkey),
                                                Integer.valueOf(count),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_POSTGRES_SERVER_USER_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readBoolean()?in.readUTF():null;
                                            process.setCommand(
                                                AOSHCommand.SET_POSTGRES_SERVER_USER_PASSWORD,
                                                Integer.valueOf(pkey),
                                                password
                                            );
                                            PostgresHandler.setPostgresServerUserPassword(
                                                conn,
                                                source,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case SET_POSTGRES_SERVER_USER_PREDISABLE_PASSWORD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String password=in.readBoolean()?in.readUTF():null;
                                            process.setCommand(
                                                "set_postgres_server_user_predisable_password",
                                                Integer.valueOf(pkey),
                                                AOServProtocol.FILTERED
                                            );
                                            PostgresHandler.setPostgresServerUserPredisablePassword(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                password
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_PRIMARY_HTTPD_SITE_URL :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_PRIMARY_HTTPD_SITE_URL,
                                                Integer.valueOf(pkey)
                                            );
                                            HttpdHandler.setPrimaryHttpdSiteURL(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_PRIMARY_LINUX_GROUP_ACCOUNT :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.SET_PRIMARY_LINUX_GROUP_ACCOUNT,
                                                Integer.valueOf(pkey)
                                            );
                                            LinuxAccountHandler.setPrimaryLinuxGroupAccount(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(ticketID),
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
                                                Integer.valueOf(ticketID),
                                                contactEmails
                                            );
                                            TicketHandler.setTicketContactEmails(
                                                conn,
                                                source,
                                                invalidateList,
                                                ticketID,
                                                contactEmails
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_TICKET_CONTACT_PHONE_NUMBERS :
                                        {
                                            int ticketID=in.readCompressedInt(); 
                                            String contactPhoneNumbers=in.readUTF();
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
                                                String username=in.readUTF();
                                                String comments=in.readUTF();
                                            }
                                            process.setCommand(
                                                "set_ticket_contact_phone_numbers",
                                                Integer.valueOf(ticketID),
                                                contactPhoneNumbers
                                            );
                                            TicketHandler.setTicketContactPhoneNumbers(
                                                conn,
                                                source,
                                                invalidateList,
                                                ticketID,
                                                contactPhoneNumbers
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_TICKET_BUSINESS :
                                        {
                                            int ticketID = in.readCompressedInt();
                                            String oldAccounting;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) {
                                                // Added old accounting to behave like atomic variable
                                                oldAccounting = in.readUTF();
                                                if(oldAccounting.length()==0) oldAccounting = null;
                                            } else {
                                                oldAccounting = null;
                                            }
                                            String newAccounting = in.readUTF();
                                            if(newAccounting.length()==0) newAccounting = null;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_43)<=0) {
                                                String username = in.readUTF();
                                                String comments = in.readUTF();
                                            }
                                            process.setCommand(
                                                "set_ticket_business",
                                                Integer.valueOf(ticketID),
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
                                            resp1=AOServProtocol.DONE;
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_48)>=0) {
                                                // Added boolean updated response
                                                resp2Boolean = updated;
                                                hasResp2Boolean = true;
                                            }
                                            sendInvalidateList=true;
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
                                                Integer.valueOf(ticketID),
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
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean = updated;
                                            hasResp2Boolean = true;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_TICKET_INTERNAL_NOTES :
                                        {
                                            int ticketID = in.readCompressedInt();
                                            String oldInternalNotes = in.readLongUTF();
                                            String newInternalNotes = in.readLongUTF();
                                            process.setCommand(
                                                "set_ticket_internal_notes",
                                                Integer.valueOf(ticketID),
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
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean = updated;
                                            hasResp2Boolean = true;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case START_APACHE :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_APACHE,
                                                Integer.valueOf(aoServer)
                                            );
                                            HttpdHandler.startApache(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_CRON :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_CRON,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.startCron(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_DISTRO :
                                        {
                                            process.setPriority(Thread.MIN_PRIORITY+1);
                                            currentThread.setPriority(Thread.MIN_PRIORITY+1);

                                            int ao_server=in.readCompressedInt();
                                            boolean includeUser=in.readBoolean();
                                            process.setCommand(
                                                AOSHCommand.START_DISTRO,
                                                Integer.valueOf(ao_server),
                                                includeUser?Boolean.TRUE:Boolean.FALSE
                                            );
                                            AOServerHandler.startDistro(
                                                conn,
                                                source,
                                                ao_server,
                                                includeUser
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_JVM :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_JVM,
                                                Integer.valueOf(pkey)
                                            );
                                            String message=HttpdHandler.startJVM(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=message!=null;
                                            hasResp2Boolean=true;
                                            if(message!=null) resp3String=message;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_MYSQL :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_MYSQL,
                                                Integer.valueOf(aoServer)
                                            );
                                            MySQLHandler.startMySQL(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_POSTGRESQL :
                                        {
                                            int postgresServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_POSTGRESQL,
                                                Integer.valueOf(postgresServer)
                                            );
                                            PostgresHandler.startPostgreSQL(
                                                conn,
                                                source,
                                                postgresServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_XFS :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_XFS,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.startXfs(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case START_XVFB :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.START_XVFB,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.startXvfb(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_APACHE :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_APACHE,
                                                Integer.valueOf(aoServer)
                                            );
                                            HttpdHandler.stopApache(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_CRON :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_CRON,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.stopCron(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_JVM :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_JVM,
                                                Integer.valueOf(pkey)
                                            );
                                            String message=HttpdHandler.stopJVM(
                                                conn,
                                                source,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            resp2Boolean=message!=null;
                                            hasResp2Boolean=true;
                                            if(message!=null) resp3String=message;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_MYSQL :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_MYSQL,
                                                Integer.valueOf(aoServer)
                                            );
                                            MySQLHandler.stopMySQL(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_POSTGRESQL :
                                        {
                                            int postgresServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_POSTGRESQL,
                                                Integer.valueOf(postgresServer)
                                            );
                                            PostgresHandler.stopPostgreSQL(
                                                conn,
                                                source,
                                                postgresServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_XFS :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_XFS,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.stopXfs(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    case STOP_XVFB :
                                        {
                                            int aoServer=in.readCompressedInt();
                                            process.setCommand(
                                                AOSHCommand.STOP_XVFB,
                                                Integer.valueOf(aoServer)
                                            );
                                            AOServerHandler.stopXvfb(
                                                conn,
                                                source,
                                                aoServer
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                        }
                                        break;
                                    /*case TICKET_WORK :
                                        {
                                            int ticketID=in.readCompressedInt();
                                            String username=in.readUTF().trim();
                                            String comments=in.readUTF().trim();
                                            process.setCommand(
                                                AOSHCommand.ADD_TICKET_WORK,
                                                Integer.valueOf(ticketID),
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
                                            int transid=in.readCompressedInt();
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_28)<=0) {
                                                String paymentType=in.readUTF();
                                                String paymentInfo=in.readBoolean()?in.readUTF():null;
                                                String merchant=in.readBoolean()?in.readUTF():null;
                                                String apr_num;
                                                if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_0_A_128)<0) apr_num=Integer.toString(in.readCompressedInt());
                                                else apr_num=in.readUTF();
                                                throw new SQLException("approve_transaction for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
                                            }
                                            int creditCardTransaction = in.readCompressedInt();
                                            process.setCommand(
                                                "approve_transaction",
                                                Integer.valueOf(transid),
                                                Integer.valueOf(creditCardTransaction)
                                            );
                                            TransactionHandler.transactionApproved(
                                                conn,
                                                source,
                                                invalidateList,
                                                transid,
                                                creditCardTransaction
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case TRANSACTION_DECLINED :
                                        {
                                            int transid=in.readCompressedInt();
                                            if(source.getProtocolVersion().compareTo(AOServProtocol.Version.VERSION_1_28)<=0) {
                                                String paymentType=in.readUTF().trim();
                                                String paymentInfo=in.readBoolean()?in.readUTF().trim():null;
                                                String merchant=in.readBoolean()?in.readUTF().trim():null;
                                                throw new SQLException("decline_transaction for protocol version "+AOServProtocol.Version.VERSION_1_28+" or older is no longer supported.");
                                            }
                                            int creditCardTransaction = in.readCompressedInt();
                                            process.setCommand(
                                                "decline_transaction",
                                                Integer.valueOf(transid),
                                                Integer.valueOf(creditCardTransaction)
                                            );
                                            TransactionHandler.transactionDeclined(
                                                conn,
                                                source,
                                                invalidateList,
                                                transid,
                                                creditCardTransaction
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case TRANSACTION_HELD :
                                        {
                                            int transid=in.readCompressedInt();
                                            int creditCardTransaction = in.readCompressedInt();
                                            process.setCommand(
                                                "hold_transaction",
                                                Integer.valueOf(transid),
                                                Integer.valueOf(creditCardTransaction)
                                            );
                                            TransactionHandler.transactionHeld(
                                                conn,
                                                source,
                                                invalidateList,
                                                transid,
                                                creditCardTransaction
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case REACTIVATE_CREDIT_CARD :
                                        {
                                            int pkey=in.readCompressedInt();
                                            process.setCommand(
                                                "reactivate_credit_card",
                                                Integer.valueOf(pkey)
                                            );
                                            CreditCardHandler.reactivateCreditCard(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_CREDIT_CARD_USE_MONTHLY :
                                        {
                                            String accounting = in.readUTF();
                                            int pkey=in.readCompressedInt();

                                            process.setCommand(
                                                "set_credit_card_use_monthly",
                                                accounting,
                                                Integer.valueOf(pkey)
                                            );
                                            CreditCardHandler.setCreditCardUseMonthly(
                                                conn,
                                                source,
                                                invalidateList,
                                                accounting,
                                                pkey
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_FAILOVER_FILE_REPLICATION_BIT_RATE :
                                        {
                                            int pkey = in.readCompressedInt();
                                            int bitRate = in.readCompressedInt();

                                            process.setCommand(
                                                "set_failover_file_replication_bit_rate",
                                                pkey,
                                                bitRate==BitRateProvider.UNLIMITED_BANDWIDTH ? "unlimited" : Integer.toString(bitRate)
                                            );
                                            FailoverHandler.setFailoverFileReplicationBitRate(
                                                conn,
                                                source,
                                                invalidateList,
                                                pkey,
                                                bitRate
                                            );
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case SET_FAILOVER_FILE_SCHEDULES :
                                        {
                                            int replication = in.readCompressedInt();
                                            int size = in.readCompressedInt();
                                            List<Short> hours = new ArrayList<Short>(size);
                                            List<Short> minutes = new ArrayList<Short>(size);
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                     case UPDATE_CREDIT_CARD :
                                        {
                                            int pkey=in.readCompressedInt();
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
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case UPDATE_CREDIT_CARD_NUMBER_AND_EXPIRATION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String maskedCardNumber = in.readUTF();
                                            String encryptedCardNumber = in.readNullUTF();
                                            String encryptedExpiration = in.readNullUTF();
                                            int encryptionFrom = in.readCompressedInt();
                                            int encryptionRecipient = in.readCompressedInt();
                                            process.setCommand(
                                                "update_credit_card_number_and_expiration",
                                                Integer.valueOf(pkey),
                                                maskedCardNumber,
                                                encryptedCardNumber==null ? null : AOServProtocol.FILTERED,
                                                encryptedExpiration==null ? null : AOServProtocol.FILTERED,
                                                encryptionFrom==-1 ? null : Integer.valueOf(encryptionFrom),
                                                encryptionRecipient==-1 ? null : Integer.valueOf(encryptionRecipient)
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case UPDATE_CREDIT_CARD_EXPIRATION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String encryptedExpiration = in.readUTF();
                                            int encryptionFrom = in.readCompressedInt();
                                            int encryptionRecipient = in.readCompressedInt();
                                            process.setCommand(
                                                "update_credit_card_expiration",
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case UPDATE_HTTPD_TOMCAT_DATA_SOURCE:
                                        {
                                            int pkey=in.readCompressedInt();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case UPDATE_HTTPD_TOMCAT_PARAMETER :
                                        {
                                            int pkey=in.readCompressedInt();
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case UPDATE_PACKAGE_DEFINITION :
                                        {
                                            int pkey=in.readCompressedInt();
                                            String accounting=in.readUTF();
                                            String category=in.readUTF();
                                            String name=in.readUTF().trim();
                                            String version=in.readUTF().trim();
                                            String display=in.readUTF().trim();
                                            String description=in.readUTF().trim();
                                            int setupFee=in.readCompressedInt();
                                            String setupFeeTransactionType=in.readBoolean()?in.readUTF():null;
                                            int monthlyRate=in.readCompressedInt();
                                            String monthlyRateTransactionType=in.readUTF();
                                            process.setCommand(
                                                "update_package_definition",
                                                Integer.valueOf(pkey),
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=true;
                                        }
                                        break;
                                    case WAIT_FOR_REBUILD :
                                        {
                                            int clientTableID=in.readCompressedInt();
                                            SchemaTable.TableID tableID=TableHandler.convertFromClientTableID(conn, source, clientTableID);
                                            if(tableID==null) throw new IOException("Client table not supported: #"+clientTableID);
                                            int aoServer=in.readCompressedInt();
                                            switch(tableID) {
                                                case HTTPD_SITES :
                                                    process.setCommand(
                                                        AOSHCommand.WAIT_FOR_HTTPD_SITE_REBUILD,
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                                        Integer.valueOf(aoServer)
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
                                            resp1=AOServProtocol.DONE;
                                            sendInvalidateList=false;
                                            break;
                                        }
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
                            } catch (SQLException err) {
                                if(conn.rollbackAndClose()) {
                                    connRolledBack=true;
                                    invalidateList=null;
                                }
                                throw err;
                            } catch(IOException err) {
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
                        if(invalidateList!=null) invalidateTables(invalidateList, source);

                        // Write the response codes
                        if(resp1!=-1) out.writeByte(resp1);
                        if(hasResp2Int) out.writeCompressedInt(resp2Int);
                        else if(hasResp2Long) out.writeLong(resp2Long);
                        else if(hasResp2Short) out.writeShort(resp2Short);
                        else if(resp2String!=null) out.writeUTF(resp2String);
                        else if(hasResp2NullLongString) out.writeNullLongUTF(resp2NullLongString);
                        else if(resp2LongString!=null) out.writeLongUTF(resp2LongString);
                        else if(hasResp2Boolean) out.writeBoolean(resp2Boolean);
                        else if(hasResp2InboxAttributes) {
                            out.writeBoolean(resp2InboxAttributes!=null);
                            if(resp2InboxAttributes!=null) resp2InboxAttributes.write(out, source.getProtocolVersion());
                        } else if(hasResp2LongArray) {
                            for(int c=0;c<resp2LongArray.length;c++) out.writeLong(resp2LongArray[c]);
                        }
                        if(resp3String!=null) out.writeUTF(resp3String);
                        if(resp4String!=null) out.writeUTF(resp4String);

                        // Write the invalidate list
                        if(sendInvalidateList) {
                            int numTables=clientInvalidateList.size();
                            for(int c=0;c<numTables;c++) {
                                int tableID=clientInvalidateList.getInt(c);
                                out.writeCompressedInt(tableID);
                            }
                            out.writeCompressedInt(-1);
                        }
                    } catch(RuntimeException err) {
                        keepOpen = false;
                        throw err;
                    } catch(SQLException err) {
                        logger.log(Level.SEVERE, null, err);
                        String message=err.getMessage();
                        out.writeByte(AOServProtocol.SQL_EXCEPTION);
                        out.writeUTF(message==null?"":message);
                    } catch(IOException err) {
                        logger.log(Level.SEVERE, null, err);
                        String message=err.getMessage();
                        out.writeByte(AOServProtocol.IO_EXCEPTION);
                        out.writeUTF(message==null?"":message);
                    } finally {
                        if(currentThread.getPriority()!=Thread.NORM_PRIORITY) {
                            currentThread.setPriority(Thread.NORM_PRIORITY);
                            process.setPriority(Thread.NORM_PRIORITY);
                        }
                    }
                }
            }
            out.flush();
            if(addTime) addHistory(process);
            process.commandCompleted();
        } finally {
            if(addTime) {
                synchronized(MasterServer.class) {
                    concurrency--;
                    totalTime+=(System.currentTimeMillis()-requestStartTime);
                }
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
        List<RequestSource> listenerCopy=new ArrayList<RequestSource>(cacheListeners.size());
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
                                    List<String> affectedBusinesses=invalidateList.getAffectedBusinesses(tableID);
                                    List<Integer> affectedServers=invalidateList.getAffectedServers(tableID);
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
            Profiler.setProfilerLevel(MasterConfiguration.getProfilerLevel());

            // Configure the SSL
            synchronized(SSLServer.class) {
                if(!SSLServer.sslProviderLoaded[0]) {
                    boolean useSSL=false;
                    String trustStorePath=MasterConfiguration.getSSLTruststorePath();
                    if(trustStorePath!=null && trustStorePath.length()>0) {
                        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                        useSSL=true;
                    }
                    String trustStorePassword=MasterConfiguration.getSSLTruststorePassword();
                    if(trustStorePassword!=null && trustStorePassword.length()>0) {
                        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
                        useSSL=true;
                    }
                    String keyStorePath=MasterConfiguration.getSSLKeystorePath();
                    if(keyStorePath!=null && keyStorePath.length()>0) {
                        System.setProperty("javax.net.ssl.keyStore", keyStorePath);
                        useSSL=true;
                    }
                    String keyStorePassword=MasterConfiguration.getSSLKeystorePassword();
                    if(keyStorePassword!=null && keyStorePassword.length()>0) {
                        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
                        useSSL=true;
                    }
                    if(useSSL) {
                        Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
                        SSLServer.sslProviderLoaded[0]=true;
                    }
                }
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
                        if(TCPServer.PROTOCOL.equals(protocol)) new TCPServer(bind, port);
                        else if(SSLServer.PROTOCOL.equals(protocol)) new SSLServer(bind, port);
                        else throw new IllegalArgumentException("Unknown protocol: "+protocol);
                    }
                }
            }

            AccountCleaner.start();
            CreditCardHandler.start();
            DNSHandler.start();
            FailoverHandler.start();
            //TicketHandler.start();
        } catch (IOException err) {
            ErrorPrinter.printStackTraces(err);
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
    public static <T extends AOServObject> void writeObjects(
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
    public static void writeObjects(RequestSource source, CompressedDataOutputStream out, boolean provideProgress, List<? extends AOServObject> objs) throws IOException {
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
    public static void writeObjectsSynced(RequestSource source, CompressedDataOutputStream out, boolean provideProgress, List<? extends AOServObject> objs) throws IOException {
        AOServProtocol.Version version=source.getProtocolVersion();

        int size=objs.size();
        if (provideProgress) {
            out.writeByte(AOServProtocol.NEXT);
            out.writeCompressedInt(size);
        }
        for(int c=0;c<size;c++) {
            out.writeByte(AOServProtocol.NEXT);
            AOServObject obj=objs.get(c);
            synchronized(obj) {
                obj.write(out, version);
            }
        }
    }

    public static String authenticate(
        DatabaseConnection conn,
        String remoteHost, 
        String connectAs, 
        String authenticateAs, 
        String password
    ) throws IOException, SQLException {
        if(connectAs.length()==0) return "Connection attempted with empty connect username";
        if(authenticateAs.length()==0) return "Connection attempted with empty authentication username";

        if(!BusinessHandler.isBusinessAdministrator(conn, authenticateAs)) return "Unable to find BusinessAdministrator: "+authenticateAs;

        if(BusinessHandler.isBusinessAdministratorDisabled(conn, authenticateAs)) return "BusinessAdministrator disabled: "+authenticateAs;

        if (!isHostAllowed(conn, authenticateAs, remoteHost)) return "Connection from "+remoteHost+" as "+authenticateAs+" not allowed.";

        // Authenticate the client first
        if(password.length()==0) return "Connection attempted with empty password";

        String correctCrypted=BusinessHandler.getBusinessAdministrator(conn, authenticateAs).getPassword();
        if(
            correctCrypted==null
            || correctCrypted.length()<=2
            || !BusinessAdministrator.passwordMatches(password, correctCrypted)
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

    private static final Object historyLock=new Object();
    private static MasterHistory[] masterHistory;
    private static int masterHistoryStart=0;

    private static void addHistory(
        MasterProcess process
    ) throws IOException {
        synchronized(historyLock) {
            if(masterHistory==null) masterHistory=new MasterHistory[
                MasterConfiguration.getHistorySize()
            ];
            masterHistory[masterHistoryStart]=new MasterHistory(
                process.getProcessID(),
                process.getConnectorID(),
                process.getAuthenticatedUser(),
                process.getEffectiveUser(),
                process.getHost(),
                process.getProtocol(),
                process.isSecure(),
                process.getStateStartTime(),
                System.currentTimeMillis(),
                process.getCommand()
            );
            masterHistoryStart++;
            if(masterHistoryStart>=masterHistory.length) {
                masterHistoryStart=0;
            }
        }
    }

    public static void writeHistory(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        MasterUser masterUser,
        com.aoindustries.aoserv.client.MasterServer[] masterServers
    ) throws IOException, SQLException {
        // Create the list of objects first
        List<MasterHistory> objs=new ArrayList<MasterHistory>();
        synchronized(historyLock) {
            // Grab a copy of the history
            MasterHistory[] history=masterHistory;
            if(history!=null) {
                int historyLen=history.length;
                //objs.ensureCapacity(historyLen);
                int startPos=masterHistoryStart;
                for(int c=0;c<historyLen;c++) {
                    MasterHistory mh=history[(c+startPos)%historyLen];
                    if(mh!=null) {
                        if(masterUser!=null && masterServers.length==0) {
                            objs.add(mh);
                        } else {
                            if(UsernameHandler.canAccessUsername(conn, source, mh.getEffectiveUser())) objs.add(mh);
                        }
                    }
                }
            }
        }
        writeObjects(source, out, provideProgress, objs);
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
            List<MasterServerStat> objs=new ArrayList<MasterServerStat>();
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

            addStat(objs, MasterServerStat.METHOD_CONCURRENCY, Integer.toString(Profiler.getConcurrency()), "Current number of virtual machine methods in use");
            addStat(objs, MasterServerStat.METHOD_MAX_CONCURRENCY, Integer.toString(Profiler.getMaxConcurrency()), "Peak number of virtual machine methods in use");
            addStat(objs, MasterServerStat.METHOD_PROFILE_LEVEL, Integer.toString(Profiler.getProfilerLevel()), "Current method profiling level");
            addStat(objs, MasterServerStat.METHOD_USES, Long.toString(Profiler.getMethodUses()), "Number of virtual machine methods invoked");

            addStat(objs, MasterServerStat.PROTOCOL_VERSION, StringUtility.buildList(AOServProtocol.Version.values()), "Supported AOServProtocol version numbers");

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
     */
    public static void checkAccessHostname(DatabaseConnection conn, RequestSource source, String action, String hostname, List<String> tlds) throws IOException, SQLException {
        try {
            String zone = DNSZoneTable.getDNSZoneForHostname(hostname, tlds);

            if(conn.executeBooleanQuery(
                "select (select zone from dns_forbidden_zones where zone=?) is not null",
                zone
            )) throw new SQLException("Access to this hostname forbidden: Exists in dns_forbidden_zones: "+hostname);

            String username = source.getUsername();

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
        } catch(IllegalArgumentException err) {
            SQLException sqlErr=new SQLException();
            sqlErr.initCause(err);
            throw sqlErr;
        }
    }

    public static com.aoindustries.aoserv.client.MasterServer[] getMasterServers(DatabaseConnection conn, String username) throws IOException, SQLException {
        synchronized(MasterServer.class) {
            if(masterServers==null) masterServers=new HashMap<String,com.aoindustries.aoserv.client.MasterServer[]>();
            com.aoindustries.aoserv.client.MasterServer[] mss=masterServers.get(username);
            if(mss!=null) return mss;
            PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement("select ms.* from master_users mu, master_servers ms where mu.is_active and mu.username=? and mu.username=ms.username");
            try {
                List<com.aoindustries.aoserv.client.MasterServer> v=new ArrayList<com.aoindustries.aoserv.client.MasterServer>();
                pstmt.setString(1, username);
                ResultSet results=pstmt.executeQuery();
                while(results.next()) {
                    com.aoindustries.aoserv.client.MasterServer ms=new com.aoindustries.aoserv.client.MasterServer();
                    ms.init(results);
                    v.add(ms);
                }
                mss=new com.aoindustries.aoserv.client.MasterServer[v.size()];
                v.toArray(mss);
                masterServers.put(username, mss);
                return mss;
            } finally {
                pstmt.close();
            }
        }
    }

    public static MasterUser getMasterUser(DatabaseConnection conn, String username) throws IOException, SQLException {
        synchronized(MasterServer.class) {
            if(masterUsers==null) {
                Statement stmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                try {
                    Map<String,MasterUser> table=new HashMap<String,MasterUser>();
                    ResultSet results=stmt.executeQuery("select * from master_users where is_active");
                    while(results.next()) {
                        MasterUser mu=new MasterUser();
                        mu.init(results);
                        table.put(results.getString(1), mu);
                    }
                    masterUsers=table;
                } finally {
                    stmt.close();
                }
            }
            return masterUsers.get(username);
        }
    }

    /**
     * Gets the hosts that are allowed for the provided username.
     */
    public static boolean isHostAllowed(DatabaseConnection conn, String username, String host) throws IOException, SQLException {
        synchronized(MasterServer.class) {
            if(masterHosts==null) {
                Statement stmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                try {
                    Map<String,List<String>> table=new HashMap<String,List<String>>();
                    ResultSet results=stmt.executeQuery("select mh.username, mh.host from master_hosts mh, master_users mu where mh.username=mu.username and mu.is_active");
                    while(results.next()) {
                        String un=results.getString(1);
                        String ho=results.getString(2);
                        List<String> sv=table.get(un);
                        if(sv==null) table.put(un, sv=new SortedArrayList<String>());
                        sv.add(ho);
                    }
                    masterHosts=table;
                } finally {
                    stmt.close();
                }
            }
            if(getMasterUser(conn, username)!=null) {
                List<String> hosts=masterHosts.get(username);
                // Allow from anywhere if no hosts are provided
                if(hosts==null) return true;
                String remoteHost=InetAddress.getByName(host).getHostAddress();
                int size = hosts.size();
                for (int c = 0; c < size; c++) {
                    String tempAddress = InetAddress.getByName(hosts.get(c)).getHostAddress();
                    if (tempAddress.equals(remoteHost)) return true;
                }
                return false;
            } else {
                // Normal users can connect from any where
                return BusinessHandler.getBusinessAdministrator(conn, username)!=null;
            }
        }
    }

    public static void writeObject(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String sql,
        int param1,
        String param2,
        AOServObject obj
    ) throws IOException, SQLException {
        AOServProtocol.Version version = source.getProtocolVersion();

        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setInt(1, param1);
            pstmt.setString(2, param2);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    obj.init(results);
                    out.writeByte(AOServProtocol.NEXT);
                    obj.write(out, version);
                } else out.writeByte(AOServProtocol.DONE);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void writeObject(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String sql,
        int param1,
        AOServObject obj
    ) throws IOException, SQLException {
        AOServProtocol.Version version=source.getProtocolVersion();

        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setInt(1, param1);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    obj.init(results);
                    out.writeByte(AOServProtocol.NEXT);
                    obj.write(out, version);
                } else out.writeByte(AOServProtocol.DONE);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void writeObject(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String sql,
        String param1,
        int param2,
        AOServObject obj
    ) throws IOException, SQLException {
        AOServProtocol.Version version=source.getProtocolVersion();

        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setString(1, param1);
            pstmt.setInt(2, param2);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    obj.init(results);
                    out.writeByte(AOServProtocol.NEXT);
                    obj.write(out, version);
                } else out.writeByte(AOServProtocol.DONE);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void writeObject(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String sql,
        int param1,
        String param2,
        String param3,
        String param4,
        String param5,
        AOServObject obj
    ) throws IOException, SQLException {
        AOServProtocol.Version version=source.getProtocolVersion();

        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setInt(1, param1);
            pstmt.setString(2, param2);
            pstmt.setString(3, param3);
            pstmt.setString(4, param4);
            pstmt.setString(5, param5);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    obj.init(results);
                    out.writeByte(AOServProtocol.NEXT);
                    obj.write(out, version);
                } else out.writeByte(AOServProtocol.DONE);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void writeObject(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        String sql,
        String param1,
        int param2,
        String param3,
        int param4,
        String param5,
        int param6,
        String param7,
        int param8,
        AOServObject obj
    ) throws IOException, SQLException {
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setString(1, param1);
            pstmt.setInt(2, param2);
            pstmt.setString(3, param3);
            pstmt.setInt(4, param4);
            pstmt.setString(5, param5);
            pstmt.setInt(6, param6);
            pstmt.setString(7, param7);
            pstmt.setInt(8, param8);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    obj.init(results);
                    out.writeByte(AOServProtocol.NEXT);
                    obj.write(out, source.getProtocolVersion());
                } else out.writeByte(AOServProtocol.DONE);
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void fetchObjects(
        DatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        AOServObject obj,
        String sql,
        Object ... params
    ) throws IOException, SQLException {
        AOServProtocol.Version version=source.getProtocolVersion();

        Connection dbConn=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false);

        PreparedStatement pstmt=dbConn.prepareStatement("declare fetch_objects cursor for "+sql);
        try {
            DatabaseConnection.setParams(pstmt, params);
            pstmt.executeUpdate();
        } catch(SQLException err) {
            System.err.println("Error from select: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }

        String sqlString="fetch "+TableHandler.RESULT_SET_BATCH_SIZE+" from fetch_objects";
        Statement stmt = dbConn.createStatement();
        try {
            while(true) {
                int batchSize=0;
                ResultSet results=stmt.executeQuery(sqlString);
                try {
                    while(results.next()) {
                        obj.init(results);
                        out.writeByte(AOServProtocol.NEXT);
                        obj.write(out, version);
                        batchSize++;
                    }
                } finally {
                    results.close();
                }
                if(batchSize<TableHandler.RESULT_SET_BATCH_SIZE) break;
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+sqlString);
            throw err;
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
        AOServObject obj,
        String sql,
        Object ... params
    ) throws IOException, SQLException {
        if(!provideProgress) fetchObjects(conn, source, out, obj, sql, params);
        else {
            PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
            try {
                DatabaseConnection.setParams(pstmt, params);
                ResultSet results = pstmt.executeQuery();
                try {
                    writeObjects(source, out, provideProgress, obj, results);
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+pstmt.toString());
                throw err;
            } finally {
                pstmt.close();
            }
        }
    }

    public static void writePenniesCheckBusiness(
        DatabaseConnection conn,
        RequestSource source,
        String action,
        String accounting,
        CompressedDataOutputStream out,
        String sql,
        String param1
    ) throws IOException, SQLException {
        BusinessHandler.checkAccessBusiness(conn, source, action, accounting);
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setString(1, param1);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    out.writeByte(AOServProtocol.DONE);
                    out.writeCompressedInt(SQLUtility.getPennies(results.getString(1)));
                } else throw new SQLException("No row returned.");
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void writePenniesCheckBusiness(
        DatabaseConnection conn,
        RequestSource source,
        String action,
        String accounting,
        CompressedDataOutputStream out,
        String sql,
        String param1,
        Timestamp param2
    ) throws IOException, SQLException {
        BusinessHandler.checkAccessBusiness(conn, source, action, accounting);
        PreparedStatement pstmt = conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(sql);
        try {
            pstmt.setString(1, param1);
            pstmt.setTimestamp(2, param2);
            ResultSet results=pstmt.executeQuery();
            try {
                if(results.next()) {
                    out.writeByte(AOServProtocol.DONE);
                    out.writeCompressedInt(SQLUtility.getPennies(results.getString(1)));
                } else throw new SQLException("No row returned.");
            } finally {
                results.close();
            }
        } catch(SQLException err) {
            System.err.println("Error from query: "+pstmt.toString());
            throw err;
        } finally {
            pstmt.close();
        }
    }

    public static void invalidateTable(SchemaTable.TableID tableID) {
        if(tableID==SchemaTable.TableID.MASTER_HOSTS) {
            synchronized(MasterServer.class) {
                masterHosts=null;
            }
        } else if(tableID==SchemaTable.TableID.MASTER_SERVERS) {
            synchronized(MasterServer.class) {
                masterHosts=null;
                masterServers=null;
            }
        } else if(tableID==SchemaTable.TableID.MASTER_USERS) {
            synchronized(MasterServer.class) {
                masterHosts=null;
                masterServers=null;
                masterUsers=null;
            }
        }
    }

    public static void updateAOServProtocolLastUsed(DatabaseConnection conn, AOServProtocol.Version protocolVersion) throws IOException, SQLException {
        conn.executeUpdate("update aoserv_protocols set last_used=now()::date where version=? and (last_used is null or last_used<now()::date)", protocolVersion.getVersion());
    }
}
