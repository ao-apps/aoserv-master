package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.AOServConnectorUtils;
import com.aoindustries.aoserv.client.AOServPermissionService;
import com.aoindustries.aoserv.client.AOServService;
import com.aoindustries.aoserv.client.AOServerResourceService;
import com.aoindustries.aoserv.client.AOServerService;
import com.aoindustries.aoserv.client.ArchitectureService;
import com.aoindustries.aoserv.client.BackupPartitionService;
import com.aoindustries.aoserv.client.BackupRetentionService;
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorService;
import com.aoindustries.aoserv.client.BusinessService;
import com.aoindustries.aoserv.client.CountryCodeService;
import com.aoindustries.aoserv.client.DisableLogService;
import com.aoindustries.aoserv.client.FailoverFileReplicationService;
import com.aoindustries.aoserv.client.FailoverMySQLReplicationService;
import com.aoindustries.aoserv.client.LanguageService;
import com.aoindustries.aoserv.client.MySQLDBUserService;
import com.aoindustries.aoserv.client.MySQLDatabaseService;
import com.aoindustries.aoserv.client.MySQLReservedWordService;
import com.aoindustries.aoserv.client.MySQLServerService;
import com.aoindustries.aoserv.client.MySQLUserService;
import com.aoindustries.aoserv.client.NetBindService;
import com.aoindustries.aoserv.client.NetDeviceIDService;
import com.aoindustries.aoserv.client.NetProtocolService;
import com.aoindustries.aoserv.client.OperatingSystemService;
import com.aoindustries.aoserv.client.OperatingSystemVersionService;
import com.aoindustries.aoserv.client.PackageCategoryService;
import com.aoindustries.aoserv.client.PostgresServerService;
import com.aoindustries.aoserv.client.PostgresVersionService;
import com.aoindustries.aoserv.client.ProtocolService;
import com.aoindustries.aoserv.client.ResourceService;
import com.aoindustries.aoserv.client.ResourceTypeService;
import com.aoindustries.aoserv.client.ServerFarmService;
import com.aoindustries.aoserv.client.ServerService;
import com.aoindustries.aoserv.client.ServiceName;
import com.aoindustries.aoserv.client.TechnologyClassService;
import com.aoindustries.aoserv.client.TechnologyNameService;
import com.aoindustries.aoserv.client.TechnologyService;
import com.aoindustries.aoserv.client.TechnologyVersionService;
import com.aoindustries.aoserv.client.TicketCategoryService;
import com.aoindustries.aoserv.client.TicketPriorityService;
import com.aoindustries.aoserv.client.TicketStatusService;
import com.aoindustries.aoserv.client.TicketTypeService;
import com.aoindustries.aoserv.client.TimeZoneService;
import com.aoindustries.aoserv.client.UsernameService;
import com.aoindustries.security.LoginException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of <code>AOServConnector</code> that operates directly on
 * the master database.  This level is also responsible for coordinating the
 * cache invalidation signals through the system.
 *
 * TODO: Check if disabled on all calls?  This would also set a timestamp and unexport/remove objects when not used for a period of time.
 *       This way things won't build over time and disabled accounts will take affect immediately.
 *
 * @author  AO Industries, Inc.
 */
final public class DatabaseConnector implements AOServConnector<DatabaseConnector,DatabaseConnectorFactory> {

    final DatabaseConnectorFactory factory;
    Locale locale;
    final String connectAs;
    private final String authenticateAs;
    private final String password;
    /* TODO
    final DatabaseAOServerDaemonHostService aoserverDaemonHosts;
     */
    final DatabaseAOServerResourceService aoserverResources;
    final DatabaseAOServerService aoservers;
    final DatabaseAOServPermissionService aoservPermissions;
    /* TODO
    final DatabaseAOServProtocolService aoservProtocols;
    final DatabaseAOSHCommandService aoshCommands;
     */
    final DatabaseArchitectureService architectures;
    final DatabaseBackupPartitionService backupPartitions;
    final DatabaseBackupRetentionService backupRetentions;
    /* TODO
    final DatabaseBankAccountService bankAccounts;
    final DatabaseBankTransactionTypeService bankTransactionTypes;
    final DatabaseBankTransactionService bankTransactions;
    final DatabaseBankService banks;
    final DatabaseBlackholeEmailAddressService blackholeEmailAddresss;
    final DatabaseBrandService brands;
     */
    final DatabaseBusinessAdministratorService businessAdministrators;
    /* TODO
    final DatabaseBusinessAdministratorPermissionService businessAdministratorPermissions;
    final DatabaseBusinessProfileService businessProfiles;
     */
    final DatabaseBusinessService businesses;
    /* TODO
    final DatabaseBusinessServerService businessServers;
    final DatabaseClientJvmProfileService clientJvmProfiles;
     */
    final DatabaseCountryCodeService countryCodes;
    /* TODO
    final DatabaseCreditCardProcessorService creditCardProcessors;
    final DatabaseCreditCardTransactionService creditCardTransactions;
    final DatabaseCreditCardService creditCards;
    final DatabaseCvsRepositoryService cvsRepositories;
     */
    final DatabaseDisableLogService disableLogs;
    /*
    final DatabaseDistroFileTypeService distroFileTypes;
    final DatabaseDistroFileService distroFiles;
    final DatabaseDNSForbiddenZoneService dnsForbiddenZones;
    final DatabaseDNSRecordService dnsRecords;
    final DatabaseDNSTLDService dnsTLDs;
    final DatabaseDNSTypeService dnsTypes;
    final DatabaseDNSZoneService dnsZones;
    final DatabaseEmailAddressService emailAddresss;
    final DatabaseEmailAttachmentBlockService emailAttachmentBlocks;
    final DatabaseEmailAttachmentTypeService emailAttachmentTypes;
    final DatabaseEmailDomainService emailDomains;
    final DatabaseEmailForwardingService emailForwardings;
    final DatabaseEmailListAddressService emailListAddresss;
    final DatabaseEmailListService emailLists;
    final DatabaseEmailPipeAddressService emailPipeAddresss;
    final DatabaseEmailPipeService emailPipes;
    final DatabaseEmailSmtpRelayTypeService emailSmtpRelayTypes;
    final DatabaseEmailSmtpRelayService emailSmtpRelays;
    final DatabaseEmailSmtpSmartHostDomainService emailSmtpSmartHostDomains;
    final DatabaseEmailSmtpSmartHostService emailSmtpSmartHosts;
    final DatabaseEmailSpamAssassinIntegrationModeService emailSpamAssassinIntegrationModes;
    final DatabaseEncryptionKeyService encryptionKeys;
    final DatabaseExpenseCategoryService expenseCategories;
    final DatabaseFailoverFileLogService failoverFileLogs;
     */
    final DatabaseFailoverFileReplicationService failoverFileReplications;
    // TODO: final DatabaseFailoverFileScheduleService failoverFileSchedules;
    final DatabaseFailoverMySQLReplicationService failoverMySQLReplications;
    /* TODO
    final DatabaseFileBackupSettingService fileBackupSettings;
    final DatabaseFTPGuestUserService ftpGuestUsers;
    final DatabaseHttpdBindService httpdBinds;
    final DatabaseHttpdJBossSiteService httpdJBossSites;
    final DatabaseHttpdJBossVersionService httpdJBossVersions;
    final DatabaseHttpdJKCodeService httpdJKCodes;
    final DatabaseHttpdJKProtocolService httpdJKProtocols;
    final DatabaseHttpdServerService httpdServers;
    final DatabaseHttpdSharedTomcatService httpdSharedTomcats;
    final DatabaseHttpdSiteAuthenticatedLocationService httpdSiteAuthenticatedLocations;
    final DatabaseHttpdSiteBindService httpdSiteBinds;
    final DatabaseHttpdSiteURLService httpdSiteURLs;
    final DatabaseHttpdSiteService httpdSites;
    final DatabaseHttpdStaticSiteService httpdStaticSites;
    final DatabaseHttpdTomcatContextService httpdTomcatContexts;
    final DatabaseHttpdTomcatDataSourceService httpdTomcatDataSources;
    final DatabaseHttpdTomcatParameterService httpdTomcatParameters;
    final DatabaseHttpdTomcatSiteService httpdTomcatSites;
    final DatabaseHttpdTomcatSharedSiteService httpdTomcatSharedSites;
    final DatabaseHttpdTomcatStdSiteService httpdTomcatStdSites;
    final DatabaseHttpdTomcatVersionService httpdTomcatVersions;
    final DatabaseHttpdWorkerService httpdWorkers;
    final DatabaseIPAddressService ipAddresss;
    */
    final DatabaseLanguageService languages;
    /* TODO
    final DatabaseLinuxAccAddressService linuxAccAddresss;
    final DatabaseLinuxAccountTypeService linuxAccountTypes;
    final DatabaseLinuxAccountService linuxAccounts;
    final DatabaseLinuxGroupAccountService linuxGroupAccounts;
    final DatabaseLinuxGroupTypeService linuxGroupTypes;
    final DatabaseLinuxGroupService linuxGroups;
    final DatabaseLinuxIDService linuxIDs;
    final DatabaseLinuxServerAccountService linuxServerAccounts;
    final DatabaseLinuxServerGroupService linuxServerGroups;
    final DatabaseMajordomoListService majordomoLists;
    final DatabaseMajordomoServerService majordomoServers;
    final DatabaseMajordomoVersionService majordomoVersions;
    final DatabaseMasterHistoryService masterHistories;
    final DatabaseMasterHostService masterHosts;
    final DatabaseMasterServerService masterServers;
    final DatabaseMasterUserService masterUsers;
    final DatabaseMonthlyChargeService monthlyCharges;
     */
    final DatabaseMySQLDatabaseService mysqlDatabases;
    final DatabaseMySQLDBUserService mysqlDBUsers;
    final DatabaseMySQLReservedWordService mysqlReservedWords;
    final DatabaseMySQLServerService mysqlServers;
    final DatabaseMySQLUserService mysqlUsers;
    final DatabaseNetBindService netBinds;
    final DatabaseNetDeviceIDService netDeviceIDs;
    /* TODO
    final DatabaseNetDeviceService netDevices;
    final DatabaseNetPortService netPorts;
     */
    final DatabaseNetProtocolService netProtocols;
    /* TODO
    final DatabaseNetTcpRedirectService netTcpRedirects;
    final DatabaseNoticeLogService noticeLogs;
    final DatabaseNoticeTypeService noticeTypes;
    */
    final DatabaseOperatingSystemVersionService operatingSystemVersions;
    final DatabaseOperatingSystemService operatingSystems;
    final DatabasePackageCategoryService packageCategories;
    /* TODO
    final DatabasePackageDefinitionLimitService packageDefinitionLimits;
    final DatabasePackageDefinitionService packageDefinitions;
    final DatabasePaymentTypeService paymentTypes;
    final DatabasePhysicalServerService physicalServers;
    final DatabasePostgresDatabaseService postgresDatabases;
    final DatabasePostgresEncodingService postgresEncodings;
    final DatabasePostgresReservedWordService postgresReservedWords;
    final DatabasePostgresServerUserService postgresServerUsers;
     */
    final DatabasePostgresServerService postgresServers;
    // TODO: final DatabasePostgresUserService postgresUsers;
    final DatabasePostgresVersionService postgresVersions;
    // TODO: final DatabasePrivateFTPServerService privateFTPServers;
    // TODO: final DatabaseProcessorTypeService processorTypes;
    final DatabaseProtocolService protocols;
    /* TODO
    final DatabaseRackService racks;
    final DatabaseResellerService resellers;
     */
    final DatabaseResourceTypeService resourceTypes;
    final DatabaseResourceService resources;
    final DatabaseServerFarmService serverFarms;
    final DatabaseServerService servers;
    /* TODO
    final DatabaseShellService shells;
    final DatabaseSignupRequestOptionService signupRequestOptions;
    final DatabaseSignupRequestService signupRequests;
    final DatabaseSpamEmailMessageService spamEmailMessages;
    final DatabaseSystemEmailAliasService systemEmailAliass;
     */
    final DatabaseTechnologyService technologies;
    final DatabaseTechnologyClassService technologyClasses;
    final DatabaseTechnologyNameService technologyNames;
    final DatabaseTechnologyVersionService technologyVersions;
    /* TODO
    final DatabaseTicketActionTypeService ticketActionTypes;
    final DatabaseTicketActionService ticketActions;
    final DatabaseTicketAssignmentService ticketAssignments;
    final DatabaseTicketBrandCategoryService ticketBrandCategories;
    */
    final DatabaseTicketCategoryService ticketCategories;
    final DatabaseTicketPriorityService ticketPriorities;
    final DatabaseTicketStatusService ticketStatuses;
    final DatabaseTicketTypeService ticketTypes;
    /* TODO
    final DatabaseTicketService tickets;
    */
    final DatabaseTimeZoneService timeZones;
    /* TODO
    final DatabaseTransactionTypeService transactionTypes;
    final DatabaseTransactionService transactions;
    final DatabaseUSStateService usStates;
     */
    final DatabaseUsernameService usernames;
    /* TODO
    final DatabaseVirtualDiskService virtualDisks;
    final DatabaseVirtualServerService virtualServers;
    final DatabaseWhoisHistoryService whoisHistories;
     */

    DatabaseConnector(DatabaseConnectorFactory factory, Locale locale, String connectAs, String authenticateAs, String password) throws RemoteException, LoginException {
        this.factory = factory;
        this.locale = locale;
        this.connectAs = connectAs;
        this.authenticateAs = authenticateAs;
        this.password = password;
        /* TODO
        aoserverDaemonHosts = new DatabaseAOServerDaemonHostService(this);
         */
        aoserverResources = new DatabaseAOServerResourceService(this);
        aoservers = new DatabaseAOServerService(this);
        aoservPermissions = new DatabaseAOServPermissionService(this);
        /* TODO
        aoservProtocols = new DatabaseAOServProtocolService(this);
        aoshCommands = new DatabaseAOSHCommandService(this);
         */
        architectures = new DatabaseArchitectureService(this);
        backupPartitions = new DatabaseBackupPartitionService(this);
        backupRetentions = new DatabaseBackupRetentionService(this);
        /* TODO
        bankAccounts = new DatabaseBankAccountService(this);
        bankTransactionTypes = new DatabaseBankTransactionTypeService(this);
        bankTransactions = new DatabaseBankTransactionService(this);
        banks = new DatabaseBankService(this);
        blackholeEmailAddresss = new DatabaseBlackholeEmailAddressService(this);
        brands = new DatabaseBrandService(this);
         */
        businessAdministrators = new DatabaseBusinessAdministratorService(this);
        /* TODO
        businessAdministratorPermissions = new DatabaseBusinessAdministratorPermissionService(this);
        businessProfiles = new DatabaseBusinessProfileService(this);
         */
        businesses = new DatabaseBusinessService(this);
        /* TODO
        businessServers = new DatabaseBusinessServerService(this);
        clientJvmProfiles = new DatabaseClientJvmProfileService(this);
         */
        countryCodes = new DatabaseCountryCodeService(this);
        /* TODO
        creditCardProcessors = new DatabaseCreditCardProcessorService(this);
        creditCardTransactions = new DatabaseCreditCardTransactionService(this);
        creditCards = new DatabaseCreditCardService(this);
        cvsRepositories = new DatabaseCvsRepositoryService(this);
         */
        disableLogs = new DatabaseDisableLogService(this);
        /*
        distroFileTypes = new DatabaseDistroFileTypeService(this);
        distroFiles = new DatabaseDistroFileService(this);
        dnsForbiddenZones = new DatabaseDNSForbiddenZoneService(this);
        dnsRecords = new DatabaseDNSRecordService(this);
        dnsTLDs = new DatabaseDNSTLDService(this);
        dnsTypes = new DatabaseDNSTypeService(this);
        dnsZones = new DatabaseDNSZoneService(this);
        emailAddresss = new DatabaseEmailAddressService(this);
        emailAttachmentBlocks = new DatabaseEmailAttachmentBlockService(this);
        emailAttachmentTypes = new DatabaseEmailAttachmentTypeService(this);
        emailDomains = new DatabaseEmailDomainService(this);
        emailForwardings = new DatabaseEmailForwardingService(this);
        emailListAddresss = new DatabaseEmailListAddressService(this);
        emailLists = new DatabaseEmailListService(this);
        emailPipeAddresss = new DatabaseEmailPipeAddressService(this);
        emailPipes = new DatabaseEmailPipeService(this);
        emailSmtpRelayTypes = new DatabaseEmailSmtpRelayTypeService(this);
        emailSmtpRelays = new DatabaseEmailSmtpRelayService(this);
        emailSmtpSmartHostDomains = new DatabaseEmailSmtpSmartHostDomainService(this);
        emailSmtpSmartHosts = new DatabaseEmailSmtpSmartHostService(this);
        emailSpamAssassinIntegrationModes = new DatabaseEmailSpamAssassinIntegrationModeService(this);
        encryptionKeys = new DatabaseEncryptionKeyService(this);
        expenseCategories = new DatabaseExpenseCategoryService(this);
        failoverFileLogs = new DatabaseFailoverFileLogService(this);
         */
        failoverFileReplications = new DatabaseFailoverFileReplicationService(this);
        // TODO: failoverFileSchedules = new DatabaseFailoverFileScheduleService(this);
        failoverMySQLReplications = new DatabaseFailoverMySQLReplicationService(this);
        /* TODO
        fileBackupSettings = new DatabaseFileBackupSettingService(this);
        ftpGuestUsers = new DatabaseFTPGuestUserService(this);
        httpdBinds = new DatabaseHttpdBindService(this);
        httpdJBossSites = new DatabaseHttpdJBossSiteService(this);
        httpdJBossVersions = new DatabaseHttpdJBossVersionService(this);
        httpdJKCodes = new DatabaseHttpdJKCodeService(this);
        httpdJKProtocols = new DatabaseHttpdJKProtocolService(this);
        httpdServers = new DatabaseHttpdServerService(this);
        httpdSharedTomcats = new DatabaseHttpdSharedTomcatService(this);
        httpdSiteAuthenticatedLocations = new DatabaseHttpdSiteAuthenticatedLocationService(this);
        httpdSiteBinds = new DatabaseHttpdSiteBindService(this);
        httpdSiteURLs = new DatabaseHttpdSiteURLService(this);
        httpdSites = new DatabaseHttpdSiteService(this);
        httpdStaticSites = new DatabaseHttpdStaticSiteService(this);
        httpdTomcatContexts = new DatabaseHttpdTomcatContextService(this);
        httpdTomcatDataSources = new DatabaseHttpdTomcatDataSourceService(this);
        httpdTomcatParameters = new DatabaseHttpdTomcatParameterService(this);
        httpdTomcatSites = new DatabaseHttpdTomcatSiteService(this);
        httpdTomcatSharedSites = new DatabaseHttpdTomcatSharedSiteService(this);
        httpdTomcatStdSites = new DatabaseHttpdTomcatStdSiteService(this);
        httpdTomcatVersions = new DatabaseHttpdTomcatVersionService(this);
        httpdWorkers = new DatabaseHttpdWorkerService(this);
        ipAddresss = new DatabaseIPAddressService(this);
        */
        languages = new DatabaseLanguageService(this);
        /* TODO
        linuxAccAddresss = new DatabaseLinuxAccAddressService(this);
        linuxAccountTypes = new DatabaseLinuxAccountTypeService(this);
        linuxAccounts = new DatabaseLinuxAccountService(this);
        linuxGroupAccounts = new DatabaseLinuxGroupAccountService(this);
        linuxGroupTypes = new DatabaseLinuxGroupTypeService(this);
        linuxGroups = new DatabaseLinuxGroupService(this);
        linuxIDs = new DatabaseLinuxIDService(this);
        linuxServerAccounts = new DatabaseLinuxServerAccountService(this);
        linuxServerGroups = new DatabaseLinuxServerGroupService(this);
        majordomoLists = new DatabaseMajordomoListService(this);
        majordomoServers = new DatabaseMajordomoServerService(this);
        majordomoVersions = new DatabaseMajordomoVersionService(this);
        masterHistories = new DatabaseMasterHistoryService(this);
        masterHosts = new DatabaseMasterHostService(this);
        masterServers = new DatabaseMasterServerService(this);
        masterUsers = new DatabaseMasterUserService(this);
        monthlyCharges = new DatabaseMonthlyChargeService(this);
         */
        mysqlDatabases = new DatabaseMySQLDatabaseService(this);
        mysqlDBUsers = new DatabaseMySQLDBUserService(this);
        mysqlReservedWords = new DatabaseMySQLReservedWordService(this);
        mysqlServers = new DatabaseMySQLServerService(this);
        mysqlUsers = new DatabaseMySQLUserService(this);
        netBinds = new DatabaseNetBindService(this);
        netDeviceIDs = new DatabaseNetDeviceIDService(this);
        /* TODO
        netDevices = new DatabaseNetDeviceService(this);
        netPorts = new DatabaseNetPortService(this);
         */
        netProtocols = new DatabaseNetProtocolService(this);
        /* TODO
        netTcpRedirects = new DatabaseNetTcpRedirectService(this);
        noticeLogs = new DatabaseNoticeLogService(this);
        noticeTypes = new DatabaseNoticeTypeService(this);
        */
        operatingSystemVersions = new DatabaseOperatingSystemVersionService(this);
        operatingSystems = new DatabaseOperatingSystemService(this);
        packageCategories = new DatabasePackageCategoryService(this);
        /* TODO
        packageDefinitionLimits = new DatabasePackageDefinitionLimitService(this);
        packageDefinitions = new DatabasePackageDefinitionService(this);
        paymentTypes = new DatabasePaymentTypeService(this);
        physicalServers = new DatabasePhysicalServerService(this);
        postgresDatabases = new DatabasePostgresDatabaseService(this);
        postgresEncodings = new DatabasePostgresEncodingService(this);
        postgresReservedWords = new DatabasePostgresReservedWordService(this);
        postgresServerUsers = new DatabasePostgresServerUserService(this);
         */
        postgresServers = new DatabasePostgresServerService(this);
        // TODO: postgresUsers = new DatabasePostgresUserService(this);
        postgresVersions = new DatabasePostgresVersionService(this);
        // TODO: privateFTPServers = new DatabasePrivateFTPServerService(this);
        // TODO: processorTypes = new DatabaseProcessorTypeService(this);
        protocols = new DatabaseProtocolService(this);
        /* TODO
        racks = new DatabaseRackService(this);
        resellers = new DatabaseResellerService(this);
         */
        resourceTypes = new DatabaseResourceTypeService(this);
        resources = new DatabaseResourceService(this);
        serverFarms = new DatabaseServerFarmService(this);
        servers = new DatabaseServerService(this);
        /* TODO
        shells = new DatabaseShellService(this);
        signupRequestOptions = new DatabaseSignupRequestOptionService(this);
        signupRequests = new DatabaseSignupRequestService(this);
        spamEmailMessages = new DatabaseSpamEmailMessageService(this);
        systemEmailAliass = new DatabaseSystemEmailAliasService(this);
         */
        technologies = new DatabaseTechnologyService(this);
        technologyClasses = new DatabaseTechnologyClassService(this);
        technologyNames = new DatabaseTechnologyNameService(this);
        technologyVersions = new DatabaseTechnologyVersionService(this);
        /* TODO
        ticketActionTypes = new DatabaseTicketActionTypeService(this);
        ticketActions = new DatabaseTicketActionService(this);
        ticketAssignments = new DatabaseTicketAssignmentService(this);
        ticketBrandCategories = new DatabaseTicketBrandCategoryService(this);
        */
        ticketCategories = new DatabaseTicketCategoryService(this);
        ticketPriorities = new DatabaseTicketPriorityService(this);
        ticketStatuses = new DatabaseTicketStatusService(this);
        ticketTypes = new DatabaseTicketTypeService(this);
        /* TODO
        tickets = new DatabaseTicketService(this);
        */
        timeZones = new DatabaseTimeZoneService(this);
        /* TODO
        transactionTypes = new DatabaseTransactionTypeService(this);
        transactions = new DatabaseTransactionService(this);
        usStates = new DatabaseUSStateService(this);
         */
        usernames = new DatabaseUsernameService(this);
        /* TODO
        virtualDisks = new DatabaseVirtualDiskService(this);
        virtualServers = new DatabaseVirtualServerService(this);
        whoisHistories = new DatabaseWhoisHistoryService(this);
         */
    }

    enum AccountType {
        MASTER,
        DAEMON,
        BUSINESS,
        DISABLED
    };

    /**
     * Determines the type of account logged-in based on the connectAs value.  This controls filtering and access.
     */
    AccountType getAccountType() throws IOException, SQLException {
        if(factory.isEnabledMasterUser(connectAs)) return AccountType.MASTER;
        if(factory.isEnabledDaemonUser(connectAs)) return AccountType.DAEMON;
        if(factory.isEnabledBusinessAdministrator(connectAs)) return AccountType.BUSINESS;
        return AccountType.DISABLED;
    }

    public DatabaseConnectorFactory getFactory() {
        return factory;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public String getConnectAs() {
        return connectAs;
    }

    public BusinessAdministrator getThisBusinessAdministrator() throws RemoteException {
        BusinessAdministrator obj = getBusinessAdministrators().get(connectAs);
        if(obj==null) throw new RemoteException("Unable to find BusinessAdministrator: "+connectAs);
        return obj;
    }

    public String getAuthenticateAs() {
        return authenticateAs;
    }

    public String getPassword() {
        return password;
    }

    private final AtomicReference<Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>>> tables = new AtomicReference<Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>>>();
    public Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>> getServices() throws RemoteException {
        Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>> ts = tables.get();
        if(ts==null) {
            ts = AOServConnectorUtils.createServiceMap(this);
            if(!tables.compareAndSet(null, ts)) ts = tables.get();
        }
        return ts;
    }

    /*
     * TODO
    public AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> getAoServerDaemonHosts();
    */
    public AOServerResourceService<DatabaseConnector,DatabaseConnectorFactory> getAoServerResources() {
        return aoserverResources;
    }

    public AOServerService<DatabaseConnector,DatabaseConnectorFactory> getAoServers() {
        return aoservers;
    }

    public AOServPermissionService<DatabaseConnector,DatabaseConnectorFactory> getAoservPermissions() {
        return aoservPermissions;
    }
    /* TODO
    public AOServProtocolService<DatabaseConnector,DatabaseConnectorFactory> getAoservProtocols();

    public AOSHCommandService<DatabaseConnector,DatabaseConnectorFactory> getAoshCommands();
    */
    public ArchitectureService<DatabaseConnector,DatabaseConnectorFactory> getArchitectures() {
        return architectures;
    }

    public BackupPartitionService<DatabaseConnector,DatabaseConnectorFactory> getBackupPartitions() {
        return backupPartitions;
    }

    public BackupRetentionService<DatabaseConnector,DatabaseConnectorFactory> getBackupRetentions() {
        return backupRetentions;
    }
    /* TODO
    public BankAccountService<DatabaseConnector,DatabaseConnectorFactory> getBankAccounts();

    public BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactionTypes();

    public BankTransactionService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactions();

    public BankService<DatabaseConnector,DatabaseConnectorFactory> getBanks();

    public BlackholeEmailAddressService<DatabaseConnector,DatabaseConnectorFactory> getBlackholeEmailAddresses();

    public BrandService<DatabaseConnector,DatabaseConnectorFactory> getBrands();
     */
    public BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministrators() {
        return businessAdministrators;
    }
    /*
    public BusinessAdministratorPermissionService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministratorPermissions();

    public BusinessProfileService<DatabaseConnector,DatabaseConnectorFactory> getBusinessProfiles();
     */
    public BusinessService<DatabaseConnector,DatabaseConnectorFactory> getBusinesses() {
        return businesses;
    }

    /* TODO
    public BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> getBusinessServers();

    public ClientJvmProfileService<DatabaseConnector,DatabaseConnectorFactory> getClientJvmProfiles();
    */
    public CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> getCountryCodes() {
        return countryCodes;
    }
    /* TODO
    public CreditCardProcessorService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardProcessors();

    public CreditCardTransactionService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardTransactions();

    public CreditCardService<DatabaseConnector,DatabaseConnectorFactory> getCreditCards();

    public CvsRepositoryService<DatabaseConnector,DatabaseConnectorFactory> getCvsRepositories();
     */
    public DisableLogService<DatabaseConnector,DatabaseConnectorFactory> getDisableLogs() {
        return disableLogs;
    }
    /*
    public DistroFileTypeService<DatabaseConnector,DatabaseConnectorFactory> getDistroFileTypes();

    public DistroFileService<DatabaseConnector,DatabaseConnectorFactory> getDistroFiles();

    public DNSForbiddenZoneService<DatabaseConnector,DatabaseConnectorFactory> getDnsForbiddenZones();

    public DNSRecordService<DatabaseConnector,DatabaseConnectorFactory> getDnsRecords();

    public DNSTLDService<DatabaseConnector,DatabaseConnectorFactory> getDnsTLDs();

    public DNSTypeService<DatabaseConnector,DatabaseConnectorFactory> getDnsTypes();

    public DNSZoneService<DatabaseConnector,DatabaseConnectorFactory> getDnsZones();

    public EmailAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailAddresses();

    public EmailAttachmentBlockService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentBlocks();

    public EmailAttachmentTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentTypes();

    public EmailDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailDomains();

    public EmailForwardingService<DatabaseConnector,DatabaseConnectorFactory> getEmailForwardings();

    public EmailListAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailListAddresses();

    public EmailListService<DatabaseConnector,DatabaseConnectorFactory> getEmailLists();

    public EmailPipeAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipeAddresses();

    public EmailPipeService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipes();

    public EmailSmtpRelayTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelayTypes();

    public EmailSmtpRelayService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelays();

    public EmailSmtpSmartHostDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHostDomains();

    public EmailSmtpSmartHostService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHosts();

    public EmailSpamAssassinIntegrationModeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSpamAssassinIntegrationModes();

    public EncryptionKeyService<DatabaseConnector,DatabaseConnectorFactory> getEncryptionKeys();

    public ExpenseCategoryService<DatabaseConnector,DatabaseConnectorFactory> getExpenseCategories();

    public FailoverFileLogService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileLogs();
    */
    public FailoverFileReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileReplications() {
        return failoverFileReplications;
    }
    /* TODO
    public FailoverFileScheduleService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileSchedules();
    */
    public FailoverMySQLReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverMySQLReplications() {
        return failoverMySQLReplications;
    }
    /* TODO
    public FileBackupSettingService<DatabaseConnector,DatabaseConnectorFactory> getFileBackupSettings();

    public FTPGuestUserService<DatabaseConnector,DatabaseConnectorFactory> getFtpGuestUsers();

    public HttpdBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdBinds();

    public HttpdJBossSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossSites();

    public HttpdJBossVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossVersions();

    public HttpdJKCodeService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKCodes();

    public HttpdJKProtocolService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKProtocols();

    public HttpdServerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdServers();

    public HttpdSharedTomcatService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSharedTomcats();

    public HttpdSiteAuthenticatedLocationService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteAuthenticatedLocations();

    public HttpdSiteBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteBinds();

    public HttpdSiteURLService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteURLs();

    public HttpdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSites();

    public HttpdStaticSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdStaticSites();

    public HttpdTomcatContextService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatContexts();

    public HttpdTomcatDataSourceService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatDataSources();

    public HttpdTomcatParameterService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatParameters();

    public HttpdTomcatSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSites();

    public HttpdTomcatSharedSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSharedSites();

    public HttpdTomcatStdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatStdSites();

    public HttpdTomcatVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatVersions();

    public HttpdWorkerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdWorkers();

    public IPAddressService<DatabaseConnector,DatabaseConnectorFactory> getIpAddresses();
    */
    public LanguageService<DatabaseConnector,DatabaseConnectorFactory> getLanguages() {
        return languages;
    }
    /* TODO
    public LinuxAccAddressService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccAddresses();

    public LinuxAccountTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccountTypes();

    public LinuxAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccounts();

    public LinuxGroupAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroupAccounts();

    public LinuxGroupTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroupTypes();

    public LinuxGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroups();

    public LinuxIDService<DatabaseConnector,DatabaseConnectorFactory> getLinuxIDs();

    public LinuxServerAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxServerAccounts();

    public LinuxServerGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxServerGroups();

    public MajordomoListService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoLists();

    public MajordomoServerService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoServers();

    public MajordomoVersionService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoVersions();

    public MasterHistoryService<DatabaseConnector,DatabaseConnectorFactory> getMasterHistory();

    public MasterHostService<DatabaseConnector,DatabaseConnectorFactory> getMasterHosts();

    public MasterServerService<DatabaseConnector,DatabaseConnectorFactory> getMasterServers();

    public MasterUserService<DatabaseConnector,DatabaseConnectorFactory> getMasterUsers();

    public MonthlyChargeService<DatabaseConnector,DatabaseConnectorFactory> getMonthlyCharges();
    */
    public MySQLDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDatabases() {
        return mysqlDatabases;
    }

    public MySQLDBUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDBUsers() {
        return mysqlDBUsers;
    }

    public MySQLReservedWordService<DatabaseConnector,DatabaseConnectorFactory> getMysqlReservedWords() {
        return mysqlReservedWords;
    }

    public MySQLServerService<DatabaseConnector,DatabaseConnectorFactory> getMysqlServers() {
        return mysqlServers;
    }

    public MySQLUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlUsers() {
        return mysqlUsers;
    }

    public NetBindService<DatabaseConnector,DatabaseConnectorFactory> getNetBinds() {
        return netBinds;
    }

    public NetDeviceIDService<DatabaseConnector,DatabaseConnectorFactory> getNetDeviceIDs() {
        return netDeviceIDs;
    }
    /* TODO
    public NetDeviceService<DatabaseConnector,DatabaseConnectorFactory> getNetDevices();

    public NetPortService<DatabaseConnector,DatabaseConnectorFactory> getNetPorts();
    */
    public NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> getNetProtocols() {
        return netProtocols;
    }
    /* TODO
    public NetTcpRedirectService<DatabaseConnector,DatabaseConnectorFactory> getNetTcpRedirects();

    public NoticeLogService<DatabaseConnector,DatabaseConnectorFactory> getNoticeLogs();

    public NoticeTypeService<DatabaseConnector,DatabaseConnectorFactory> getNoticeTypes();
    */
    public OperatingSystemVersionService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystemVersions() {
        return operatingSystemVersions;
    }

    public OperatingSystemService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystems() {
        return operatingSystems;
    }

    public PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> getPackageCategories() {
        return packageCategories;
    }
    /*
    public PackageDefinitionLimitService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitionLimits();

    public PackageDefinitionService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitions();

    public PaymentTypeService<DatabaseConnector,DatabaseConnectorFactory> getPaymentTypes();

    public PhysicalServerService<DatabaseConnector,DatabaseConnectorFactory> getPhysicalServers();

    public PostgresDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getPostgresDatabases();

    public PostgresEncodingService<DatabaseConnector,DatabaseConnectorFactory> getPostgresEncodings();

    public PostgresReservedWordService<DatabaseConnector,DatabaseConnectorFactory> getPostgresReservedWords();

    public PostgresServerUserService<DatabaseConnector,DatabaseConnectorFactory> getPostgresServerUsers();
    */
    public PostgresServerService<DatabaseConnector,DatabaseConnectorFactory> getPostgresServers() {
        return postgresServers;
    }

    // TODO: public PostgresUserService<DatabaseConnector,DatabaseConnectorFactory> getPostgresUsers();

    public PostgresVersionService<DatabaseConnector,DatabaseConnectorFactory> getPostgresVersions() {
        return postgresVersions;
    }

    // TODO: public PrivateFTPServerService<DatabaseConnector,DatabaseConnectorFactory> getPrivateFTPServers();

    // TODO: public ProcessorTypeService<DatabaseConnector,DatabaseConnectorFactory> getProcessorTypes();

    public ProtocolService<DatabaseConnector,DatabaseConnectorFactory> getProtocols() {
        return protocols;
    }
    /* TODO
    public RackService<DatabaseConnector,DatabaseConnectorFactory> getRacks();

    public ResellerService<DatabaseConnector,DatabaseConnectorFactory> getResellers();
    */
    public ResourceTypeService<DatabaseConnector,DatabaseConnectorFactory> getResourceTypes() {
        return resourceTypes;
    }

    public ResourceService<DatabaseConnector,DatabaseConnectorFactory> getResources() {
        return resources;
    }

    public ServerFarmService<DatabaseConnector,DatabaseConnectorFactory> getServerFarms() {
        return serverFarms;
    }

    public ServerService<DatabaseConnector,DatabaseConnectorFactory> getServers() {
        return servers;
    }
    /* TODO
    public ShellService<DatabaseConnector,DatabaseConnectorFactory> getShells();

    public SignupRequestOptionService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequestOptions();

    public SignupRequestService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequests();

    public SpamEmailMessageService<DatabaseConnector,DatabaseConnectorFactory> getSpamEmailMessages();

    public SystemEmailAliasService<DatabaseConnector,DatabaseConnectorFactory> getSystemEmailAliases();
    */
    public TechnologyService<DatabaseConnector,DatabaseConnectorFactory> getTechnologies() {
        return technologies;
    }

    public TechnologyClassService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyClasses() {
        return technologyClasses;
    }

    public TechnologyNameService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyNames() {
        return technologyNames;
    }

    public TechnologyVersionService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyVersions() {
        return technologyVersions;
    }
    /* TODO
    public TicketActionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTicketActionTypes();

    public TicketActionService<DatabaseConnector,DatabaseConnectorFactory> getTicketActions();

    public TicketAssignmentService<DatabaseConnector,DatabaseConnectorFactory> getTicketAssignments();

    public TicketBrandCategoryService<DatabaseConnector,DatabaseConnectorFactory> getTicketBrandCategories();
    */
    public TicketCategoryService<DatabaseConnector,DatabaseConnectorFactory> getTicketCategories() {
        return ticketCategories;
    }

    public TicketPriorityService<DatabaseConnector,DatabaseConnectorFactory> getTicketPriorities() {
        return ticketPriorities;
    }

    public TicketStatusService<DatabaseConnector,DatabaseConnectorFactory> getTicketStatuses() {
        return ticketStatuses;
    }

    public TicketTypeService<DatabaseConnector,DatabaseConnectorFactory> getTicketTypes() {
        return ticketTypes;
    }
    /* TODO
    public TicketService<DatabaseConnector,DatabaseConnectorFactory> getTickets();
    */
    public TimeZoneService<DatabaseConnector,DatabaseConnectorFactory> getTimeZones() {
        return timeZones;
    }
    /* TODO
    public TransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTransactionTypes();

    public TransactionService<DatabaseConnector,DatabaseConnectorFactory> getTransactions();

    public USStateService<DatabaseConnector,DatabaseConnectorFactory> getUsStates();
    */
    public UsernameService<DatabaseConnector,DatabaseConnectorFactory> getUsernames() {
        return usernames;
    }
    /* TODO

    public VirtualDiskService<DatabaseConnector,DatabaseConnectorFactory> getVirtualDisks();

    public VirtualServerService<DatabaseConnector,DatabaseConnectorFactory> getVirtualServers();

    public WhoisHistoryService<DatabaseConnector,DatabaseConnectorFactory> getWhoisHistory();
 */
}
