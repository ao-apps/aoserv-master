package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.AOServConnectorUtils;
import com.aoindustries.aoserv.client.AOServService;
import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.BusinessAdministratorService;
import com.aoindustries.aoserv.client.BusinessService;
import com.aoindustries.aoserv.client.DisableLogService;
import com.aoindustries.aoserv.client.LanguageService;
import com.aoindustries.aoserv.client.PackageCategoryService;
import com.aoindustries.aoserv.client.ResourceTypeService;
import com.aoindustries.aoserv.client.ServiceName;
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
    /* TODO
    final DatabaseAOServerDaemonHostService aoserverDaemonHosts;
    final DatabaseAOServerResourceService aoserverResources;
    final DatabaseAOServerService aoservers;
    final DatabaseAOServPermissionService aoservPermissions;
    final DatabaseAOServProtocolService aoservProtocols;
    final DatabaseAOSHCommandService aoshCommands;
    final DatabaseArchitectureService architectures;
    final DatabaseBackupPartitionService backupPartitions;
    final DatabaseBackupRetentionService backupRetentions;
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
    final DatabaseCountryCodeService countryCodes;
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
    final DatabaseFailoverFileReplicationService failoverFileReplications;
    final DatabaseFailoverFileScheduleService failoverFileSchedules;
    final DatabaseFailoverMySQLReplicationService failoverMySQLReplications;
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
    final DatabaseMySQLDatabaseService mysqlDatabases;
    final DatabaseMySQLDBUserService mysqlDBUsers;
    final DatabaseMySQLReservedWordService mysqlReservedWords;
    final DatabaseMySQLServerService mysqlServers;
    final DatabaseMySQLUserService mysqlUsers;
    final DatabaseNetBindService netBinds;
    final DatabaseNetDeviceIDService netDeviceIDs;
    final DatabaseNetDeviceService netDevices;
    final DatabaseNetPortService netPorts;
    final DatabaseNetProtocolService netProtocols;
    final DatabaseNetTcpRedirectService netTcpRedirects;
    final DatabaseNoticeLogService noticeLogs;
    final DatabaseNoticeTypeService noticeTypes;
    final DatabaseOperatingSystemVersionService operatingSystemVersions;
    final DatabaseOperatingSystemService operatingSystems;
    */
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
    final DatabasePostgresServerService postgresServers;
    final DatabasePostgresUserService postgresUsers;
    final DatabasePostgresVersionService postgresVersions;
    final DatabasePrivateFTPServerService privateFTPServers;
    final DatabaseProcessorTypeService processorTypes;
    final DatabaseProtocolService protocols;
    final DatabaseRackService racks;
    final DatabaseResellerService resellers;
     */
    final DatabaseResourceTypeService resourceTypes;
    /* TODO
    final DatabaseResourceService resources;
    final DatabaseServerFarmService serverFarms;
    final DatabaseServerService servers;
    final DatabaseShellService shells;
    final DatabaseSignupRequestOptionService signupRequestOptions;
    final DatabaseSignupRequestService signupRequests;
    final DatabaseSpamEmailMessageService spamEmailMessages;
    final DatabaseSystemEmailAliasService systemEmailAliass;
    final DatabaseTechnologyService technologies;
    final DatabaseTechnologyClassService technologyClasss;
    final DatabaseTechnologyNameService technologyNames;
    final DatabaseTechnologyVersionService technologyVersions;
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

    DatabaseConnector(DatabaseConnectorFactory factory, Locale locale, String connectAs) throws RemoteException, LoginException {
        this.factory = factory;
        this.locale = locale;
        this.connectAs = connectAs;
        /* TODO
        aoserverDaemonHosts = new DatabaseAOServerDaemonHostService(this);
        aoserverResources = new DatabaseAOServerResourceService(this);
        aoservers = new DatabaseAOServerService(this);
        aoservPermissions = new DatabaseAOServPermissionService(this);
        aoservProtocols = new DatabaseAOServProtocolService(this);
        aoshCommands = new DatabaseAOSHCommandService(this);
        architectures = new DatabaseArchitectureService(this);
        backupPartitions = new DatabaseBackupPartitionService(this);
        backupRetentions = new DatabaseBackupRetentionService(this);
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
        countryCodes = new DatabaseCountryCodeService(this);
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
        failoverFileReplications = new DatabaseFailoverFileReplicationService(this);
        failoverFileSchedules = new DatabaseFailoverFileScheduleService(this);
        failoverMySQLReplications = new DatabaseFailoverMySQLReplicationService(this);
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
        mysqlDatabases = new DatabaseMySQLDatabaseService(this);
        mysqlDBUsers = new DatabaseMySQLDBUserService(this);
        mysqlReservedWords = new DatabaseMySQLReservedWordService(this);
        mysqlServers = new DatabaseMySQLServerService(this);
        mysqlUsers = new DatabaseMySQLUserService(this);
        netBinds = new DatabaseNetBindService(this);
        netDeviceIDs = new DatabaseNetDeviceIDService(this);
        netDevices = new DatabaseNetDeviceService(this);
        netPorts = new DatabaseNetPortService(this);
        netProtocols = new DatabaseNetProtocolService(this);
        netTcpRedirects = new DatabaseNetTcpRedirectService(this);
        noticeLogs = new DatabaseNoticeLogService(this);
        noticeTypes = new DatabaseNoticeTypeService(this);
        operatingSystemVersions = new DatabaseOperatingSystemVersionService(this);
        operatingSystems = new DatabaseOperatingSystemService(this);
        */
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
        postgresServers = new DatabasePostgresServerService(this);
        postgresUsers = new DatabasePostgresUserService(this);
        postgresVersions = new DatabasePostgresVersionService(this);
        privateFTPServers = new DatabasePrivateFTPServerService(this);
        processorTypes = new DatabaseProcessorTypeService(this);
        protocols = new DatabaseProtocolService(this);
        racks = new DatabaseRackService(this);
        resellers = new DatabaseResellerService(this);
         */
        resourceTypes = new DatabaseResourceTypeService(this);
        /* TODO
        resources = new DatabaseResourceService(this);
        serverFarms = new DatabaseServerFarmService(this);
        servers = new DatabaseServerService(this);
        shells = new DatabaseShellService(this);
        signupRequestOptions = new DatabaseSignupRequestOptionService(this);
        signupRequests = new DatabaseSignupRequestService(this);
        spamEmailMessages = new DatabaseSpamEmailMessageService(this);
        systemEmailAliass = new DatabaseSystemEmailAliasService(this);
        technologies = new DatabaseTechnologyService(this);
        technologyClasss = new DatabaseTechnologyClassService(this);
        technologyNames = new DatabaseTechnologyNameService(this);
        technologyVersions = new DatabaseTechnologyVersionService(this);
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
    AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> getAoServerDaemonHosts();

    AOServerResourceService<DatabaseConnector,DatabaseConnectorFactory> getAoServerResources();

    AOServerService<DatabaseConnector,DatabaseConnectorFactory> getAoServers();

    AOServPermissionService<DatabaseConnector,DatabaseConnectorFactory> getAoservPermissions();

    AOServProtocolService<DatabaseConnector,DatabaseConnectorFactory> getAoservProtocols();

    AOSHCommandService<DatabaseConnector,DatabaseConnectorFactory> getAoshCommands();

    ArchitectureService<DatabaseConnector,DatabaseConnectorFactory> getArchitectures();

    BackupPartitionService<DatabaseConnector,DatabaseConnectorFactory> getBackupPartitions();

    BackupRetentionService<DatabaseConnector,DatabaseConnectorFactory> getBackupRetentions();

    BankAccountService<DatabaseConnector,DatabaseConnectorFactory> getBankAccounts();

    BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactionTypes();

    BankTransactionService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactions();

    BankService<DatabaseConnector,DatabaseConnectorFactory> getBanks();

    BlackholeEmailAddressService<DatabaseConnector,DatabaseConnectorFactory> getBlackholeEmailAddresses();

    BrandService<DatabaseConnector,DatabaseConnectorFactory> getBrands();
     */
    public BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministrators() {
        return businessAdministrators;
    }
    /*
    BusinessAdministratorPermissionService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministratorPermissions();

    BusinessProfileService<DatabaseConnector,DatabaseConnectorFactory> getBusinessProfiles();
     */
    public BusinessService<DatabaseConnector,DatabaseConnectorFactory> getBusinesses() {
        return businesses;
    }

    /*
    BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> getBusinessServers();

    ClientJvmProfileService<DatabaseConnector,DatabaseConnectorFactory> getClientJvmProfiles();

    CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> getCountryCodes();

    CreditCardProcessorService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardProcessors();

    CreditCardTransactionService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardTransactions();

    CreditCardService<DatabaseConnector,DatabaseConnectorFactory> getCreditCards();

    CvsRepositoryService<DatabaseConnector,DatabaseConnectorFactory> getCvsRepositories();
     */
    public DisableLogService<DatabaseConnector,DatabaseConnectorFactory> getDisableLogs() {
        return disableLogs;
    }
    /*
    DistroFileTypeService<DatabaseConnector,DatabaseConnectorFactory> getDistroFileTypes();

    DistroFileService<DatabaseConnector,DatabaseConnectorFactory> getDistroFiles();

    DNSForbiddenZoneService<DatabaseConnector,DatabaseConnectorFactory> getDnsForbiddenZones();

    DNSRecordService<DatabaseConnector,DatabaseConnectorFactory> getDnsRecords();

    DNSTLDService<DatabaseConnector,DatabaseConnectorFactory> getDnsTLDs();

    DNSTypeService<DatabaseConnector,DatabaseConnectorFactory> getDnsTypes();

    DNSZoneService<DatabaseConnector,DatabaseConnectorFactory> getDnsZones();

    EmailAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailAddresses();

    EmailAttachmentBlockService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentBlocks();

    EmailAttachmentTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentTypes();

    EmailDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailDomains();

    EmailForwardingService<DatabaseConnector,DatabaseConnectorFactory> getEmailForwardings();

    EmailListAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailListAddresses();

    EmailListService<DatabaseConnector,DatabaseConnectorFactory> getEmailLists();

    EmailPipeAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipeAddresses();

    EmailPipeService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipes();

    EmailSmtpRelayTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelayTypes();

    EmailSmtpRelayService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelays();

    EmailSmtpSmartHostDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHostDomains();

    EmailSmtpSmartHostService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHosts();

    EmailSpamAssassinIntegrationModeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSpamAssassinIntegrationModes();

    EncryptionKeyService<DatabaseConnector,DatabaseConnectorFactory> getEncryptionKeys();

    ExpenseCategoryService<DatabaseConnector,DatabaseConnectorFactory> getExpenseCategories();

    FailoverFileLogService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileLogs();

    FailoverFileReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileReplications();

    FailoverFileScheduleService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileSchedules();

    FailoverMySQLReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverMySQLReplications();

    FileBackupSettingService<DatabaseConnector,DatabaseConnectorFactory> getFileBackupSettings();

    FTPGuestUserService<DatabaseConnector,DatabaseConnectorFactory> getFtpGuestUsers();

    HttpdBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdBinds();

    HttpdJBossSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossSites();

    HttpdJBossVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossVersions();

    HttpdJKCodeService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKCodes();

    HttpdJKProtocolService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKProtocols();

    HttpdServerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdServers();

    HttpdSharedTomcatService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSharedTomcats();

    HttpdSiteAuthenticatedLocationService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteAuthenticatedLocations();

    HttpdSiteBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteBinds();

    HttpdSiteURLService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteURLs();

    HttpdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSites();

    HttpdStaticSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdStaticSites();

    HttpdTomcatContextService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatContexts();

    HttpdTomcatDataSourceService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatDataSources();

    HttpdTomcatParameterService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatParameters();

    HttpdTomcatSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSites();

    HttpdTomcatSharedSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSharedSites();

    HttpdTomcatStdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatStdSites();

    HttpdTomcatVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatVersions();

    HttpdWorkerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdWorkers();

    IPAddressService<DatabaseConnector,DatabaseConnectorFactory> getIpAddresses();
    */
    public LanguageService<DatabaseConnector,DatabaseConnectorFactory> getLanguages() {
        return languages;
    }
    /* TODO
    LinuxAccAddressService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccAddresses();

    LinuxAccountTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccountTypes();

    LinuxAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccounts();

    LinuxGroupAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroupAccounts();

    LinuxGroupTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroupTypes();

    LinuxGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroups();

    LinuxIDService<DatabaseConnector,DatabaseConnectorFactory> getLinuxIDs();

    LinuxServerAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxServerAccounts();

    LinuxServerGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxServerGroups();

    MajordomoListService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoLists();

    MajordomoServerService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoServers();

    MajordomoVersionService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoVersions();

    MasterHistoryService<DatabaseConnector,DatabaseConnectorFactory> getMasterHistory();

    MasterHostService<DatabaseConnector,DatabaseConnectorFactory> getMasterHosts();

    MasterServerService<DatabaseConnector,DatabaseConnectorFactory> getMasterServers();

    MasterUserService<DatabaseConnector,DatabaseConnectorFactory> getMasterUsers();

    MonthlyChargeService<DatabaseConnector,DatabaseConnectorFactory> getMonthlyCharges();

    MySQLDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDatabases();

    MySQLDBUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDBUsers();

    MySQLReservedWordService<DatabaseConnector,DatabaseConnectorFactory> getMysqlReservedWords();

    MySQLServerService<DatabaseConnector,DatabaseConnectorFactory> getMysqlServers();

    MySQLUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlUsers();

    NetBindService<DatabaseConnector,DatabaseConnectorFactory> getNetBinds();

    NetDeviceIDService<DatabaseConnector,DatabaseConnectorFactory> getNetDeviceIDs();

    NetDeviceService<DatabaseConnector,DatabaseConnectorFactory> getNetDevices();

    NetPortService<DatabaseConnector,DatabaseConnectorFactory> getNetPorts();

    NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> getNetProtocols();

    NetTcpRedirectService<DatabaseConnector,DatabaseConnectorFactory> getNetTcpRedirects();

    NoticeLogService<DatabaseConnector,DatabaseConnectorFactory> getNoticeLogs();

    NoticeTypeService<DatabaseConnector,DatabaseConnectorFactory> getNoticeTypes();

    OperatingSystemVersionService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystemVersions();

    OperatingSystemService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystems();
    */
    public PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> getPackageCategories() {
        return packageCategories;
    }
    /*
    PackageDefinitionLimitService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitionLimits();

    PackageDefinitionService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitions();

    PaymentTypeService<DatabaseConnector,DatabaseConnectorFactory> getPaymentTypes();

    PhysicalServerService<DatabaseConnector,DatabaseConnectorFactory> getPhysicalServers();

    PostgresDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getPostgresDatabases();

    PostgresEncodingService<DatabaseConnector,DatabaseConnectorFactory> getPostgresEncodings();

    PostgresReservedWordService<DatabaseConnector,DatabaseConnectorFactory> getPostgresReservedWords();

    PostgresServerUserService<DatabaseConnector,DatabaseConnectorFactory> getPostgresServerUsers();

    PostgresServerService<DatabaseConnector,DatabaseConnectorFactory> getPostgresServers();

    PostgresUserService<DatabaseConnector,DatabaseConnectorFactory> getPostgresUsers();

    PostgresVersionService<DatabaseConnector,DatabaseConnectorFactory> getPostgresVersions();

    PrivateFTPServerService<DatabaseConnector,DatabaseConnectorFactory> getPrivateFTPServers();

    ProcessorTypeService<DatabaseConnector,DatabaseConnectorFactory> getProcessorTypes();

    ProtocolService<DatabaseConnector,DatabaseConnectorFactory> getProtocols();

    RackService<DatabaseConnector,DatabaseConnectorFactory> getRacks();

    ResellerService<DatabaseConnector,DatabaseConnectorFactory> getResellers();
*/
    public ResourceTypeService<DatabaseConnector,DatabaseConnectorFactory> getResourceTypes() {
        return resourceTypes;
    }
/* TODO
    ResourceService<DatabaseConnector,DatabaseConnectorFactory> getResources();

    ServerFarmService<DatabaseConnector,DatabaseConnectorFactory> getServerFarms();

    ServerTable getServers();

    ShellService<DatabaseConnector,DatabaseConnectorFactory> getShells();

    SignupRequestOptionService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequestOptions();

    SignupRequestService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequests();

    SpamEmailMessageService<DatabaseConnector,DatabaseConnectorFactory> getSpamEmailMessages();

    SystemEmailAliasService<DatabaseConnector,DatabaseConnectorFactory> getSystemEmailAliases();

    TechnologyService<DatabaseConnector,DatabaseConnectorFactory> getTechnologies();

    TechnologyClassService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyClasses();

    TechnologyNameService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyNames();

    TechnologyVersionService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyVersions();

    TicketActionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTicketActionTypes();

    TicketActionService<DatabaseConnector,DatabaseConnectorFactory> getTicketActions();

    TicketAssignmentService<DatabaseConnector,DatabaseConnectorFactory> getTicketAssignments();

    TicketBrandCategoryService<DatabaseConnector,DatabaseConnectorFactory> getTicketBrandCategories();
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
    TicketService<DatabaseConnector,DatabaseConnectorFactory> getTickets();
    */
    public TimeZoneService<DatabaseConnector,DatabaseConnectorFactory> getTimeZones() {
        return timeZones;
    }
    /* TODO
    TransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTransactionTypes();

    TransactionService<DatabaseConnector,DatabaseConnectorFactory> getTransactions();

    USStateService<DatabaseConnector,DatabaseConnectorFactory> getUsStates();
    */
    public UsernameService<DatabaseConnector,DatabaseConnectorFactory> getUsernames() {
        return usernames;
    }
    /* TODO

    VirtualDiskService<DatabaseConnector,DatabaseConnectorFactory> getVirtualDisks();

    VirtualServerService<DatabaseConnector,DatabaseConnectorFactory> getVirtualServers();

    WhoisHistoryService<DatabaseConnector,DatabaseConnectorFactory> getWhoisHistory();
 */
}
