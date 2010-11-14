/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.command.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseCallable;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.WrappedException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    final UserId connectAs;
    private final UserId authenticateAs;
    private final String password;
    final DatabaseAOServerDaemonHostService aoserverDaemonHosts;
    final DatabaseAOServerResourceService aoserverResources;
    final DatabaseAOServerService aoservers;
    final DatabaseAOServPermissionService aoservPermissions;
    final DatabaseAOServRoleService aoservRoles;
    final DatabaseAOServRolePermissionService aoservRolePermissions;
    final DatabaseArchitectureService architectures;
    final DatabaseBackupPartitionService backupPartitions;
    final DatabaseBackupRetentionService backupRetentions;
    // TODO: final DatabaseBankAccountService bankAccounts;
    final DatabaseBankTransactionTypeService bankTransactionTypes;
    // TODO: final DatabaseBankTransactionService bankTransactions;
    // TODO: final DatabaseBankService banks;
    final DatabaseBrandService brands;
    final DatabaseBusinessAdministratorService businessAdministrators;
    final DatabaseBusinessAdministratorRoleService businessAdministratorRoles;
    final DatabaseBusinessProfileService businessProfiles;
    final DatabaseBusinessService businesses;
    final DatabaseBusinessServerService businessServers;
    final DatabaseCountryCodeService countryCodes;
    final DatabaseCreditCardProcessorService creditCardProcessors;
    final DatabaseCreditCardTransactionService creditCardTransactions;
    final DatabaseCreditCardService creditCards;
    final DatabaseCvsRepositoryService cvsRepositories;
    final DatabaseDisableLogService disableLogs;
    /* TODO
    final DatabaseDistroFileTypeService distroFileTypes;
    final DatabaseDistroFileService distroFiles;
     */
    final DatabaseDnsRecordService dnsRecords;
    final DatabaseDnsTldService dnsTlds;
    final DatabaseDnsTypeService dnsTypes;
    final DatabaseDnsZoneService dnsZones;
    // TODO: final DatabaseEmailAddressService emailAddresss;
    // TODO: final DatabaseEmailAttachmentBlockService emailAttachmentBlocks;
    final DatabaseEmailAttachmentTypeService emailAttachmentTypes;
    // TODO: final DatabaseEmailDomainService emailDomains;
    // TODO: final DatabaseEmailForwardingService emailForwardings;
    final DatabaseEmailInboxService emailInboxes;
    /* TODO
    final DatabaseEmailListAddressService emailListAddresss;
    final DatabaseEmailListService emailLists;
    final DatabaseEmailPipeAddressService emailPipeAddresss;
    final DatabaseEmailPipeService emailPipes;
     */
    final DatabaseEmailSmtpRelayTypeService emailSmtpRelayTypes;
    // TODO: final DatabaseEmailSmtpRelayService emailSmtpRelays;
    // TODO: final DatabaseEmailSmtpSmartHostDomainService emailSmtpSmartHostDomains;
    // TODO: final DatabaseEmailSmtpSmartHostService emailSmtpSmartHosts;
    final DatabaseEmailSpamAssassinIntegrationModeService emailSpamAssassinIntegrationModes;
    // TODO: final DatabaseEncryptionKeyService encryptionKeys;
    final DatabaseExpenseCategoryService expenseCategories;
    final DatabaseFailoverFileLogService failoverFileLogs;
    final DatabaseFailoverFileReplicationService failoverFileReplications;
    final DatabaseFailoverFileScheduleService failoverFileSchedules;
    final DatabaseFailoverMySQLReplicationService failoverMySQLReplications;
    final DatabaseFileBackupSettingService fileBackupSettings;
    final DatabaseFtpGuestUserService ftpGuestUsers;
    final DatabaseGroupNameService groupNames;
    // TODO: final DatabaseHttpdBindService httpdBinds;
    // TODO: final DatabaseHttpdJBossSiteService httpdJBossSites;
    final DatabaseHttpdJBossVersionService httpdJBossVersions;
    final DatabaseHttpdJKCodeService httpdJKCodes;
    final DatabaseHttpdJKProtocolService httpdJKProtocols;
    final DatabaseHttpdServerService httpdServers;
    /* TODO
    final DatabaseHttpdSharedTomcatService httpdSharedTomcats;
    final DatabaseHttpdSiteAuthenticatedLocationService httpdSiteAuthenticatedLocations;
    final DatabaseHttpdSiteBindService httpdSiteBinds;
    final DatabaseHttpdSiteURLService httpdSiteURLs;
     */
    final DatabaseHttpdSiteService httpdSites;
    // TODO: final DatabaseHttpdStaticSiteService httpdStaticSites;
    // TODO: final DatabaseHttpdTomcatContextService httpdTomcatContexts;
    // TODO: final DatabaseHttpdTomcatDataSourceService httpdTomcatDataSources;
    // TODO: final DatabaseHttpdTomcatParameterService httpdTomcatParameters;
    // TODO: final DatabaseHttpdTomcatSiteService httpdTomcatSites;
    // TODO: final DatabaseHttpdTomcatSharedSiteService httpdTomcatSharedSites;
    // TODO: final DatabaseHttpdTomcatStdSiteService httpdTomcatStdSites;
    final DatabaseHttpdTomcatVersionService httpdTomcatVersions;
    // TODO: final DatabaseHttpdWorkerService httpdWorkers;
    final DatabaseIPAddressService ipAddresses;
    final DatabaseLanguageService languages;
    // TODO: final DatabaseLinuxAccAddressService linuxAccAddresss;
    final DatabaseLinuxAccountGroupService linuxAccountGroups;
    final DatabaseLinuxAccountTypeService linuxAccountTypes;
    final DatabaseLinuxAccountService linuxAccounts;
    final DatabaseLinuxGroupTypeService linuxGroupTypes;
    final DatabaseLinuxGroupService linuxGroups;
    // TODO: final DatabaseMajordomoListService majordomoLists;
    // TODO: final DatabaseMajordomoServerService majordomoServers;
    final DatabaseMajordomoVersionService majordomoVersions;
    final DatabaseMasterHostService masterHosts;
    final DatabaseMasterServerService masterServers;
    final DatabaseMasterUserService masterUsers;
    // TODO: final DatabaseMonthlyChargeService monthlyCharges;
    final DatabaseMySQLDatabaseService mysqlDatabases;
    final DatabaseMySQLDBUserService mysqlDBUsers;
    final DatabaseMySQLServerService mysqlServers;
    final DatabaseMySQLUserService mysqlUsers;
    final DatabaseNetBindService netBinds;
    final DatabaseNetDeviceIDService netDeviceIDs;
    final DatabaseNetDeviceService netDevices;
    final DatabaseNetProtocolService netProtocols;
    final DatabaseNetTcpRedirectService netTcpRedirects;
    // TODO: final DatabaseNoticeLogService noticeLogs;
    final DatabaseNoticeTypeService noticeTypes;
    final DatabaseOperatingSystemVersionService operatingSystemVersions;
    final DatabaseOperatingSystemService operatingSystems;
    final DatabasePackageCategoryService packageCategories;
    final DatabasePackageDefinitionBusinessService packageDefinitionBusinesses;
    final DatabasePackageDefinitionLimitService packageDefinitionLimits;
    final DatabasePackageDefinitionService packageDefinitions;
    final DatabasePaymentTypeService paymentTypes;
    // TODO: final DatabasePhysicalServerService physicalServers;
    final DatabasePostgresDatabaseService postgresDatabases;
    final DatabasePostgresEncodingService postgresEncodings;
    final DatabasePostgresServerService postgresServers;
    final DatabasePostgresUserService postgresUsers;
    final DatabasePostgresVersionService postgresVersions;
    final DatabasePrivateFtpServerService privateFtpServers;
    final DatabaseProcessorTypeService processorTypes;
    final DatabaseProtocolService protocols;
    /* TODO
    final DatabaseRackService racks;
     */
    final DatabaseResellerService resellers;
    final DatabaseResourceTypeService resourceTypes;
    final DatabaseResourceService resources;
    final DatabaseServerFarmService serverFarms;
    final DatabaseServerResourceService serverResources;
    final DatabaseServerService servers;
    final DatabaseShellService shells;
    /* TODO
    final DatabaseSignupRequestOptionService signupRequestOptions;
    final DatabaseSignupRequestService signupRequests;
    final DatabaseSpamEmailMessageService spamEmailMessages;
    final DatabaseSystemEmailAliasService systemEmailAliass;
     */
    final DatabaseTechnologyService technologies;
    final DatabaseTechnologyClassService technologyClasses;
    final DatabaseTechnologyNameService technologyNames;
    final DatabaseTechnologyVersionService technologyVersions;
    final DatabaseTicketActionTypeService ticketActionTypes;
    final DatabaseTicketActionService ticketActions;
    final DatabaseTicketAssignmentService ticketAssignments;
    // TODO: final DatabaseTicketBrandCategoryService ticketBrandCategories;
    final DatabaseTicketCategoryService ticketCategories;
    final DatabaseTicketPriorityService ticketPriorities;
    final DatabaseTicketStatusService ticketStatuses;
    final DatabaseTicketTypeService ticketTypes;
    final DatabaseTicketService tickets;
    final DatabaseTimeZoneService timeZones;
    final DatabaseTransactionTypeService transactionTypes;
    final DatabaseTransactionService transactions;
    // TODO: final DatabaseUSStateService usStates;
    final DatabaseUsernameService usernames;
    // TODO: final DatabaseVirtualDiskService virtualDisks;
    DatabaseVirtualServerService virtualServers;
    // TODO: final DatabaseWhoisHistoryService whoisHistories;

    DatabaseConnector(DatabaseConnectorFactory factory, Locale locale, UserId connectAs, UserId authenticateAs, String password) {
        this.factory = factory;
        this.locale = locale;
        this.connectAs = connectAs;
        this.authenticateAs = authenticateAs;
        this.password = password;
        aoserverDaemonHosts = new DatabaseAOServerDaemonHostService(this);
        aoserverResources = new DatabaseAOServerResourceService(this);
        aoservers = new DatabaseAOServerService(this);
        aoservPermissions = new DatabaseAOServPermissionService(this);
        aoservRoles = new DatabaseAOServRoleService(this);
        aoservRolePermissions = new DatabaseAOServRolePermissionService(this);
        architectures = new DatabaseArchitectureService(this);
        backupPartitions = new DatabaseBackupPartitionService(this);
        backupRetentions = new DatabaseBackupRetentionService(this);
        // TODO: bankAccounts = new DatabaseBankAccountService(this);
        bankTransactionTypes = new DatabaseBankTransactionTypeService(this);
        // TODO: bankTransactions = new DatabaseBankTransactionService(this);
        // TODO: banks = new DatabaseBankService(this);
        brands = new DatabaseBrandService(this);
        businessAdministrators = new DatabaseBusinessAdministratorService(this);
        businessAdministratorRoles = new DatabaseBusinessAdministratorRoleService(this);
        businessProfiles = new DatabaseBusinessProfileService(this);
        businesses = new DatabaseBusinessService(this);
        businessServers = new DatabaseBusinessServerService(this);
        countryCodes = new DatabaseCountryCodeService(this);
        creditCardProcessors = new DatabaseCreditCardProcessorService(this);
        creditCardTransactions = new DatabaseCreditCardTransactionService(this);
        creditCards = new DatabaseCreditCardService(this);
        cvsRepositories = new DatabaseCvsRepositoryService(this);
        disableLogs = new DatabaseDisableLogService(this);
        /* TODO
        distroFileTypes = new DatabaseDistroFileTypeService(this);
        distroFiles = new DatabaseDistroFileService(this);
         */
        dnsRecords = new DatabaseDnsRecordService(this);
        dnsTlds = new DatabaseDnsTldService(this);
        dnsTypes = new DatabaseDnsTypeService(this);
        dnsZones = new DatabaseDnsZoneService(this);
        // TODO: emailAddresss = new DatabaseEmailAddressService(this);
        // TODO: emailAttachmentBlocks = new DatabaseEmailAttachmentBlockService(this);
        emailAttachmentTypes = new DatabaseEmailAttachmentTypeService(this);
        // TODO: emailDomains = new DatabaseEmailDomainService(this);
        // TODO: emailForwardings = new DatabaseEmailForwardingService(this);
        emailInboxes = new DatabaseEmailInboxService(this);
        /* TODO
        emailListAddresss = new DatabaseEmailListAddressService(this);
        emailLists = new DatabaseEmailListService(this);
        emailPipeAddresss = new DatabaseEmailPipeAddressService(this);
        emailPipes = new DatabaseEmailPipeService(this);
         */
        emailSmtpRelayTypes = new DatabaseEmailSmtpRelayTypeService(this);
        // TODO: emailSmtpRelays = new DatabaseEmailSmtpRelayService(this);
        // TODO: emailSmtpSmartHostDomains = new DatabaseEmailSmtpSmartHostDomainService(this);
        // TODO: emailSmtpSmartHosts = new DatabaseEmailSmtpSmartHostService(this);
        emailSpamAssassinIntegrationModes = new DatabaseEmailSpamAssassinIntegrationModeService(this);
        // TODO: encryptionKeys = new DatabaseEncryptionKeyService(this);
        expenseCategories = new DatabaseExpenseCategoryService(this);
        failoverFileLogs = new DatabaseFailoverFileLogService(this);
        failoverFileReplications = new DatabaseFailoverFileReplicationService(this);
        failoverFileSchedules = new DatabaseFailoverFileScheduleService(this);
        failoverMySQLReplications = new DatabaseFailoverMySQLReplicationService(this);
        fileBackupSettings = new DatabaseFileBackupSettingService(this);
        ftpGuestUsers = new DatabaseFtpGuestUserService(this);
        groupNames = new DatabaseGroupNameService(this);
        // TODO: httpdBinds = new DatabaseHttpdBindService(this);
        // TODO: httpdJBossSites = new DatabaseHttpdJBossSiteService(this);
        httpdJBossVersions = new DatabaseHttpdJBossVersionService(this);
        httpdJKCodes = new DatabaseHttpdJKCodeService(this);
        httpdJKProtocols = new DatabaseHttpdJKProtocolService(this);
        httpdServers = new DatabaseHttpdServerService(this);
        /* TODO
        httpdSharedTomcats = new DatabaseHttpdSharedTomcatService(this);
        httpdSiteAuthenticatedLocations = new DatabaseHttpdSiteAuthenticatedLocationService(this);
        httpdSiteBinds = new DatabaseHttpdSiteBindService(this);
        httpdSiteURLs = new DatabaseHttpdSiteURLService(this);
         */
        httpdSites = new DatabaseHttpdSiteService(this);
        // TODO: httpdStaticSites = new DatabaseHttpdStaticSiteService(this);
        // TODO: httpdTomcatContexts = new DatabaseHttpdTomcatContextService(this);
        // TODO: httpdTomcatDataSources = new DatabaseHttpdTomcatDataSourceService(this);
        // TODO: httpdTomcatParameters = new DatabaseHttpdTomcatParameterService(this);
        // TODO: httpdTomcatSites = new DatabaseHttpdTomcatSiteService(this);
        // TODO: httpdTomcatSharedSites = new DatabaseHttpdTomcatSharedSiteService(this);
        // TODO: httpdTomcatStdSites = new DatabaseHttpdTomcatStdSiteService(this);
        httpdTomcatVersions = new DatabaseHttpdTomcatVersionService(this);
        // TODO: httpdWorkers = new DatabaseHttpdWorkerService(this);
        ipAddresses = new DatabaseIPAddressService(this);
        languages = new DatabaseLanguageService(this);
        // TODO: linuxAccAddresss = new DatabaseLinuxAccAddressService(this);
        linuxAccountGroups = new DatabaseLinuxAccountGroupService(this);
        linuxAccountTypes = new DatabaseLinuxAccountTypeService(this);
        linuxAccounts = new DatabaseLinuxAccountService(this);
        linuxGroupTypes = new DatabaseLinuxGroupTypeService(this);
        linuxGroups = new DatabaseLinuxGroupService(this);
        // TODO: majordomoLists = new DatabaseMajordomoListService(this);
        // TODO: majordomoServers = new DatabaseMajordomoServerService(this);
        majordomoVersions = new DatabaseMajordomoVersionService(this);
        masterHosts = new DatabaseMasterHostService(this);
        masterServers = new DatabaseMasterServerService(this);
        masterUsers = new DatabaseMasterUserService(this);
        // TODO: monthlyCharges = new DatabaseMonthlyChargeService(this);
        mysqlDatabases = new DatabaseMySQLDatabaseService(this);
        mysqlDBUsers = new DatabaseMySQLDBUserService(this);
        mysqlServers = new DatabaseMySQLServerService(this);
        mysqlUsers = new DatabaseMySQLUserService(this);
        netBinds = new DatabaseNetBindService(this);
        netDeviceIDs = new DatabaseNetDeviceIDService(this);
        netDevices = new DatabaseNetDeviceService(this);
        netProtocols = new DatabaseNetProtocolService(this);
        netTcpRedirects = new DatabaseNetTcpRedirectService(this);
        // TODO: noticeLogs = new DatabaseNoticeLogService(this);
        noticeTypes = new DatabaseNoticeTypeService(this);
        operatingSystemVersions = new DatabaseOperatingSystemVersionService(this);
        operatingSystems = new DatabaseOperatingSystemService(this);
        packageCategories = new DatabasePackageCategoryService(this);
        packageDefinitionBusinesses = new DatabasePackageDefinitionBusinessService(this);
        packageDefinitionLimits = new DatabasePackageDefinitionLimitService(this);
        packageDefinitions = new DatabasePackageDefinitionService(this);
        paymentTypes = new DatabasePaymentTypeService(this);
        // TODO: physicalServers = new DatabasePhysicalServerService(this);
        postgresDatabases = new DatabasePostgresDatabaseService(this);
        postgresEncodings = new DatabasePostgresEncodingService(this);
        postgresServers = new DatabasePostgresServerService(this);
        postgresUsers = new DatabasePostgresUserService(this);
        postgresVersions = new DatabasePostgresVersionService(this);
        privateFtpServers = new DatabasePrivateFtpServerService(this);
        processorTypes = new DatabaseProcessorTypeService(this);
        protocols = new DatabaseProtocolService(this);
        /* TODO
        racks = new DatabaseRackService(this);
         */
        resellers = new DatabaseResellerService(this);
        resourceTypes = new DatabaseResourceTypeService(this);
        resources = new DatabaseResourceService(this);
        serverFarms = new DatabaseServerFarmService(this);
        serverResources = new DatabaseServerResourceService(this);
        servers = new DatabaseServerService(this);
        shells = new DatabaseShellService(this);
        /* TODO
        signupRequestOptions = new DatabaseSignupRequestOptionService(this);
        signupRequests = new DatabaseSignupRequestService(this);
        spamEmailMessages = new DatabaseSpamEmailMessageService(this);
        systemEmailAliass = new DatabaseSystemEmailAliasService(this);
         */
        technologies = new DatabaseTechnologyService(this);
        technologyClasses = new DatabaseTechnologyClassService(this);
        technologyNames = new DatabaseTechnologyNameService(this);
        technologyVersions = new DatabaseTechnologyVersionService(this);
        ticketActionTypes = new DatabaseTicketActionTypeService(this);
        ticketActions = new DatabaseTicketActionService(this);
        ticketAssignments = new DatabaseTicketAssignmentService(this);
        // TODO: ticketBrandCategories = new DatabaseTicketBrandCategoryService(this);
        ticketCategories = new DatabaseTicketCategoryService(this);
        ticketPriorities = new DatabaseTicketPriorityService(this);
        ticketStatuses = new DatabaseTicketStatusService(this);
        ticketTypes = new DatabaseTicketTypeService(this);
        tickets = new DatabaseTicketService(this);
        timeZones = new DatabaseTimeZoneService(this);
        transactionTypes = new DatabaseTransactionTypeService(this);
        transactions = new DatabaseTransactionService(this);
        // TODO: usStates = new DatabaseUSStateService(this);
        usernames = new DatabaseUsernameService(this);
        // TODO: virtualDisks = new DatabaseVirtualDiskService(this);
        virtualServers = new DatabaseVirtualServerService(this);
        // TODO: whoisHistories = new DatabaseWhoisHistoryService(this);
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
    AccountType getAccountType(DatabaseConnection db) throws SQLException {
        if(factory.isEnabledMasterUser(db, connectAs)) return AccountType.MASTER;
        if(factory.isEnabledDaemonUser(db, connectAs)) return AccountType.DAEMON;
        if(factory.isEnabledBusinessAdministrator(db, connectAs)) return AccountType.BUSINESS;
        return AccountType.DISABLED;
    }

    @Override
    public DatabaseConnectorFactory getFactory() {
        return factory;
    }

    @Override
    public boolean isAoServObjectConnectorSettable() {
        return true;
    }

    @Override
    public Locale getLocale() {
        return locale;
    }

    @Override
    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    @Override
    public UserId getConnectAs() {
        return connectAs;
    }

    @Override
    public BusinessAdministrator getThisBusinessAdministrator() throws RemoteException {
        return getBusinessAdministrators().get(connectAs);
    }

    @Override
    public UserId getAuthenticateAs() {
        return authenticateAs;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public <R> CommandResult<R> executeCommand(final RemoteCommand<R> remoteCommand, final boolean isInteractive) throws RemoteException {
        try {
            // Make sure not accidentally running command on root user
            if(
                authenticateAs.equals(factory.rootConnector.getAuthenticateAs())
                || connectAs.equals(factory.rootConnector.getConnectAs())
            ) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnector.executeCommand.refusingRootConnector", remoteCommand.getCommandName()));

            final InvalidateSet invalidateSet = InvalidateSet.getInstance();
            try {
                R result = factory.database.executeTransaction(
                    new DatabaseCallable<R>() {
                        @Override
                        public R call(DatabaseConnection db) throws SQLException {
                            try {
                                // Make sure current user is enabled
                                if(!factory.isEnabledBusinessAdministrator(db, authenticateAs)) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));
                                if(!factory.isEnabledBusinessAdministrator(db, connectAs)) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));

                                // Check permissions using root connector
                                Set<AOServPermission.Permission> permissions = remoteCommand.getCommandName().getPermissions();
                                BusinessAdministrator rootBa = factory.rootConnector.getBusinessAdministrators().get(connectAs);
                                if(!rootBa.hasPermissions(permissions)) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnector.executeCommand.permissionDenied", remoteCommand.getCommandName()));

                                // Validate command using root connector
                                Map<String,List<String>> errors = remoteCommand.validate(factory.rootConnector);
                                if(!errors.isEmpty()) throw new CommandValidationException(remoteCommand, errors);

                                // Execute command using this connector
                                switch(remoteCommand.getCommandName()) {
                                    // <editor-fold defaultstate="collapsed" desc="Business Administrators">
                                    case set_business_administrator_password : {
                                        SetBusinessAdministratorPasswordCommand command = (SetBusinessAdministratorPasswordCommand)remoteCommand;
                                        businessAdministrators.setBusinessAdministratorPassword(db, invalidateSet, command.getUsername(), command.getPlaintext());
                                        return null;
                                    }
                                    // </editor-fold>
                                    // <editor-fold defaultstate="collapsed" desc="Businesses">
                                    case cancel_business : {
                                        CancelBusinessCommand command = (CancelBusinessCommand)remoteCommand;
                                        businesses.cancelBusiness(db, invalidateSet, command.getAccounting(), command.getCancelReason());
                                        return null;
                                    }
                                    // </editor-fold>
                                    // <editor-fold defaultstate="collapsed" desc="Linux Accounts">
                                    case set_linux_account_password : {
                                        SetLinuxAccountPasswordCommand command = (SetLinuxAccountPasswordCommand)remoteCommand;
                                        linuxAccounts.setLinuxAccountPassword(db, invalidateSet, command.getLinuxAccount(), command.getPlaintext());
                                        return null;
                                    }
                                    // </editor-fold>
                                    // <editor-fold defaultstate="collapsed" desc="MySQL Users">
                                    case set_mysql_user_password : {
                                        SetMySQLUserPasswordCommand command = (SetMySQLUserPasswordCommand)remoteCommand;
                                        mysqlUsers.setMySQLUserPassword(db, invalidateSet, command.getMysqlUser(), command.getPlaintext());
                                        return null;
                                    }
                                    // </editor-fold>
                                    // <editor-fold defaultstate="collapsed" desc="Postgres Users">
                                    case set_postgres_user_password : {
                                        SetPostgresUserPasswordCommand command = (SetPostgresUserPasswordCommand)remoteCommand;
                                        postgresUsers.setPostgresUserPassword(db, invalidateSet, command.getPostgresUser(), command.getPlaintext());
                                        return null;
                                    }
                                    // </editor-fold>
                                    // <editor-fold defaultstate="collapsed" desc="Usernames">
                                    case set_username_password : {
                                        SetUsernamePasswordCommand command = (SetUsernamePasswordCommand)remoteCommand;
                                        usernames.setUsernamePassword(db, invalidateSet, command.getUsername(), command.getPlaintext(), isInteractive);
                                        return null;
                                    }
                                    // </editor-fold>
                                    default : throw new RemoteException("Command not implemented: " + remoteCommand.getCommandName());
                                }
                            } catch(RemoteException err) {
                                throw new WrappedException(err);
                            }
                        }
                    }
                );
                // If the transaction has been committed, send invalidation signals and return result.
                Set<ServiceName> modifiedServiceNames;
                if(factory.database.isInTransaction()) {
                    // Nothing actually modified yet since commit not yet occured
                    modifiedServiceNames = Collections.emptySet();
                } else {
                    // Commit occurred, notify system.
                    modifiedServiceNames = factory.addInvalidateSet(this, invalidateSet);
                }
                return new CommandResult<R>(
                    result,
                    modifiedServiceNames
                );
            } finally {
                // On either commit or rollback, always clear the invalidateSet for next use.
                if(!factory.database.isInTransaction()) invalidateSet.clear();
            }
        } catch(WrappedException err) {
            Throwable wrapped = err.getCause();
            if(wrapped instanceof RemoteException) throw (RemoteException)wrapped;
            throw err;
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(RuntimeException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    // <editor-fold defaultstate="collapsed" desc="Invalidate Set Management">
    private final EnumSet<ServiceName> invalidatedServices = EnumSet.noneOf(ServiceName.class);
    void clearInvalidatedServices(EnumSet<ServiceName> addTo) {
        addTo.addAll(invalidatedServices);
        invalidatedServices.clear();
    }

    /**
     * This is called with the factory.connectors lock held - must complete
     * quickly with careful locking to avoid deadlock.
     */
    void servicesInvalidated(EnumSet<ServiceName> invalidatedSet) {
        invalidatedServices.addAll(invalidatedSet);
        // TODO: Cache signaling
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Services">
    private final AtomicReference<Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>>> tables = new AtomicReference<Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>>>();
    @Override
    public Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>> getServices() throws RemoteException {
        Map<ServiceName,AOServService<DatabaseConnector,DatabaseConnectorFactory,?,?>> ts = tables.get();
        if(ts==null) {
            ts = AOServConnectorUtils.createServiceMap(this);
            if(!tables.compareAndSet(null, ts)) ts = tables.get();
        }
        return ts;
    }

    @Override
    public AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> getAoServerDaemonHosts() {
        return aoserverDaemonHosts;
    }

    @Override
    public AOServerResourceService<DatabaseConnector,DatabaseConnectorFactory> getAoServerResources() {
        return aoserverResources;
    }

    @Override
    public AOServerService<DatabaseConnector,DatabaseConnectorFactory> getAoServers() {
        return aoservers;
    }

    @Override
    public AOServPermissionService<DatabaseConnector,DatabaseConnectorFactory> getAoservPermissions() {
        return aoservPermissions;
    }

    @Override
    public AOServRoleService<DatabaseConnector,DatabaseConnectorFactory> getAoservRoles() {
        return aoservRoles;
    }

    @Override
    public AOServRolePermissionService<DatabaseConnector,DatabaseConnectorFactory> getAoservRolePermissions() {
        return aoservRolePermissions;
    }

    @Override
    public ArchitectureService<DatabaseConnector,DatabaseConnectorFactory> getArchitectures() {
        return architectures;
    }

    @Override
    public BackupPartitionService<DatabaseConnector,DatabaseConnectorFactory> getBackupPartitions() {
        return backupPartitions;
    }

    @Override
    public BackupRetentionService<DatabaseConnector,DatabaseConnectorFactory> getBackupRetentions() {
        return backupRetentions;
    }

    // TODO: public BankAccountService<DatabaseConnector,DatabaseConnectorFactory> getBankAccounts();

    @Override
    public BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactionTypes() {
        return bankTransactionTypes;
    }

    // TODO: public BankTransactionService<DatabaseConnector,DatabaseConnectorFactory> getBankTransactions();

    // TODO: public BankService<DatabaseConnector,DatabaseConnectorFactory> getBanks();

    @Override
    public BrandService<DatabaseConnector,DatabaseConnectorFactory> getBrands() {
        return brands;
    }

    @Override
    public BusinessAdministratorService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministrators() {
        return businessAdministrators;
    }

    @Override
    public BusinessAdministratorRoleService<DatabaseConnector,DatabaseConnectorFactory> getBusinessAdministratorRoles() {
        return businessAdministratorRoles;
    }

    @Override
    public BusinessProfileService<DatabaseConnector,DatabaseConnectorFactory> getBusinessProfiles() {
        return businessProfiles;
    }

    @Override
    public BusinessService<DatabaseConnector,DatabaseConnectorFactory> getBusinesses() {
        return businesses;
    }

    @Override
    public BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> getBusinessServers() {
        return businessServers;
    }

    @Override
    public CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> getCountryCodes() {
        return countryCodes;
    }

    @Override
    public CreditCardProcessorService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardProcessors() {
        return creditCardProcessors;
    }

    @Override
    public CreditCardTransactionService<DatabaseConnector,DatabaseConnectorFactory> getCreditCardTransactions() {
        return creditCardTransactions;
    }

    @Override
    public CreditCardService<DatabaseConnector,DatabaseConnectorFactory> getCreditCards() {
        return creditCards;
    }

    @Override
    public CvsRepositoryService<DatabaseConnector,DatabaseConnectorFactory> getCvsRepositories() {
        return cvsRepositories;
    }

    @Override
    public DisableLogService<DatabaseConnector,DatabaseConnectorFactory> getDisableLogs() {
        return disableLogs;
    }
    /* TODO
    public DistroFileTypeService<DatabaseConnector,DatabaseConnectorFactory> getDistroFileTypes();

    public DistroFileService<DatabaseConnector,DatabaseConnectorFactory> getDistroFiles();
     */
    @Override
    public DnsRecordService<DatabaseConnector,DatabaseConnectorFactory> getDnsRecords() {
        return dnsRecords;
    }

    @Override
    public DnsTldService<DatabaseConnector,DatabaseConnectorFactory> getDnsTlds() {
        return dnsTlds;
    }

    @Override
    public DnsTypeService<DatabaseConnector,DatabaseConnectorFactory> getDnsTypes() {
        return dnsTypes;
    }

    @Override
    public DnsZoneService<DatabaseConnector,DatabaseConnectorFactory> getDnsZones() {
        return dnsZones;
    }

    // TODO: public EmailAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailAddresses();

    // TODO: public EmailAttachmentBlockService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentBlocks();

    @Override
    public EmailAttachmentTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailAttachmentTypes() {
        return emailAttachmentTypes;
    }

    // TODO: public EmailDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailDomains();

    // TODO: public EmailForwardingService<DatabaseConnector,DatabaseConnectorFactory> getEmailForwardings();

    @Override
    public EmailInboxService<DatabaseConnector,DatabaseConnectorFactory> getEmailInboxes() {
        return emailInboxes;
    }
    /* TODO
    public EmailListAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailListAddresses();

    public EmailListService<DatabaseConnector,DatabaseConnectorFactory> getEmailLists();

    public EmailPipeAddressService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipeAddresses();

    public EmailPipeService<DatabaseConnector,DatabaseConnectorFactory> getEmailPipes();
    */
    @Override
    public EmailSmtpRelayTypeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelayTypes() {
        return emailSmtpRelayTypes;
    }

    // TODO: public EmailSmtpRelayService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpRelays();

    // TODO: public EmailSmtpSmartHostDomainService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHostDomains();

    // TODO: public EmailSmtpSmartHostService<DatabaseConnector,DatabaseConnectorFactory> getEmailSmtpSmartHosts();

    @Override
    public EmailSpamAssassinIntegrationModeService<DatabaseConnector,DatabaseConnectorFactory> getEmailSpamAssassinIntegrationModes() {
        return emailSpamAssassinIntegrationModes;
    }

    // TODO: public EncryptionKeyService<DatabaseConnector,DatabaseConnectorFactory> getEncryptionKeys();

    @Override
    public ExpenseCategoryService<DatabaseConnector,DatabaseConnectorFactory> getExpenseCategories() {
        return expenseCategories;
    }

    @Override
    public FailoverFileLogService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileLogs() {
        return failoverFileLogs;
    }

    @Override
    public FailoverFileReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileReplications() {
        return failoverFileReplications;
    }

    @Override
    public FailoverFileScheduleService<DatabaseConnector,DatabaseConnectorFactory> getFailoverFileSchedules() {
        return failoverFileSchedules;
    }

    @Override
    public FailoverMySQLReplicationService<DatabaseConnector,DatabaseConnectorFactory> getFailoverMySQLReplications() {
        return failoverMySQLReplications;
    }

    @Override
    public FileBackupSettingService<DatabaseConnector,DatabaseConnectorFactory> getFileBackupSettings() {
        return fileBackupSettings;
    }

    @Override
    public FtpGuestUserService<DatabaseConnector,DatabaseConnectorFactory> getFtpGuestUsers() {
        return ftpGuestUsers;
    }

    @Override
    public GroupNameService<DatabaseConnector,DatabaseConnectorFactory> getGroupNames() {
        return groupNames;
    }

    // TODO: public HttpdBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdBinds();

    // TODO: public HttpdJBossSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossSites();

    @Override
    public HttpdJBossVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJBossVersions() {
        return httpdJBossVersions;
    }

    @Override
    public HttpdJKCodeService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKCodes() {
        return httpdJKCodes;
    }

    @Override
    public HttpdJKProtocolService<DatabaseConnector,DatabaseConnectorFactory> getHttpdJKProtocols() {
        return httpdJKProtocols;
    }

    @Override
    public HttpdServerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdServers() {
        return httpdServers;
    }
    /* TODO
    public HttpdSharedTomcatService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSharedTomcats();

    public HttpdSiteAuthenticatedLocationService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteAuthenticatedLocations();

    public HttpdSiteBindService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteBinds();

    public HttpdSiteURLService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSiteURLs();
    */
    @Override
    public HttpdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdSites() {
        return httpdSites;
    }
    /* TODO
    public HttpdStaticSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdStaticSites();

    public HttpdTomcatContextService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatContexts();

    public HttpdTomcatDataSourceService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatDataSources();

    public HttpdTomcatParameterService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatParameters();

    public HttpdTomcatSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSites();

    public HttpdTomcatSharedSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatSharedSites();

    public HttpdTomcatStdSiteService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatStdSites();
    */
    @Override
    public HttpdTomcatVersionService<DatabaseConnector,DatabaseConnectorFactory> getHttpdTomcatVersions() {
        return httpdTomcatVersions;
    }

    // TODO: public HttpdWorkerService<DatabaseConnector,DatabaseConnectorFactory> getHttpdWorkers();

    @Override
    public IPAddressService<DatabaseConnector,DatabaseConnectorFactory> getIpAddresses() {
        return ipAddresses;
    }

    @Override
    public LanguageService<DatabaseConnector,DatabaseConnectorFactory> getLanguages() {
        return languages;
    }

    // TODO: public LinuxAccAddressService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccAddresses();

    @Override
    public LinuxAccountGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccountGroups() {
        return linuxAccountGroups;
    }

    @Override
    public LinuxAccountTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccountTypes() {
        return linuxAccountTypes;
    }

    @Override
    public LinuxAccountService<DatabaseConnector,DatabaseConnectorFactory> getLinuxAccounts() {
        return linuxAccounts;
    }

    @Override
    public LinuxGroupTypeService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroupTypes() {
        return linuxGroupTypes;
    }

    @Override
    public LinuxGroupService<DatabaseConnector,DatabaseConnectorFactory> getLinuxGroups() {
        return linuxGroups;
    }

    // TODO: public MajordomoListService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoLists();

    // TODO: public MajordomoServerService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoServers();

    @Override
    public MajordomoVersionService<DatabaseConnector,DatabaseConnectorFactory> getMajordomoVersions() {
        return majordomoVersions;
    }

    // TODO: public MasterHistoryService<DatabaseConnector,DatabaseConnectorFactory> getMasterHistory();

    @Override
    public MasterHostService<DatabaseConnector,DatabaseConnectorFactory> getMasterHosts() {
        return masterHosts;
    }

    @Override
    public MasterServerService<DatabaseConnector,DatabaseConnectorFactory> getMasterServers() {
        return masterServers;
    }

    @Override
    public MasterUserService<DatabaseConnector,DatabaseConnectorFactory> getMasterUsers() {
        return masterUsers;
    }

    // TODO: public MonthlyChargeService<DatabaseConnector,DatabaseConnectorFactory> getMonthlyCharges();

    @Override
    public MySQLDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDatabases() {
        return mysqlDatabases;
    }

    @Override
    public MySQLDBUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlDBUsers() {
        return mysqlDBUsers;
    }

    @Override
    public MySQLServerService<DatabaseConnector,DatabaseConnectorFactory> getMysqlServers() {
        return mysqlServers;
    }

    @Override
    public MySQLUserService<DatabaseConnector,DatabaseConnectorFactory> getMysqlUsers() {
        return mysqlUsers;
    }

    @Override
    public NetBindService<DatabaseConnector,DatabaseConnectorFactory> getNetBinds() {
        return netBinds;
    }

    @Override
    public NetDeviceIDService<DatabaseConnector,DatabaseConnectorFactory> getNetDeviceIDs() {
        return netDeviceIDs;
    }

    @Override
    public NetDeviceService<DatabaseConnector,DatabaseConnectorFactory> getNetDevices() {
        return netDevices;
    }

    @Override
    public NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> getNetProtocols() {
        return netProtocols;
    }

    @Override
    public NetTcpRedirectService<DatabaseConnector,DatabaseConnectorFactory> getNetTcpRedirects() {
        return netTcpRedirects;
    }

    // TODO: public NoticeLogService<DatabaseConnector,DatabaseConnectorFactory> getNoticeLogs();

    @Override
    public NoticeTypeService<DatabaseConnector,DatabaseConnectorFactory> getNoticeTypes() {
        return noticeTypes;
    }

    @Override
    public OperatingSystemVersionService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystemVersions() {
        return operatingSystemVersions;
    }

    @Override
    public OperatingSystemService<DatabaseConnector,DatabaseConnectorFactory> getOperatingSystems() {
        return operatingSystems;
    }

    @Override
    public PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> getPackageCategories() {
        return packageCategories;
    }

    @Override
    public PackageDefinitionBusinessService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitionBusinesses() {
        return packageDefinitionBusinesses;
    }

    @Override
    public PackageDefinitionLimitService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitionLimits() {
        return packageDefinitionLimits;
    }

    @Override
    public PackageDefinitionService<DatabaseConnector,DatabaseConnectorFactory> getPackageDefinitions() {
        return packageDefinitions;
    }

    @Override
    public PaymentTypeService<DatabaseConnector,DatabaseConnectorFactory> getPaymentTypes() {
        return paymentTypes;
    }

    // TODO: public PhysicalServerService<DatabaseConnector,DatabaseConnectorFactory> getPhysicalServers();

    @Override
    public PostgresDatabaseService<DatabaseConnector,DatabaseConnectorFactory> getPostgresDatabases() {
        return postgresDatabases;
    }

    @Override
    public PostgresEncodingService<DatabaseConnector,DatabaseConnectorFactory> getPostgresEncodings() {
        return postgresEncodings;
    }

    @Override
    public PostgresServerService<DatabaseConnector,DatabaseConnectorFactory> getPostgresServers() {
        return postgresServers;
    }

    @Override
    public PostgresUserService<DatabaseConnector,DatabaseConnectorFactory> getPostgresUsers() {
        return postgresUsers;
    }

    @Override
    public PostgresVersionService<DatabaseConnector,DatabaseConnectorFactory> getPostgresVersions() {
        return postgresVersions;
    }

    @Override
    public PrivateFtpServerService<DatabaseConnector,DatabaseConnectorFactory> getPrivateFtpServers() {
        return privateFtpServers;
    }

    @Override
    public ProcessorTypeService<DatabaseConnector,DatabaseConnectorFactory> getProcessorTypes() {
        return processorTypes;
    }

    @Override
    public ProtocolService<DatabaseConnector,DatabaseConnectorFactory> getProtocols() {
        return protocols;
    }
    /* TODO
    public RackService<DatabaseConnector,DatabaseConnectorFactory> getRacks();
    */
    @Override
    public ResellerService<DatabaseConnector,DatabaseConnectorFactory> getResellers() {
        return resellers;
    }

    @Override
    public ResourceTypeService<DatabaseConnector,DatabaseConnectorFactory> getResourceTypes() {
        return resourceTypes;
    }

    @Override
    public ResourceService<DatabaseConnector,DatabaseConnectorFactory> getResources() {
        return resources;
    }

    @Override
    public ServerFarmService<DatabaseConnector,DatabaseConnectorFactory> getServerFarms() {
        return serverFarms;
    }

    @Override
    public ServerResourceService<DatabaseConnector,DatabaseConnectorFactory> getServerResources() {
        return serverResources;
    }

    @Override
    public ServerService<DatabaseConnector,DatabaseConnectorFactory> getServers() {
        return servers;
    }

    @Override
    public ShellService<DatabaseConnector,DatabaseConnectorFactory> getShells() {
        return shells;
    }
    /* TODO
    public SignupRequestOptionService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequestOptions();

    public SignupRequestService<DatabaseConnector,DatabaseConnectorFactory> getSignupRequests();

    public SpamEmailMessageService<DatabaseConnector,DatabaseConnectorFactory> getSpamEmailMessages();

    public SystemEmailAliasService<DatabaseConnector,DatabaseConnectorFactory> getSystemEmailAliases();
    */
    @Override
    public TechnologyService<DatabaseConnector,DatabaseConnectorFactory> getTechnologies() {
        return technologies;
    }

    @Override
    public TechnologyClassService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyClasses() {
        return technologyClasses;
    }

    @Override
    public TechnologyNameService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyNames() {
        return technologyNames;
    }

    @Override
    public TechnologyVersionService<DatabaseConnector,DatabaseConnectorFactory> getTechnologyVersions() {
        return technologyVersions;
    }

    @Override
    public TicketActionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTicketActionTypes() {
        return ticketActionTypes;
    }

    @Override
    public TicketActionService<DatabaseConnector,DatabaseConnectorFactory> getTicketActions() {
        return ticketActions;
    }

    @Override
    public TicketAssignmentService<DatabaseConnector,DatabaseConnectorFactory> getTicketAssignments() {
        return ticketAssignments;
    }

    // TODO: public TicketBrandCategoryService<DatabaseConnector,DatabaseConnectorFactory> getTicketBrandCategories();

    @Override
    public TicketCategoryService<DatabaseConnector,DatabaseConnectorFactory> getTicketCategories() {
        return ticketCategories;
    }

    @Override
    public TicketPriorityService<DatabaseConnector,DatabaseConnectorFactory> getTicketPriorities() {
        return ticketPriorities;
    }

    @Override
    public TicketStatusService<DatabaseConnector,DatabaseConnectorFactory> getTicketStatuses() {
        return ticketStatuses;
    }

    @Override
    public TicketTypeService<DatabaseConnector,DatabaseConnectorFactory> getTicketTypes() {
        return ticketTypes;
    }

    @Override
    public TicketService<DatabaseConnector,DatabaseConnectorFactory> getTickets() {
        return tickets;
    }

    @Override
    public TimeZoneService<DatabaseConnector,DatabaseConnectorFactory> getTimeZones() {
        return timeZones;
    }

    @Override
    public TransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> getTransactionTypes() {
        return transactionTypes;
    }

    @Override
    public TransactionService<DatabaseConnector,DatabaseConnectorFactory> getTransactions() {
        return transactions;
    }
    /* TODO
    public USStateService<DatabaseConnector,DatabaseConnectorFactory> getUsStates();
    */
    @Override
    public UsernameService<DatabaseConnector,DatabaseConnectorFactory> getUsernames() {
        return usernames;
    }
    /* TODO
    public VirtualDiskService<DatabaseConnector,DatabaseConnectorFactory> getVirtualDisks();
     */
    @Override
    public VirtualServerService<DatabaseConnector,DatabaseConnectorFactory> getVirtualServers() {
        return virtualServers;
    }
    /* TODO
    public WhoisHistoryService<DatabaseConnector,DatabaseConnectorFactory> getWhoisHistory();
     */
    // </editor-fold>
}
