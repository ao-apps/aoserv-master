/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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
final public class DatabaseConnector extends AbstractConnector {

    final DatabaseConnectorFactory factory;
    final DatabaseAOServerDaemonHostService aoserverDaemonHosts;
    final DatabaseAOServerService aoservers;
    final DatabaseAOServPermissionService aoservPermissions;
    final DatabaseAOServRoleService aoservRoles;
    final DatabaseAOServRolePermissionService aoservRolePermissions;
    final DatabaseArchitectureService architectures;
    final DatabaseBackupPartitionService backupPartitions;
    final DatabaseBackupRetentionService backupRetentions;
    final DatabaseBackupServerService backupServers;
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
    final DatabasePhysicalServerService physicalServers;
    final DatabasePostgresDatabaseService postgresDatabases;
    final DatabasePostgresEncodingService postgresEncodings;
    final DatabasePostgresServerService postgresServers;
    final DatabasePostgresUserService postgresUsers;
    final DatabasePostgresVersionService postgresVersions;
    final DatabasePrivateFtpServerService privateFtpServers;
    final DatabaseProcessorTypeService processorTypes;
    final DatabaseProtocolService protocols;
    final DatabaseRackService racks;
    final DatabaseResellerService resellers;
    final DatabaseResourceTypeService resourceTypes;
    final DatabaseServerFarmService serverFarms;
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

    DatabaseConnector(DatabaseConnectorFactory factory, Locale locale, UserId connectAs, UserId authenticateAs, String password, DomainName daemonServer) {
        super(locale, connectAs, authenticateAs, password, daemonServer);
        this.factory = factory;
        // TODO: Move to where declared
        aoserverDaemonHosts = new DatabaseAOServerDaemonHostService(this);
        aoservers = new DatabaseAOServerService(this);
        aoservPermissions = new DatabaseAOServPermissionService(this);
        aoservRoles = new DatabaseAOServRoleService(this);
        aoservRolePermissions = new DatabaseAOServRolePermissionService(this);
        architectures = new DatabaseArchitectureService(this);
        backupPartitions = new DatabaseBackupPartitionService(this);
        backupRetentions = new DatabaseBackupRetentionService(this);
        backupServers = new DatabaseBackupServerService(this);
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
        physicalServers = new DatabasePhysicalServerService(this);
        postgresDatabases = new DatabasePostgresDatabaseService(this);
        postgresEncodings = new DatabasePostgresEncodingService(this);
        postgresServers = new DatabasePostgresServerService(this);
        postgresUsers = new DatabasePostgresUserService(this);
        postgresVersions = new DatabasePostgresVersionService(this);
        privateFtpServers = new DatabasePrivateFtpServerService(this);
        processorTypes = new DatabaseProcessorTypeService(this);
        protocols = new DatabaseProtocolService(this);
        racks = new DatabaseRackService(this);
        resellers = new DatabaseResellerService(this);
        resourceTypes = new DatabaseResourceTypeService(this);
        serverFarms = new DatabaseServerFarmService(this);
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
        if(factory.isEnabledMasterUser(db, getConnectAs())) return AccountType.MASTER;
        if(factory.isEnabledDaemonUser(db, getConnectAs())) return AccountType.DAEMON;
        if(factory.isEnabledBusinessAdministrator(db, getConnectAs())) return AccountType.BUSINESS;
        return AccountType.DISABLED;
    }

    @Override
    public DatabaseConnectorFactory getFactory() {
        return factory;
    }

    @Override
    public <R> CommandResult<R> executeCommand(final RemoteCommand<R> remoteCommand, final boolean isInteractive) throws RemoteException {
        try {
            // Make sure not accidentally running command on root user
            if(
                getAuthenticateAs().equals(factory.rootConnector.getAuthenticateAs())
                || getConnectAs().equals(factory.rootConnector.getConnectAs())
            ) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnector.executeCommand.refusingRootConnector", remoteCommand.getCommandName()));

            final InvalidateSet invalidateSet = InvalidateSet.getInstance();
            try {
                R result = factory.database.executeTransaction(
                    new DatabaseCallable<R>() {
                        @Override
                        public R call(DatabaseConnection db) throws SQLException {
                            try {
                                // Make sure current user is enabled
                                if(!factory.isEnabledBusinessAdministrator(db, getAuthenticateAs())) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));
                                if(!factory.isEnabledBusinessAdministrator(db, getConnectAs())) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));

                                // Check permissions using root connector
                                Set<AOServPermission.Permission> permissions = remoteCommand.getCommandName().getPermissions();
                                BusinessAdministrator rootBa = factory.rootConnector.getBusinessAdministrators().get(getConnectAs());
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
    @Override
    public AOServerDaemonHostService getAoServerDaemonHosts() {
        return aoserverDaemonHosts;
    }

    @Override
    public AOServerService getAoServers() {
        return aoservers;
    }

    @Override
    public AOServPermissionService getAoservPermissions() {
        return aoservPermissions;
    }

    @Override
    public AOServRoleService getAoservRoles() {
        return aoservRoles;
    }

    @Override
    public AOServRolePermissionService getAoservRolePermissions() {
        return aoservRolePermissions;
    }

    @Override
    public ArchitectureService getArchitectures() {
        return architectures;
    }

    @Override
    public BackupPartitionService getBackupPartitions() {
        return backupPartitions;
    }

    @Override
    public BackupRetentionService getBackupRetentions() {
        return backupRetentions;
    }

    @Override
    public BackupServerService getBackupServers() {
        return backupServers;
    }

    // TODO: public BankAccountService getBankAccounts();

    @Override
    public BankTransactionTypeService getBankTransactionTypes() {
        return bankTransactionTypes;
    }

    // TODO: public BankTransactionService getBankTransactions();

    // TODO: public BankService getBanks();

    @Override
    public BrandService getBrands() {
        return brands;
    }

    @Override
    public BusinessAdministratorService getBusinessAdministrators() {
        return businessAdministrators;
    }

    @Override
    public BusinessAdministratorRoleService getBusinessAdministratorRoles() {
        return businessAdministratorRoles;
    }

    @Override
    public BusinessProfileService getBusinessProfiles() {
        return businessProfiles;
    }

    @Override
    public BusinessService getBusinesses() {
        return businesses;
    }

    @Override
    public BusinessServerService getBusinessServers() {
        return businessServers;
    }

    @Override
    public CountryCodeService getCountryCodes() {
        return countryCodes;
    }

    @Override
    public CreditCardProcessorService getCreditCardProcessors() {
        return creditCardProcessors;
    }

    @Override
    public CreditCardTransactionService getCreditCardTransactions() {
        return creditCardTransactions;
    }

    @Override
    public CreditCardService getCreditCards() {
        return creditCards;
    }

    @Override
    public CvsRepositoryService getCvsRepositories() {
        return cvsRepositories;
    }

    @Override
    public DisableLogService getDisableLogs() {
        return disableLogs;
    }
    /* TODO
    public DistroFileTypeService getDistroFileTypes();

    public DistroFileService getDistroFiles();
     */
    @Override
    public DnsRecordService getDnsRecords() {
        return dnsRecords;
    }

    @Override
    public DnsTldService getDnsTlds() {
        return dnsTlds;
    }

    @Override
    public DnsTypeService getDnsTypes() {
        return dnsTypes;
    }

    @Override
    public DnsZoneService getDnsZones() {
        return dnsZones;
    }

    // TODO: public EmailAddressService getEmailAddresses();

    // TODO: public EmailAttachmentBlockService getEmailAttachmentBlocks();

    @Override
    public EmailAttachmentTypeService getEmailAttachmentTypes() {
        return emailAttachmentTypes;
    }

    // TODO: public EmailDomainService getEmailDomains();

    // TODO: public EmailForwardingService getEmailForwardings();

    @Override
    public EmailInboxService getEmailInboxes() {
        return emailInboxes;
    }
    /* TODO
    public EmailListAddressService getEmailListAddresses();

    public EmailListService getEmailLists();

    public EmailPipeAddressService getEmailPipeAddresses();

    public EmailPipeService getEmailPipes();
    */
    @Override
    public EmailSmtpRelayTypeService getEmailSmtpRelayTypes() {
        return emailSmtpRelayTypes;
    }

    // TODO: public EmailSmtpRelayService getEmailSmtpRelays();

    // TODO: public EmailSmtpSmartHostDomainService getEmailSmtpSmartHostDomains();

    // TODO: public EmailSmtpSmartHostService getEmailSmtpSmartHosts();

    @Override
    public EmailSpamAssassinIntegrationModeService getEmailSpamAssassinIntegrationModes() {
        return emailSpamAssassinIntegrationModes;
    }

    // TODO: public EncryptionKeyService getEncryptionKeys();

    @Override
    public ExpenseCategoryService getExpenseCategories() {
        return expenseCategories;
    }

    @Override
    public FailoverFileLogService getFailoverFileLogs() {
        return failoverFileLogs;
    }

    @Override
    public FailoverFileReplicationService getFailoverFileReplications() {
        return failoverFileReplications;
    }

    @Override
    public FailoverFileScheduleService getFailoverFileSchedules() {
        return failoverFileSchedules;
    }

    @Override
    public FailoverMySQLReplicationService getFailoverMySQLReplications() {
        return failoverMySQLReplications;
    }

    @Override
    public FileBackupSettingService getFileBackupSettings() {
        return fileBackupSettings;
    }

    @Override
    public FtpGuestUserService getFtpGuestUsers() {
        return ftpGuestUsers;
    }

    @Override
    public GroupNameService getGroupNames() {
        return groupNames;
    }

    // TODO: public HttpdBindService getHttpdBinds();

    // TODO: public HttpdJBossSiteService getHttpdJBossSites();

    @Override
    public HttpdJBossVersionService getHttpdJBossVersions() {
        return httpdJBossVersions;
    }

    @Override
    public HttpdJKCodeService getHttpdJKCodes() {
        return httpdJKCodes;
    }

    @Override
    public HttpdJKProtocolService getHttpdJKProtocols() {
        return httpdJKProtocols;
    }

    @Override
    public HttpdServerService getHttpdServers() {
        return httpdServers;
    }
    /* TODO
    public HttpdSharedTomcatService getHttpdSharedTomcats();

    public HttpdSiteAuthenticatedLocationService getHttpdSiteAuthenticatedLocations();

    public HttpdSiteBindService getHttpdSiteBinds();

    public HttpdSiteURLService getHttpdSiteURLs();
    */
    @Override
    public HttpdSiteService getHttpdSites() {
        return httpdSites;
    }
    /* TODO
    public HttpdStaticSiteService getHttpdStaticSites();

    public HttpdTomcatContextService getHttpdTomcatContexts();

    public HttpdTomcatDataSourceService getHttpdTomcatDataSources();

    public HttpdTomcatParameterService getHttpdTomcatParameters();

    public HttpdTomcatSiteService getHttpdTomcatSites();

    public HttpdTomcatSharedSiteService getHttpdTomcatSharedSites();

    public HttpdTomcatStdSiteService getHttpdTomcatStdSites();
    */
    @Override
    public HttpdTomcatVersionService getHttpdTomcatVersions() {
        return httpdTomcatVersions;
    }

    // TODO: public HttpdWorkerService getHttpdWorkers();

    @Override
    public IPAddressService getIpAddresses() {
        return ipAddresses;
    }

    @Override
    public LanguageService getLanguages() {
        return languages;
    }

    // TODO: public LinuxAccAddressService getLinuxAccAddresses();

    @Override
    public LinuxAccountGroupService getLinuxAccountGroups() {
        return linuxAccountGroups;
    }

    @Override
    public LinuxAccountTypeService getLinuxAccountTypes() {
        return linuxAccountTypes;
    }

    @Override
    public LinuxAccountService getLinuxAccounts() {
        return linuxAccounts;
    }

    @Override
    public LinuxGroupTypeService getLinuxGroupTypes() {
        return linuxGroupTypes;
    }

    @Override
    public LinuxGroupService getLinuxGroups() {
        return linuxGroups;
    }

    // TODO: public MajordomoListService getMajordomoLists();

    // TODO: public MajordomoServerService getMajordomoServers();

    @Override
    public MajordomoVersionService getMajordomoVersions() {
        return majordomoVersions;
    }

    // TODO: public MasterHistoryService getMasterHistory();

    @Override
    public MasterHostService getMasterHosts() {
        return masterHosts;
    }

    @Override
    public MasterServerService getMasterServers() {
        return masterServers;
    }

    @Override
    public MasterUserService getMasterUsers() {
        return masterUsers;
    }

    // TODO: public MonthlyChargeService getMonthlyCharges();

    @Override
    public MySQLDatabaseService getMysqlDatabases() {
        return mysqlDatabases;
    }

    @Override
    public MySQLDBUserService getMysqlDBUsers() {
        return mysqlDBUsers;
    }

    @Override
    public MySQLServerService getMysqlServers() {
        return mysqlServers;
    }

    @Override
    public MySQLUserService getMysqlUsers() {
        return mysqlUsers;
    }

    @Override
    public NetBindService getNetBinds() {
        return netBinds;
    }

    @Override
    public NetDeviceIDService getNetDeviceIDs() {
        return netDeviceIDs;
    }

    @Override
    public NetDeviceService getNetDevices() {
        return netDevices;
    }

    @Override
    public NetProtocolService getNetProtocols() {
        return netProtocols;
    }

    @Override
    public NetTcpRedirectService getNetTcpRedirects() {
        return netTcpRedirects;
    }

    // TODO: public NoticeLogService getNoticeLogs();

    @Override
    public NoticeTypeService getNoticeTypes() {
        return noticeTypes;
    }

    @Override
    public OperatingSystemVersionService getOperatingSystemVersions() {
        return operatingSystemVersions;
    }

    @Override
    public OperatingSystemService getOperatingSystems() {
        return operatingSystems;
    }

    @Override
    public PackageCategoryService getPackageCategories() {
        return packageCategories;
    }

    @Override
    public PackageDefinitionBusinessService getPackageDefinitionBusinesses() {
        return packageDefinitionBusinesses;
    }

    @Override
    public PackageDefinitionLimitService getPackageDefinitionLimits() {
        return packageDefinitionLimits;
    }

    @Override
    public PackageDefinitionService getPackageDefinitions() {
        return packageDefinitions;
    }

    @Override
    public PaymentTypeService getPaymentTypes() {
        return paymentTypes;
    }

    @Override
    public PhysicalServerService getPhysicalServers() {
        return physicalServers;
    }

    @Override
    public PostgresDatabaseService getPostgresDatabases() {
        return postgresDatabases;
    }

    @Override
    public PostgresEncodingService getPostgresEncodings() {
        return postgresEncodings;
    }

    @Override
    public PostgresServerService getPostgresServers() {
        return postgresServers;
    }

    @Override
    public PostgresUserService getPostgresUsers() {
        return postgresUsers;
    }

    @Override
    public PostgresVersionService getPostgresVersions() {
        return postgresVersions;
    }

    @Override
    public PrivateFtpServerService getPrivateFtpServers() {
        return privateFtpServers;
    }

    @Override
    public ProcessorTypeService getProcessorTypes() {
        return processorTypes;
    }

    @Override
    public ProtocolService getProtocols() {
        return protocols;
    }

    @Override
    public RackService getRacks() {
        return racks;
    }

    @Override
    public ResellerService getResellers() {
        return resellers;
    }

    @Override
    public ResourceTypeService getResourceTypes() {
        return resourceTypes;
    }

    @Override
    public ServerFarmService getServerFarms() {
        return serverFarms;
    }

    @Override
    public ShellService getShells() {
        return shells;
    }
    /* TODO
    public SignupRequestOptionService getSignupRequestOptions();

    public SignupRequestService getSignupRequests();

    public SpamEmailMessageService getSpamEmailMessages();

    public SystemEmailAliasService getSystemEmailAliases();
    */
    @Override
    public TechnologyService getTechnologies() {
        return technologies;
    }

    @Override
    public TechnologyClassService getTechnologyClasses() {
        return technologyClasses;
    }

    @Override
    public TechnologyNameService getTechnologyNames() {
        return technologyNames;
    }

    @Override
    public TechnologyVersionService getTechnologyVersions() {
        return technologyVersions;
    }

    @Override
    public TicketActionTypeService getTicketActionTypes() {
        return ticketActionTypes;
    }

    @Override
    public TicketActionService getTicketActions() {
        return ticketActions;
    }

    @Override
    public TicketAssignmentService getTicketAssignments() {
        return ticketAssignments;
    }

    // TODO: public TicketBrandCategoryService getTicketBrandCategories();

    @Override
    public TicketCategoryService getTicketCategories() {
        return ticketCategories;
    }

    @Override
    public TicketPriorityService getTicketPriorities() {
        return ticketPriorities;
    }

    @Override
    public TicketStatusService getTicketStatuses() {
        return ticketStatuses;
    }

    @Override
    public TicketTypeService getTicketTypes() {
        return ticketTypes;
    }

    @Override
    public TicketService getTickets() {
        return tickets;
    }

    @Override
    public TimeZoneService getTimeZones() {
        return timeZones;
    }

    @Override
    public TransactionTypeService getTransactionTypes() {
        return transactionTypes;
    }

    @Override
    public TransactionService getTransactions() {
        return transactions;
    }
    /* TODO
    public USStateService getUsStates();
    */
    @Override
    public UsernameService getUsernames() {
        return usernames;
    }
    /* TODO
    public VirtualDiskService getVirtualDisks();
     */
    @Override
    public VirtualServerService getVirtualServers() {
        return virtualServers;
    }
    /* TODO
    public WhoisHistoryService getWhoisHistory();
     */
    // </editor-fold>
}
