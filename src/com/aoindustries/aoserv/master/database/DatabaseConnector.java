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

    DatabaseConnector(DatabaseConnectorFactory factory, Locale locale, UserId username, String password, UserId switchUser, DomainName daemonServer, boolean readOnly) {
        super(locale, username, password, switchUser, daemonServer, readOnly);
        this.factory = factory;
    }

    // <editor-fold defaultstate="collapsed" desc="Security">
    enum AccountType {
        MASTER,
        DAEMON,
        BUSINESS,
        DISABLED
    };

    /**
     * Determines the type of account logged-in based on the switchUser value.  This controls filtering and access.
     */
    AccountType getAccountType(DatabaseConnection db) throws SQLException {
        UserId switchUser = getSwitchUser();
        if(factory.isEnabledMasterUser(db, switchUser)) return AccountType.MASTER;
        if(factory.isEnabledDaemonUser(db, switchUser)) return AccountType.DAEMON;
        if(factory.isEnabledBusinessAdministrator(db, switchUser)) return AccountType.BUSINESS;
        return AccountType.DISABLED;
    }

    /**
     * Determines if the logged-in user has the specified permission.  This is based on the switchUser value
     * of this connection, but checked with the root connector for performance.
     *
     * @see BusinessAdministrator#hasPermission(com.aoindustries.aoserv.client.AOServPermission.Permission)
     */
    boolean hasPermission(AOServPermission.Permission permission) throws RemoteException {
        return factory.getRootConnector().getBusinessAdministrators().get(getSwitchUser()).hasPermission(permission);
    }

    /**
     * Determines if the logged-in user is a ticket admin.  This is based on the switchUser value
     * of this connection, but checked with the root connector for performance.
     */
    boolean isTicketAdmin() throws RemoteException {
        return factory.getRootConnector().getBusinessAdministrators().get(getSwitchUser()).isTicketAdmin();
    }

    /**
     * Determines if the logged-in user can see prices.  This is based on the switchUser value
     * of this connection, but checked with the root connector for performance.
     */
    boolean getCanSeePrices() throws RemoteException {
        return factory.getRootConnector().getBusinessAdministrators().get(getSwitchUser()).getUsername().getBusiness().getCanSeePrices();
    }
    // </editor-fold>

    @Override
    public DatabaseConnectorFactory getFactory() {
        return factory;
    }

    @Override
    public <R> CommandResult<R> execute(final RemoteCommand<R> remoteCommand, final boolean isInteractive) throws RemoteException {
        try {
            // Make sure not accidentally running command on root user
            if(
                getUsername().equals(factory.rootUserId)
                || getSwitchUser().equals(factory.rootUserId)
            ) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnector.executeCommand.refusingRootConnector", remoteCommand.getCommandName()));

            final InvalidateSet invalidateSet = InvalidateSet.getInstance();
            try {
                R result = factory.database.executeTransaction(
                    new DatabaseCallable<R>() {
                        @Override
                        public R call(DatabaseConnection db) throws SQLException {
                            try {
                                // Make sure current user is enabled
                                if(!factory.isEnabledBusinessAdministrator(db, getUsername())) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));
                                if(!factory.isEnabledBusinessAdministrator(db, getSwitchUser())) throw new RemoteException(ApplicationResources.accessor.getMessage("DatabaseConnectorFactory.createConnector.accountDisabled"));

                                // Check permissions using root connector
                                AOServConnector rootConn = factory.getRootConnector();
                                BusinessAdministrator rootUser = rootConn.getBusinessAdministrators().get(getSwitchUser());

                                // Check command using root connector
                                Map<String,List<String>> errors = remoteCommand.checkExecute(DatabaseConnector.this, rootConn, rootUser);
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
    final DatabaseAOServerDaemonHostService aoserverDaemonHosts = new DatabaseAOServerDaemonHostService(this);
    @Override
    public AOServerDaemonHostService getAoServerDaemonHosts() {
        return aoserverDaemonHosts;
    }

    final DatabaseAOServerService aoservers = new DatabaseAOServerService(this);
    @Override
    public AOServerService getAoServers() {
        return aoservers;
    }

    final DatabaseAOServPermissionService aoservPermissions = new DatabaseAOServPermissionService(this);
    @Override
    public AOServPermissionService getAoservPermissions() {
        return aoservPermissions;
    }

    final DatabaseAOServRoleService aoservRoles = new DatabaseAOServRoleService(this);
    @Override
    public AOServRoleService getAoservRoles() {
        return aoservRoles;
    }

    final DatabaseAOServRolePermissionService aoservRolePermissions = new DatabaseAOServRolePermissionService(this);
    @Override
    public AOServRolePermissionService getAoservRolePermissions() {
        return aoservRolePermissions;
    }

    final DatabaseArchitectureService architectures = new DatabaseArchitectureService(this);
    @Override
    public ArchitectureService getArchitectures() {
        return architectures;
    }

    final DatabaseBackupPartitionService backupPartitions = new DatabaseBackupPartitionService(this);
    @Override
    public BackupPartitionService getBackupPartitions() {
        return backupPartitions;
    }

    final DatabaseBackupRetentionService backupRetentions = new DatabaseBackupRetentionService(this);
    @Override
    public BackupRetentionService getBackupRetentions() {
        return backupRetentions;
    }

    final DatabaseBackupServerService backupServers = new DatabaseBackupServerService(this);
    @Override
    public BackupServerService getBackupServers() {
        return backupServers;
    }

    final DatabaseBankAccountService bankAccounts = new DatabaseBankAccountService(this);
    @Override
    public BankAccountService getBankAccounts() {
        return bankAccounts;
    }

    final DatabaseBankTransactionTypeService bankTransactionTypes = new DatabaseBankTransactionTypeService(this);
    @Override
    public BankTransactionTypeService getBankTransactionTypes() {
        return bankTransactionTypes;
    }

    final DatabaseBankTransactionService bankTransactions = new DatabaseBankTransactionService(this);
    @Override
    public BankTransactionService getBankTransactions() {
        return bankTransactions;
    }

    final DatabaseBankService banks = new DatabaseBankService(this);
    @Override
    public BankService getBanks() {
        return banks;
    }

    final DatabaseBrandService brands = new DatabaseBrandService(this);
    @Override
    public BrandService getBrands() {
        return brands;
    }

    final DatabaseBusinessAdministratorService businessAdministrators = new DatabaseBusinessAdministratorService(this);
    @Override
    public BusinessAdministratorService getBusinessAdministrators() {
        return businessAdministrators;
    }

    final DatabaseBusinessAdministratorRoleService businessAdministratorRoles = new DatabaseBusinessAdministratorRoleService(this);
    @Override
    public BusinessAdministratorRoleService getBusinessAdministratorRoles() {
        return businessAdministratorRoles;
    }

    final DatabaseBusinessProfileService businessProfiles = new DatabaseBusinessProfileService(this);
    @Override
    public BusinessProfileService getBusinessProfiles() {
        return businessProfiles;
    }

    final DatabaseBusinessService businesses = new DatabaseBusinessService(this);
    @Override
    public BusinessService getBusinesses() {
        return businesses;
    }

    final DatabaseBusinessServerService businessServers = new DatabaseBusinessServerService(this);
    @Override
    public BusinessServerService getBusinessServers() {
        return businessServers;
    }

    final DatabaseCountryCodeService countryCodes = new DatabaseCountryCodeService(this);
    @Override
    public CountryCodeService getCountryCodes() {
        return countryCodes;
    }

    final DatabaseCreditCardProcessorService creditCardProcessors = new DatabaseCreditCardProcessorService(this);
    @Override
    public CreditCardProcessorService getCreditCardProcessors() {
        return creditCardProcessors;
    }

    final DatabaseCreditCardTransactionService creditCardTransactions = new DatabaseCreditCardTransactionService(this);
    @Override
    public CreditCardTransactionService getCreditCardTransactions() {
        return creditCardTransactions;
    }

    final DatabaseCreditCardService creditCards = new DatabaseCreditCardService(this);
    @Override
    public CreditCardService getCreditCards() {
        return creditCards;
    }

    final DatabaseCvsRepositoryService cvsRepositories = new DatabaseCvsRepositoryService(this);
    @Override
    public CvsRepositoryService getCvsRepositories() {
        return cvsRepositories;
    }

    final DatabaseDisableLogService disableLogs = new DatabaseDisableLogService(this);
    @Override
    public DisableLogService getDisableLogs() {
        return disableLogs;
    }
    /* TODO
    final DatabaseDistroFileTypeService distroFileTypes = new Database;
    public DistroFileTypeService getDistroFileTypes();

    final DatabaseDistroFileService distroFiles = new Database;
    public DistroFileService getDistroFiles();
     */

    final DatabaseDnsRecordService dnsRecords = new DatabaseDnsRecordService(this);
    @Override
    public DnsRecordService getDnsRecords() {
        return dnsRecords;
    }

    final DatabaseDnsTldService dnsTlds = new DatabaseDnsTldService(this);
    @Override
    public DnsTldService getDnsTlds() {
        return dnsTlds;
    }

    final DatabaseDnsTypeService dnsTypes = new DatabaseDnsTypeService(this);
    @Override
    public DnsTypeService getDnsTypes() {
        return dnsTypes;
    }

    final DatabaseDnsZoneService dnsZones = new DatabaseDnsZoneService(this);
    @Override
    public DnsZoneService getDnsZones() {
        return dnsZones;
    }

    // TODO: final DatabaseEmailAddressService emailAddresss = new Database;
    // TODO: public EmailAddressService getEmailAddresses();

    // TODO: final DatabaseEmailAttachmentBlockService emailAttachmentBlocks = new Database;
    // TODO: public EmailAttachmentBlockService getEmailAttachmentBlocks();

    final DatabaseEmailAttachmentTypeService emailAttachmentTypes = new DatabaseEmailAttachmentTypeService(this);
    @Override
    public EmailAttachmentTypeService getEmailAttachmentTypes() {
        return emailAttachmentTypes;
    }

    // TODO: final DatabaseEmailDomainService emailDomains = new Database;
    // TODO: public EmailDomainService getEmailDomains();

    // TODO: final DatabaseEmailForwardingService emailForwardings = new Database;
    // TODO: public EmailForwardingService getEmailForwardings();

    final DatabaseEmailInboxService emailInboxes = new DatabaseEmailInboxService(this);
    @Override
    public EmailInboxService getEmailInboxes() {
        return emailInboxes;
    }
    /* TODO
    final DatabaseEmailListAddressService emailListAddresss = new Database;
    public EmailListAddressService getEmailListAddresses();

    final DatabaseEmailListService emailLists = new Database;
    public EmailListService getEmailLists();

    final DatabaseEmailPipeAddressService emailPipeAddresss = new Database;
    public EmailPipeAddressService getEmailPipeAddresses();

    final DatabaseEmailPipeService emailPipes = new Database;
    public EmailPipeService getEmailPipes();
    */

    final DatabaseEmailSmtpRelayTypeService emailSmtpRelayTypes = new DatabaseEmailSmtpRelayTypeService(this);
    @Override
    public EmailSmtpRelayTypeService getEmailSmtpRelayTypes() {
        return emailSmtpRelayTypes;
    }

    // TODO: final DatabaseEmailSmtpRelayService emailSmtpRelays = new Database;
    // TODO: public EmailSmtpRelayService getEmailSmtpRelays();

    // TODO: final DatabaseEmailSmtpSmartHostDomainService emailSmtpSmartHostDomains = new Database;
    // TODO: public EmailSmtpSmartHostDomainService getEmailSmtpSmartHostDomains();

    // TODO: final DatabaseEmailSmtpSmartHostService emailSmtpSmartHosts = new Database;
    // TODO: public EmailSmtpSmartHostService getEmailSmtpSmartHosts();

    final DatabaseEmailSpamAssassinIntegrationModeService emailSpamAssassinIntegrationModes = new DatabaseEmailSpamAssassinIntegrationModeService(this);
    @Override
    public EmailSpamAssassinIntegrationModeService getEmailSpamAssassinIntegrationModes() {
        return emailSpamAssassinIntegrationModes;
    }

    // TODO: final DatabaseEncryptionKeyService encryptionKeys = new Database;
    // TODO: public EncryptionKeyService getEncryptionKeys();

    final DatabaseExpenseCategoryService expenseCategories = new DatabaseExpenseCategoryService(this);
    @Override
    public ExpenseCategoryService getExpenseCategories() {
        return expenseCategories;
    }

    final DatabaseFailoverFileLogService failoverFileLogs = new DatabaseFailoverFileLogService(this);
    @Override
    public FailoverFileLogService getFailoverFileLogs() {
        return failoverFileLogs;
    }

    final DatabaseFailoverFileReplicationService failoverFileReplications = new DatabaseFailoverFileReplicationService(this);
    @Override
    public FailoverFileReplicationService getFailoverFileReplications() {
        return failoverFileReplications;
    }

    final DatabaseFailoverFileScheduleService failoverFileSchedules = new DatabaseFailoverFileScheduleService(this);
    @Override
    public FailoverFileScheduleService getFailoverFileSchedules() {
        return failoverFileSchedules;
    }

    final DatabaseFailoverMySQLReplicationService failoverMySQLReplications = new DatabaseFailoverMySQLReplicationService(this);
    @Override
    public FailoverMySQLReplicationService getFailoverMySQLReplications() {
        return failoverMySQLReplications;
    }

    final DatabaseFileBackupSettingService fileBackupSettings = new DatabaseFileBackupSettingService(this);
    @Override
    public FileBackupSettingService getFileBackupSettings() {
        return fileBackupSettings;
    }

    final DatabaseFtpGuestUserService ftpGuestUsers = new DatabaseFtpGuestUserService(this);
    @Override
    public FtpGuestUserService getFtpGuestUsers() {
        return ftpGuestUsers;
    }

    final DatabaseGroupNameService groupNames = new DatabaseGroupNameService(this);
    @Override
    public GroupNameService getGroupNames() {
        return groupNames;
    }

    // TODO: final DatabaseHttpdBindService httpdBinds = new Database;
    // TODO: public HttpdBindService getHttpdBinds();

    // TODO: final DatabaseHttpdJBossSiteService httpdJBossSites = new Database;
    // TODO: public HttpdJBossSiteService getHttpdJBossSites();

    final DatabaseHttpdJBossVersionService httpdJBossVersions = new DatabaseHttpdJBossVersionService(this);
    @Override
    public HttpdJBossVersionService getHttpdJBossVersions() {
        return httpdJBossVersions;
    }

    final DatabaseHttpdJKCodeService httpdJKCodes = new DatabaseHttpdJKCodeService(this);
    @Override
    public HttpdJKCodeService getHttpdJKCodes() {
        return httpdJKCodes;
    }

    final DatabaseHttpdJKProtocolService httpdJKProtocols = new DatabaseHttpdJKProtocolService(this);
    @Override
    public HttpdJKProtocolService getHttpdJKProtocols() {
        return httpdJKProtocols;
    }

    final DatabaseHttpdServerService httpdServers = new DatabaseHttpdServerService(this);
    @Override
    public HttpdServerService getHttpdServers() {
        return httpdServers;
    }
    /* TODO
    final DatabaseHttpdSharedTomcatService httpdSharedTomcats = new Database;
    public HttpdSharedTomcatService getHttpdSharedTomcats();

    final DatabaseHttpdSiteAuthenticatedLocationService httpdSiteAuthenticatedLocations = new Database;
    public HttpdSiteAuthenticatedLocationService getHttpdSiteAuthenticatedLocations();

    final DatabaseHttpdSiteBindService httpdSiteBinds = new Database;
    public HttpdSiteBindService getHttpdSiteBinds();

    final DatabaseHttpdSiteURLService httpdSiteURLs = new Database;
    public HttpdSiteURLService getHttpdSiteURLs();
    */

    final DatabaseHttpdSiteService httpdSites = new DatabaseHttpdSiteService(this);
    @Override
    public HttpdSiteService getHttpdSites() {
        return httpdSites;
    }
    /* TODO
    // TODO: final DatabaseHttpdStaticSiteService httpdStaticSites = new Database;
    public HttpdStaticSiteService getHttpdStaticSites();

    // TODO: final DatabaseHttpdTomcatContextService httpdTomcatContexts = new Database;
    public HttpdTomcatContextService getHttpdTomcatContexts();

    // TODO: final DatabaseHttpdTomcatDataSourceService httpdTomcatDataSources = new Database;
    public HttpdTomcatDataSourceService getHttpdTomcatDataSources();

    // TODO: final DatabaseHttpdTomcatParameterService httpdTomcatParameters = new Database;
    public HttpdTomcatParameterService getHttpdTomcatParameters();

    // TODO: final DatabaseHttpdTomcatSiteService httpdTomcatSites = new Database;
    public HttpdTomcatSiteService getHttpdTomcatSites();

    // TODO: final DatabaseHttpdTomcatSharedSiteService httpdTomcatSharedSites = new Database;
    public HttpdTomcatSharedSiteService getHttpdTomcatSharedSites();

    // TODO: final DatabaseHttpdTomcatStdSiteService httpdTomcatStdSites = new Database;
    public HttpdTomcatStdSiteService getHttpdTomcatStdSites();
    */
    final DatabaseHttpdTomcatVersionService httpdTomcatVersions = new DatabaseHttpdTomcatVersionService(this);
    @Override
    public HttpdTomcatVersionService getHttpdTomcatVersions() {
        return httpdTomcatVersions;
    }

    // TODO: final DatabaseHttpdWorkerService httpdWorkers = new Database;
    // TODO: public HttpdWorkerService getHttpdWorkers();

    final DatabaseIPAddressService ipAddresses = new DatabaseIPAddressService(this);
    @Override
    public IPAddressService getIpAddresses() {
        return ipAddresses;
    }

    final DatabaseLanguageService languages = new DatabaseLanguageService(this);
    @Override
    public LanguageService getLanguages() {
        return languages;
    }

    // TODO: final DatabaseLinuxAccAddressService linuxAccAddresss = new Database;
    // TODO: public LinuxAccAddressService getLinuxAccAddresses();

    final DatabaseLinuxAccountGroupService linuxAccountGroups = new DatabaseLinuxAccountGroupService(this);
    @Override
    public LinuxAccountGroupService getLinuxAccountGroups() {
        return linuxAccountGroups;
    }

    final DatabaseLinuxAccountTypeService linuxAccountTypes = new DatabaseLinuxAccountTypeService(this);
    @Override
    public LinuxAccountTypeService getLinuxAccountTypes() {
        return linuxAccountTypes;
    }

    final DatabaseLinuxAccountService linuxAccounts = new DatabaseLinuxAccountService(this);
    @Override
    public LinuxAccountService getLinuxAccounts() {
        return linuxAccounts;
    }

    final DatabaseLinuxGroupTypeService linuxGroupTypes = new DatabaseLinuxGroupTypeService(this);
    @Override
    public LinuxGroupTypeService getLinuxGroupTypes() {
        return linuxGroupTypes;
    }

    final DatabaseLinuxGroupService linuxGroups = new DatabaseLinuxGroupService(this);
    @Override
    public LinuxGroupService getLinuxGroups() {
        return linuxGroups;
    }

    // TODO: final DatabaseMajordomoListService majordomoLists = new Database;
    // TODO: public MajordomoListService getMajordomoLists();

    // TODO: final DatabaseMajordomoServerService majordomoServers = new Database;
    // TODO: public MajordomoServerService getMajordomoServers();

    final DatabaseMajordomoVersionService majordomoVersions = new DatabaseMajordomoVersionService(this);
    @Override
    public MajordomoVersionService getMajordomoVersions() {
        return majordomoVersions;
    }

    final DatabaseMasterHostService masterHosts = new DatabaseMasterHostService(this);
    @Override
    public MasterHostService getMasterHosts() {
        return masterHosts;
    }

    final DatabaseMasterServerService masterServers = new DatabaseMasterServerService(this);
    @Override
    public MasterServerService getMasterServers() {
        return masterServers;
    }

    final DatabaseMasterUserService masterUsers = new DatabaseMasterUserService(this);
    @Override
    public MasterUserService getMasterUsers() {
        return masterUsers;
    }

    // TODO: final DatabaseMonthlyChargeService monthlyCharges = new Database;
    // TODO: public MonthlyChargeService getMonthlyCharges();

    final DatabaseMySQLDatabaseService mysqlDatabases = new DatabaseMySQLDatabaseService(this);
    @Override
    public MySQLDatabaseService getMysqlDatabases() {
        return mysqlDatabases;
    }

    final DatabaseMySQLDBUserService mysqlDBUsers = new DatabaseMySQLDBUserService(this);
    @Override
    public MySQLDBUserService getMysqlDBUsers() {
        return mysqlDBUsers;
    }

    final DatabaseMySQLServerService mysqlServers = new DatabaseMySQLServerService(this);
    @Override
    public MySQLServerService getMysqlServers() {
        return mysqlServers;
    }

    final DatabaseMySQLUserService mysqlUsers = new DatabaseMySQLUserService(this);
    @Override
    public MySQLUserService getMysqlUsers() {
        return mysqlUsers;
    }

    final DatabaseNetBindService netBinds = new DatabaseNetBindService(this);
    @Override
    public NetBindService getNetBinds() {
        return netBinds;
    }

    final DatabaseNetDeviceIDService netDeviceIDs = new DatabaseNetDeviceIDService(this);
    @Override
    public NetDeviceIDService getNetDeviceIDs() {
        return netDeviceIDs;
    }

    final DatabaseNetDeviceService netDevices = new DatabaseNetDeviceService(this);
    @Override
    public NetDeviceService getNetDevices() {
        return netDevices;
    }

    final DatabaseNetProtocolService netProtocols = new DatabaseNetProtocolService(this);
    @Override
    public NetProtocolService getNetProtocols() {
        return netProtocols;
    }

    final DatabaseNetTcpRedirectService netTcpRedirects = new DatabaseNetTcpRedirectService(this);
    @Override
    public NetTcpRedirectService getNetTcpRedirects() {
        return netTcpRedirects;
    }

    // TODO: final DatabaseNoticeLogService noticeLogs = new Database;
    // TODO: public NoticeLogService getNoticeLogs();

    final DatabaseNoticeTypeService noticeTypes = new DatabaseNoticeTypeService(this);
    @Override
    public NoticeTypeService getNoticeTypes() {
        return noticeTypes;
    }

    final DatabaseOperatingSystemVersionService operatingSystemVersions = new DatabaseOperatingSystemVersionService(this);
    @Override
    public OperatingSystemVersionService getOperatingSystemVersions() {
        return operatingSystemVersions;
    }

    final DatabaseOperatingSystemService operatingSystems = new DatabaseOperatingSystemService(this);
    @Override
    public OperatingSystemService getOperatingSystems() {
        return operatingSystems;
    }

    final DatabasePackageCategoryService packageCategories = new DatabasePackageCategoryService(this);
    @Override
    public PackageCategoryService getPackageCategories() {
        return packageCategories;
    }

    final DatabasePackageDefinitionBusinessService packageDefinitionBusinesses = new DatabasePackageDefinitionBusinessService(this);
    @Override
    public PackageDefinitionBusinessService getPackageDefinitionBusinesses() {
        return packageDefinitionBusinesses;
    }

    final DatabasePackageDefinitionLimitService packageDefinitionLimits = new DatabasePackageDefinitionLimitService(this);
    @Override
    public PackageDefinitionLimitService getPackageDefinitionLimits() {
        return packageDefinitionLimits;
    }

    final DatabasePackageDefinitionService packageDefinitions = new DatabasePackageDefinitionService(this);
    @Override
    public PackageDefinitionService getPackageDefinitions() {
        return packageDefinitions;
    }

    final DatabasePaymentTypeService paymentTypes = new DatabasePaymentTypeService(this);
    @Override
    public PaymentTypeService getPaymentTypes() {
        return paymentTypes;
    }

    final DatabasePhysicalServerService physicalServers = new DatabasePhysicalServerService(this);
    @Override
    public PhysicalServerService getPhysicalServers() {
        return physicalServers;
    }

    final DatabasePostgresDatabaseService postgresDatabases = new DatabasePostgresDatabaseService(this);
    @Override
    public PostgresDatabaseService getPostgresDatabases() {
        return postgresDatabases;
    }

    final DatabasePostgresEncodingService postgresEncodings = new DatabasePostgresEncodingService(this);
    @Override
    public PostgresEncodingService getPostgresEncodings() {
        return postgresEncodings;
    }

    final DatabasePostgresServerService postgresServers = new DatabasePostgresServerService(this);
    @Override
    public PostgresServerService getPostgresServers() {
        return postgresServers;
    }

    final DatabasePostgresUserService postgresUsers = new DatabasePostgresUserService(this);
    @Override
    public PostgresUserService getPostgresUsers() {
        return postgresUsers;
    }

    final DatabasePostgresVersionService postgresVersions = new DatabasePostgresVersionService(this);
    @Override
    public PostgresVersionService getPostgresVersions() {
        return postgresVersions;
    }

    final DatabasePrivateFtpServerService privateFtpServers = new DatabasePrivateFtpServerService(this);
    @Override
    public PrivateFtpServerService getPrivateFtpServers() {
        return privateFtpServers;
    }

    final DatabaseProcessorTypeService processorTypes = new DatabaseProcessorTypeService(this);
    @Override
    public ProcessorTypeService getProcessorTypes() {
        return processorTypes;
    }

    final DatabaseProtocolService protocols = new DatabaseProtocolService(this);
    @Override
    public ProtocolService getProtocols() {
        return protocols;
    }

    final DatabaseRackService racks = new DatabaseRackService(this);
    @Override
    public RackService getRacks() {
        return racks;
    }

    final DatabaseResellerService resellers = new DatabaseResellerService(this);
    @Override
    public ResellerService getResellers() {
        return resellers;
    }

    final DatabaseResourceTypeService resourceTypes = new DatabaseResourceTypeService(this);
    @Override
    public ResourceTypeService getResourceTypes() {
        return resourceTypes;
    }

    final DatabaseServerFarmService serverFarms = new DatabaseServerFarmService(this);
    @Override
    public ServerFarmService getServerFarms() {
        return serverFarms;
    }

    final DatabaseShellService shells = new DatabaseShellService(this);
    @Override
    public ShellService getShells() {
        return shells;
    }
    /* TODO
    final DatabaseSignupRequestOptionService signupRequestOptions = new Database;
    public SignupRequestOptionService getSignupRequestOptions();

    final DatabaseSignupRequestService signupRequests = new Database;
    public SignupRequestService getSignupRequests();

    final DatabaseSpamEmailMessageService spamEmailMessages = new Database;
    public SpamEmailMessageService getSpamEmailMessages();

    final DatabaseSystemEmailAliasService systemEmailAliass = new Database;
    public SystemEmailAliasService getSystemEmailAliases();
    */
    final DatabaseTechnologyService technologies = new DatabaseTechnologyService(this);
    @Override
    public TechnologyService getTechnologies() {
        return technologies;
    }

    final DatabaseTechnologyClassService technologyClasses = new DatabaseTechnologyClassService(this);
    @Override
    public TechnologyClassService getTechnologyClasses() {
        return technologyClasses;
    }

    final DatabaseTechnologyNameService technologyNames = new DatabaseTechnologyNameService(this);
    @Override
    public TechnologyNameService getTechnologyNames() {
        return technologyNames;
    }

    final DatabaseTechnologyVersionService technologyVersions = new DatabaseTechnologyVersionService(this);
    @Override
    public TechnologyVersionService getTechnologyVersions() {
        return technologyVersions;
    }

    final DatabaseTicketActionTypeService ticketActionTypes = new DatabaseTicketActionTypeService(this);
    @Override
    public TicketActionTypeService getTicketActionTypes() {
        return ticketActionTypes;
    }

    final DatabaseTicketActionService ticketActions = new DatabaseTicketActionService(this);
    @Override
    public TicketActionService getTicketActions() {
        return ticketActions;
    }

    final DatabaseTicketAssignmentService ticketAssignments = new DatabaseTicketAssignmentService(this);
    @Override
    public TicketAssignmentService getTicketAssignments() {
        return ticketAssignments;
    }

    // TODO: final DatabaseTicketBrandCategoryService ticketBrandCategories = new Database;
    // TODO: public TicketBrandCategoryService getTicketBrandCategories();

    final DatabaseTicketCategoryService ticketCategories = new DatabaseTicketCategoryService(this);
    @Override
    public TicketCategoryService getTicketCategories() {
        return ticketCategories;
    }

    final DatabaseTicketPriorityService ticketPriorities = new DatabaseTicketPriorityService(this);
    @Override
    public TicketPriorityService getTicketPriorities() {
        return ticketPriorities;
    }

    final DatabaseTicketStatusService ticketStatuses = new DatabaseTicketStatusService(this);
    @Override
    public TicketStatusService getTicketStatuses() {
        return ticketStatuses;
    }

    final DatabaseTicketTypeService ticketTypes = new DatabaseTicketTypeService(this);
    @Override
    public TicketTypeService getTicketTypes() {
        return ticketTypes;
    }

    final DatabaseTicketService tickets = new DatabaseTicketService(this);
    @Override
    public TicketService getTickets() {
        return tickets;
    }

    final DatabaseTimeZoneService timeZones = new DatabaseTimeZoneService(this);
    @Override
    public TimeZoneService getTimeZones() {
        return timeZones;
    }

    final DatabaseTransactionTypeService transactionTypes = new DatabaseTransactionTypeService(this);
    @Override
    public TransactionTypeService getTransactionTypes() {
        return transactionTypes;
    }

    final DatabaseTransactionService transactions = new DatabaseTransactionService(this);
    @Override
    public TransactionService getTransactions() {
        return transactions;
    }
    /* TODO
    // TODO: final DatabaseUSStateService usStates = new Database;
    public USStateService getUsStates();
    */
    final DatabaseUsernameService usernames = new DatabaseUsernameService(this);
    @Override
    public UsernameService getUsernames() {
        return usernames;
    }
    /* TODO
    // TODO: final DatabaseVirtualDiskService virtualDisks = new Database;
    public VirtualDiskService getVirtualDisks();
     */
    DatabaseVirtualServerService virtualServers = new DatabaseVirtualServerService(this);
    @Override
    public VirtualServerService getVirtualServers() {
        return virtualServers;
    }
    /* TODO
    // TODO: final DatabaseWhoisHistoryService whoisHistories = new Database;
    public WhoisHistoryService getWhoisHistory();
     */
    // </editor-fold>
}
