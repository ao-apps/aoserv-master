/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2021  AO Industries, Inc.
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
module com.aoindustries.aoserv.master {
	exports com.aoindustries.aoserv.master;
	exports com.aoindustries.aoserv.master.account;
	exports com.aoindustries.aoserv.master.accounting;
	exports com.aoindustries.aoserv.master.aosh;
	exports com.aoindustries.aoserv.master.backup;
	exports com.aoindustries.aoserv.master.billing;
	exports com.aoindustries.aoserv.master.cluster;
	exports com.aoindustries.aoserv.master.distribution;
	exports com.aoindustries.aoserv.master.distribution.management;
	exports com.aoindustries.aoserv.master.dns;
	exports com.aoindustries.aoserv.master.email;
	exports com.aoindustries.aoserv.master.ftp;
	exports com.aoindustries.aoserv.master.infrastructure;
	exports com.aoindustries.aoserv.master.linux;
	exports com.aoindustries.aoserv.master.master;
	exports com.aoindustries.aoserv.master.mysql;
	exports com.aoindustries.aoserv.master.net;
	exports com.aoindustries.aoserv.master.net.monitoring;
	exports com.aoindustries.aoserv.master.net.reputation;
	exports com.aoindustries.aoserv.master.payment;
	exports com.aoindustries.aoserv.master.pki;
	exports com.aoindustries.aoserv.master.postgresql;
	exports com.aoindustries.aoserv.master.reseller;
	exports com.aoindustries.aoserv.master.schema;
	exports com.aoindustries.aoserv.master.scm;
	exports com.aoindustries.aoserv.master.signup;
	exports com.aoindustries.aoserv.master.ticket;
	exports com.aoindustries.aoserv.master.tlds;
	exports com.aoindustries.aoserv.master.web;
	exports com.aoindustries.aoserv.master.web.jboss;
	exports com.aoindustries.aoserv.master.web.tomcat;
	uses com.aoindustries.aoserv.master.MasterService;
	uses com.aoindustries.aoserv.master.TableHandler.GetObjectHandler;
	uses com.aoindustries.aoserv.master.TableHandler.GetTableHandler;
	provides com.aoindustries.aoserv.master.MasterService with
		com.aoindustries.aoserv.master.billing.WhoisHistoryService,
		com.aoindustries.aoserv.master.dns.DnsService,
		com.aoindustries.aoserv.master.dns.ZoneService,
		com.aoindustries.aoserv.master.email.DomainService,
		com.aoindustries.aoserv.master.pki.CertificateNameService,
		com.aoindustries.aoserv.master.tlds.TopLevelDomainService,
		com.aoindustries.aoserv.master.web.VirtualHostNameService;
	provides com.aoindustries.aoserv.master.TableHandler.GetObjectHandler with
		com.aoindustries.aoserv.master.accounting.BankTransactionHandler.GetObject,
		com.aoindustries.aoserv.master.backup.BackupReportHandler.GetObject,
		com.aoindustries.aoserv.master.billing.TransactionHandler.GetObject,
		com.aoindustries.aoserv.master.email.SpamMessageHandler.GetObject;
	provides com.aoindustries.aoserv.master.TableHandler.GetTableHandler with
		com.aoindustries.aoserv.master.account.Account_GetTableHandler,
		com.aoindustries.aoserv.master.account.AccountHost_GetTableHandler,
		com.aoindustries.aoserv.master.account.Administrator_GetTableHandler,
		com.aoindustries.aoserv.master.account.DisableLog_GetTableHandler,
		com.aoindustries.aoserv.master.account.Profile_GetTableHandler,
		com.aoindustries.aoserv.master.account.UsState_GetTableHandler,
		com.aoindustries.aoserv.master.account.User_GetTableHandler,
		com.aoindustries.aoserv.master.accounting.Bank_GetTableHandler,
		com.aoindustries.aoserv.master.accounting.BankAccount_GetTableHandler,
		com.aoindustries.aoserv.master.accounting.BankTransactionHandler.GetTable,
		com.aoindustries.aoserv.master.accounting.BankTransactionType_GetTableHandler,
		com.aoindustries.aoserv.master.accounting.ExpenseCategory_GetTableHandler,
		com.aoindustries.aoserv.master.aosh.Command_GetTableHandler,
		com.aoindustries.aoserv.master.backup.BackupPartition_GetTableHandler,
		com.aoindustries.aoserv.master.backup.BackupReportHandler.GetTable,
		com.aoindustries.aoserv.master.backup.BackupRetention_GetTableHandler,
		com.aoindustries.aoserv.master.backup.FileReplication_GetTableHandler,
		com.aoindustries.aoserv.master.backup.FileReplicationLog_GetTableHandler,
		com.aoindustries.aoserv.master.backup.FileReplicationSchedule_GetTableHandler,
		com.aoindustries.aoserv.master.backup.FileReplicationSetting_GetTableHandler,
		com.aoindustries.aoserv.master.backup.MysqlReplication_GetTableHandler,
		com.aoindustries.aoserv.master.billing.Currency_GetTableHandler,
		com.aoindustries.aoserv.master.billing.MonthlyCharge_GetTableHandler,
		com.aoindustries.aoserv.master.billing.NoticeLog_GetTableHandler,
		com.aoindustries.aoserv.master.billing.NoticeLogBalance_GetTableHandler,
		com.aoindustries.aoserv.master.billing.NoticeType_GetTableHandler,
		com.aoindustries.aoserv.master.billing.Package_GetTableHandler,
		com.aoindustries.aoserv.master.billing.PackageCategory_GetTableHandler,
		com.aoindustries.aoserv.master.billing.PackageDefinition_GetTableHandler,
		com.aoindustries.aoserv.master.billing.PackageDefinitionLimit_GetTableHandler,
		com.aoindustries.aoserv.master.billing.Resource_GetTableHandler,
		com.aoindustries.aoserv.master.billing.TransactionHandler.GetTable,
		com.aoindustries.aoserv.master.billing.TransactionType_GetTableHandler,
		com.aoindustries.aoserv.master.billing.WhoisHistoryAccount_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.Architecture_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.OperatingSystem_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.OperatingSystemVersion_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.Software_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.SoftwareCategorization_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.SoftwareCategory_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.SoftwareVersion_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.management.DistroFile_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.management.DistroFileType_GetTableHandler,
		com.aoindustries.aoserv.master.distribution.management.DistroReportType_GetTableHandler,
		com.aoindustries.aoserv.master.dns.ForbiddenZone_GetTableHandler,
		com.aoindustries.aoserv.master.dns.Record_GetTableHandler,
		com.aoindustries.aoserv.master.dns.RecordType_GetTableHandler,
		com.aoindustries.aoserv.master.dns.TopLevelDomain_GetTableHandler,
		com.aoindustries.aoserv.master.email.Address_GetTableHandler,
		com.aoindustries.aoserv.master.email.AttachmentBlock_GetTableHandler,
		com.aoindustries.aoserv.master.email.AttachmentType_GetTableHandler,
		com.aoindustries.aoserv.master.email.BlackholeAddress_GetTableHandler,
		com.aoindustries.aoserv.master.email.CyrusImapdBind_GetTableHandler,
		com.aoindustries.aoserv.master.email.CyrusImapdServer_GetTableHandler,
		com.aoindustries.aoserv.master.email.Forwarding_GetTableHandler,
		com.aoindustries.aoserv.master.email.InboxAddress_GetTableHandler,
		com.aoindustries.aoserv.master.email.List_GetTableHandler,
		com.aoindustries.aoserv.master.email.ListAddress_GetTableHandler,
		com.aoindustries.aoserv.master.email.MajordomoList_GetTableHandler,
		com.aoindustries.aoserv.master.email.MajordomoServer_GetTableHandler,
		com.aoindustries.aoserv.master.email.MajordomoVersion_GetTableHandler,
		com.aoindustries.aoserv.master.email.Pipe_GetTableHandler,
		com.aoindustries.aoserv.master.email.PipeAddress_GetTableHandler,
		com.aoindustries.aoserv.master.email.SendmailBind_GetTableHandler,
		com.aoindustries.aoserv.master.email.SendmailServer_GetTableHandler,
		com.aoindustries.aoserv.master.email.SmtpRelay_GetTableHandler,
		com.aoindustries.aoserv.master.email.SmtpRelayType_GetTableHandler,
		com.aoindustries.aoserv.master.email.SmtpSmartHost_GetTableHandler,
		com.aoindustries.aoserv.master.email.SmtpSmartHostDomain_GetTableHandler,
		com.aoindustries.aoserv.master.email.SpamAssassinMode_GetTableHandler,
		com.aoindustries.aoserv.master.email.SpamMessageHandler.GetTable,
		com.aoindustries.aoserv.master.email.SystemAlias_GetTableHandler,
		com.aoindustries.aoserv.master.ftp.GuestUser_GetTableHandler,
		com.aoindustries.aoserv.master.ftp.PrivateServer_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.PhysicalServer_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.ProcessorType_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.Rack_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.ServerFarm_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.VirtualDisk_GetTableHandler,
		com.aoindustries.aoserv.master.infrastructure.VirtualServer_GetTableHandler,
		com.aoindustries.aoserv.master.linux.DaemonAcl_GetTableHandler,
		com.aoindustries.aoserv.master.linux.Group_GetTableHandler,
		com.aoindustries.aoserv.master.linux.GroupServer_GetTableHandler,
		com.aoindustries.aoserv.master.linux.GroupType_GetTableHandler,
		com.aoindustries.aoserv.master.linux.GroupUser_GetTableHandler,
		com.aoindustries.aoserv.master.linux.Server_GetTableHandler,
		com.aoindustries.aoserv.master.linux.Shell_GetTableHandler,
		com.aoindustries.aoserv.master.linux.TimeZone_GetTableHandler,
		com.aoindustries.aoserv.master.linux.User_GetTableHandler,
		com.aoindustries.aoserv.master.linux.UserServer_GetTableHandler,
		com.aoindustries.aoserv.master.linux.UserType_GetTableHandler,
		com.aoindustries.aoserv.master.master.AdministratorPermission_GetTableHandler,
		com.aoindustries.aoserv.master.master.Permission_GetTableHandler,
		com.aoindustries.aoserv.master.master.Process_GetTableHandler,
		com.aoindustries.aoserv.master.master.ServerStat_GetTableHandler,
		com.aoindustries.aoserv.master.master.User_GetTableHandler,
		com.aoindustries.aoserv.master.master.UserAcl_GetTableHandler,
		com.aoindustries.aoserv.master.master.UserHost_GetTableHandler,
		com.aoindustries.aoserv.master.mysql.Database_GetTableHandler,
		com.aoindustries.aoserv.master.mysql.DatabaseUser_GetTableHandler,
		com.aoindustries.aoserv.master.mysql.Server_GetTableHandler,
		com.aoindustries.aoserv.master.mysql.User_GetTableHandler,
		com.aoindustries.aoserv.master.mysql.UserServer_GetTableHandler,
		com.aoindustries.aoserv.master.net.AppProtocol_GetTableHandler,
		com.aoindustries.aoserv.master.net.Bind_GetTableHandler,
		com.aoindustries.aoserv.master.net.BindFirewallZone_GetTableHandler,
		com.aoindustries.aoserv.master.net.Device_GetTableHandler,
		com.aoindustries.aoserv.master.net.DeviceId_GetTableHandler,
		com.aoindustries.aoserv.master.net.FirewallZone_GetTableHandler,
		com.aoindustries.aoserv.master.net.Host_GetTableHandler,
		com.aoindustries.aoserv.master.net.IpAddress_GetTableHandler,
		com.aoindustries.aoserv.master.net.TcpRedirect_GetTableHandler,
		com.aoindustries.aoserv.master.net.monitoring.IpAddressMonitoring_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.Host_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.Limiter_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.LimiterClass_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.LimiterSet_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.Network_GetTableHandler,
		com.aoindustries.aoserv.master.net.reputation.Set_GetTableHandler,
		com.aoindustries.aoserv.master.payment.CountryCode_GetTableHandler,
		com.aoindustries.aoserv.master.payment.CreditCard_GetTableHandler,
		com.aoindustries.aoserv.master.payment.Payment_GetTableHandler,
		com.aoindustries.aoserv.master.payment.PaymentType_GetTableHandler,
		com.aoindustries.aoserv.master.payment.Processor_GetTableHandler,
		com.aoindustries.aoserv.master.pki.Certificate_GetTableHandler,
		com.aoindustries.aoserv.master.pki.CertificateOtherUse_GetTableHandler,
		com.aoindustries.aoserv.master.pki.EncryptionKey_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.Database_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.Encoding_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.Server_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.User_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.UserServer_GetTableHandler,
		com.aoindustries.aoserv.master.postgresql.Version_GetTableHandler,
		com.aoindustries.aoserv.master.reseller.Brand_GetTableHandler,
		com.aoindustries.aoserv.master.reseller.BrandCategory_GetTableHandler,
		com.aoindustries.aoserv.master.reseller.Category_GetTableHandler,
		com.aoindustries.aoserv.master.reseller.Reseller_GetTableHandler,
		com.aoindustries.aoserv.master.schema.AoservProtocol_GetTableHandler,
		com.aoindustries.aoserv.master.schema.Column_GetTableHandler,
		com.aoindustries.aoserv.master.schema.ForeignKey_GetTableHandler,
		com.aoindustries.aoserv.master.schema.Table_GetTableHandler,
		com.aoindustries.aoserv.master.schema.Type_GetTableHandler,
		com.aoindustries.aoserv.master.scm.CvsRepository_GetTableHandler,
		com.aoindustries.aoserv.master.signup.Option_GetTableHandler,
		com.aoindustries.aoserv.master.signup.Request_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Action_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.ActionType_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Assignment_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Language_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Priority_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Status_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.Ticket_GetTableHandler,
		com.aoindustries.aoserv.master.ticket.TicketType_GetTableHandler,
		com.aoindustries.aoserv.master.web.Header_GetTableHandler,
		com.aoindustries.aoserv.master.web.HttpdBind_GetTableHandler,
		com.aoindustries.aoserv.master.web.HttpdServer_GetTableHandler,
		com.aoindustries.aoserv.master.web.Location_GetTableHandler,
		com.aoindustries.aoserv.master.web.RewriteRule_GetTableHandler,
		com.aoindustries.aoserv.master.web.Site_GetTableHandler,
		com.aoindustries.aoserv.master.web.StaticSite_GetTableHandler,
		com.aoindustries.aoserv.master.web.VirtualHost_GetTableHandler,
		com.aoindustries.aoserv.master.web.jboss.Site_GetTableHandler,
		com.aoindustries.aoserv.master.web.jboss.Version_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.Context_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.ContextDataSource_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.ContextParameter_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.JkMount_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.JkProtocol_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.PrivateTomcatSite_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.SharedTomcat_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.SharedTomcatSite_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.Site_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.Version_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.Worker_GetTableHandler,
		com.aoindustries.aoserv.master.web.tomcat.WorkerName_GetTableHandler;
	// Direct
	requires com.aoapps.collections; // <groupId>com.aoapps</groupId><artifactId>ao-collections</artifactId>
	requires com.aoapps.cron; // <groupId>com.aoapps</groupId><artifactId>ao-cron</artifactId>
	requires com.aoapps.dbc; // <groupId>com.aoapps</groupId><artifactId>ao-dbc</artifactId>
	requires com.aoapps.hodgepodge; // <groupId>com.aoapps</groupId><artifactId>ao-hodgepodge</artifactId>
	requires com.aoapps.lang; // <groupId>com.aoapps</groupId><artifactId>ao-lang</artifactId>
	requires com.aoapps.net.types; // <groupId>com.aoapps</groupId><artifactId>ao-net-types</artifactId>
	requires com.aoapps.payments.api; // <groupId>com.aoapps</groupId><artifactId>ao-payments-api</artifactId>
	requires com.aoapps.security; // <groupId>com.aoapps</groupId><artifactId>ao-security</artifactId>
	requires com.aoapps.sql; // <groupId>com.aoapps</groupId><artifactId>ao-sql</artifactId>
	requires com.aoapps.sql.pool; // <groupId>com.aoapps</groupId><artifactId>ao-sql-pool</artifactId>
	requires com.aoapps.tlds; // <groupId>com.aoapps</groupId><artifactId>ao-tlds</artifactId>
	requires com.aoindustries.aoserv.client; // <groupId>com.aoindustries</groupId><artifactId>aoserv-client</artifactId>
	requires com.aoindustries.aoserv.daemon.client; // <groupId>com.aoindustries</groupId><artifactId>aoserv-daemon-client</artifactId>
	// Non-Java: <groupId>com.aoindustries</groupId><artifactId>aoserv-schema</artifactId><classifier>sql</classifier>
	requires org.apache.commons.lang3; // <groupId>org.apache.commons</groupId><artifactId>commons-lang3</artifactId>
	// Java SE
	requires java.logging;
	requires java.sql;
}
