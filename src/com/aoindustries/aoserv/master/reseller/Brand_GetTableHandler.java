/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.reseller;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.reseller.Brand;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Brand_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.BRANDS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Brand(),
			"select * from reseller.\"Brand\""
		);
	}

	/**
	 * Limited access to its own brand only.
	 */
	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Brand(),
			"select\n"
			+ "  br.accounting,\n"
			+ "  br.nameserver1,\n"
			+ "  br.nameserver2,\n"
			+ "  br.nameserver3,\n"
			+ "  br.nameserver4,\n"
			+ "  br.smtp_linux_server_account,\n"
			+ "  null::text,\n" // smtp_host
			+ "  ?,\n" // smtp_password
			+ "  br.imap_linux_server_account,\n"
			+ "  null::text,\n" // imap_host
			+ "  ?,\n" // imap_password
			+ "  br.support_email_address,\n"
			+ "  br.support_email_display,\n"
			+ "  br.signup_email_address,\n"
			+ "  br.signup_email_display,\n"
			+ "  br.ticket_encryption_from,\n"
			+ "  br.ticket_encryption_recipient,\n"
			+ "  br.signup_encryption_from,\n"
			+ "  br.signup_encryption_recipient,\n"
			+ "  br.support_toll_free,\n"
			+ "  br.support_day_phone,\n"
			+ "  br.support_emergency_phone1,\n"
			+ "  br.support_emergency_phone2,\n"
			+ "  br.support_fax,\n"
			+ "  br.support_mailing_address1,\n"
			+ "  br.support_mailing_address2,\n"
			+ "  br.support_mailing_address3,\n"
			+ "  br.support_mailing_address4,\n"
			+ "  br.english_enabled,\n"
			+ "  br.japanese_enabled,\n"
			+ "  br.aoweb_struts_http_url_base,\n"
			+ "  br.aoweb_struts_https_url_base,\n"
			+ "  br.aoweb_struts_google_verify_content,\n"
			+ "  br.aoweb_struts_noindex,\n"
			+ "  br.aoweb_struts_google_analytics_new_tracking_code,\n"
			+ "  br.aoweb_struts_signup_admin_address,\n"
			+ "  br.aoweb_struts_vnc_bind,\n"
			+ "  ?,\n" // aoweb_struts_keystore_type
			+ "  ?\n" // aoweb_struts_keystore_password
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  reseller.\"Brand\" br\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=br.accounting",
			AoservProtocol.FILTERED,
			AoservProtocol.FILTERED,
			AoservProtocol.FILTERED,
			AoservProtocol.FILTERED,
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Brand(),
			"select\n"
			+ "  br.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  reseller.\"Brand\" br\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  ) and bu1.accounting=br.accounting",
			source.getCurrentAdministrator()
		);
	}
}
