/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master.signup;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.signup.Request;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Request_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SIGNUP_REQUESTS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Request(),
			"select\n"
			+ "  id\n"
			+ "  brand,\n"
			+ "  \"time\",\n"
			+ "  host(ip_address) as ip_address,\n"
			+ "  package_definition,\n"
			+ "  business_name,\n"
			+ "  business_phone,\n"
			+ "  business_fax,\n"
			+ "  business_address1,\n"
			+ "  business_address2,\n"
			+ "  business_city,\n"
			+ "  business_state,\n"
			+ "  business_country,\n"
			+ "  business_zip,\n"
			+ "  ba_name,\n"
			+ "  ba_title,\n"
			+ "  ba_work_phone,\n"
			+ "  ba_cell_phone,\n"
			+ "  ba_home_phone,\n"
			+ "  ba_fax,\n"
			+ "  ba_email,\n"
			+ "  ba_address1,\n"
			+ "  ba_address2,\n"
			+ "  ba_city,\n"
			+ "  ba_state,\n"
			+ "  ba_country,\n"
			+ "  ba_zip,\n"
			+ "  ba_username,\n"
			+ "  billing_contact,\n"
			+ "  billing_email,\n"
			+ "  billing_use_monthly,\n"
			+ "  billing_pay_one_year,\n"
			+ "  encrypted_data,\n"
			+ "  encryption_from,\n"
			+ "  encryption_recipient,\n"
			+ "  completed_by,\n"
			+ "  completed_time\n"
			+ "from\n"
			+ "  signup.\"Request\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Request(),
			"select\n"
			+ "  sr.id\n"
			+ "  sr.brand,\n"
			+ "  sr.\"time\",\n"
			+ "  host(sr.ip_address) as ip_address,\n"
			+ "  sr.package_definition,\n"
			+ "  sr.business_name,\n"
			+ "  sr.business_phone,\n"
			+ "  sr.business_fax,\n"
			+ "  sr.business_address1,\n"
			+ "  sr.business_address2,\n"
			+ "  sr.business_city,\n"
			+ "  sr.business_state,\n"
			+ "  sr.business_country,\n"
			+ "  sr.business_zip,\n"
			+ "  sr.ba_name,\n"
			+ "  sr.ba_title,\n"
			+ "  sr.ba_work_phone,\n"
			+ "  sr.ba_cell_phone,\n"
			+ "  sr.ba_home_phone,\n"
			+ "  sr.ba_fax,\n"
			+ "  sr.ba_email,\n"
			+ "  sr.ba_address1,\n"
			+ "  sr.ba_address2,\n"
			+ "  sr.ba_city,\n"
			+ "  sr.ba_state,\n"
			+ "  sr.ba_country,\n"
			+ "  sr.ba_zip,\n"
			+ "  sr.ba_username,\n"
			+ "  sr.billing_contact,\n"
			+ "  sr.billing_email,\n"
			+ "  sr.billing_use_monthly,\n"
			+ "  sr.billing_pay_one_year,\n"
			+ "  sr.encrypted_data,\n"
			+ "  sr.encryption_from,\n"
			+ "  sr.encryption_recipient,\n"
			+ "  sr.completed_by,\n"
			+ "  sr.completed_time\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  signup.\"Request\" sr\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=sr.brand",
			source.getCurrentAdministrator()
		);
	}
}
