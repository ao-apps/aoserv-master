/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.ObjectFactory;
import com.aoapps.lang.i18n.Money;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.DomainName;
import com.aoapps.net.Email;
import com.aoapps.net.HostAddress;
import com.aoapps.net.InetAddress;
import com.aoapps.net.Port;
import com.aoapps.net.Protocol;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.net.FirewallZone;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Currency;
import java.util.Locale;

/**
 * A set of object factories for various types.
 *
 * @author  AO Industries, Inc.
 */
public final class ObjectFactories {

	/**
	 * Make no instances.
	 */
	private ObjectFactories() {
	}

	public static final ObjectFactory<Account.Name> accountNameFactory = (ResultSet result) -> {
		try {
			return Account.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<DomainName> domainNameFactory = (ResultSet result) -> {
		try {
			return DomainName.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Email> emailFactory = (ResultSet result) -> {
		try {
			return Email.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Group.Name> groupNameFactory = (ResultSet result) -> {
		try {
			return Group.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<FirewallZone.Name> firewallZoneNameFactory = (ResultSet result) -> {
		try {
			return FirewallZone.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<HostAddress> hostAddressFactory = (ResultSet result) -> {
		try {
			return HostAddress.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<InetAddress> inetAddressFactory = (ResultSet result) -> {
		try {
			return InetAddress.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.linux.User.Name> linuxUserNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.linux.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Money> moneyFactory = (ResultSet result) -> {
		try {
			return new Money(Currency.getInstance(result.getString(1)), result.getBigDecimal(2));
		} catch(NumberFormatException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.Database.Name> mysqlDatabaseNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.mysql.Database.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	/**
	 * Requires "mysqlServerName".
	 */
	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.Server.Name> mysqlServerNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.mysql.Server.Name.valueOf(result.getString("mysqlServerName"));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.User.Name> mysqlUserNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.mysql.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.postgresql.Database.Name> postgresqlDatabaseNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.postgresql.Database.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.postgresql.User.Name> postgresqlUserNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	/**
	 * Requires "port" and "net_protocol".
	 */
	public static final ObjectFactory<Port> portFactory = (ResultSet result) -> {
		try {
			return Port.valueOf(
				result.getInt("port"),
				Protocol.valueOf(result.getString("net_protocol").toUpperCase(Locale.ROOT))
			);
		} catch(IllegalArgumentException | ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.account.User.Name> userNameFactory = (ResultSet result) -> {
		try {
			return com.aoindustries.aoserv.client.account.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<PosixPath> posixPathFactory = (ResultSet result) -> {
		try {
			return PosixPath.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};
}
