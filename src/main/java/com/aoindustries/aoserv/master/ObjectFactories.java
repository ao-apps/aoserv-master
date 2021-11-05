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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
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
import java.sql.SQLException;
import java.util.Currency;
import java.util.Locale;

/**
 * A set of object factories for various types.
 *
 * @author  AO Industries, Inc.
 */
public abstract class ObjectFactories {

	/** Make no instances. */
	private ObjectFactories() {throw new AssertionError();}

	public static final ObjectFactory<Account.Name> accountNameFactory = result -> {
		try {
			return Account.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<DomainName> domainNameFactory = result -> {
		try {
			return DomainName.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Email> emailFactory = result -> {
		try {
			return Email.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Group.Name> groupNameFactory = result -> {
		try {
			return Group.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<FirewallZone.Name> firewallZoneNameFactory = result -> {
		try {
			return FirewallZone.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<HostAddress> hostAddressFactory = result -> {
		try {
			return HostAddress.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<InetAddress> inetAddressFactory = result -> {
		try {
			return InetAddress.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.linux.User.Name> linuxUserNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.linux.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<Money> moneyFactory = result -> {
		try {
			return new Money(Currency.getInstance(result.getString(1)), result.getBigDecimal(2));
		} catch(NumberFormatException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.Database.Name> mysqlDatabaseNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.mysql.Database.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	/**
	 * Requires "mysqlServerName".
	 */
	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.Server.Name> mysqlServerNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.mysql.Server.Name.valueOf(result.getString("mysqlServerName"));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.mysql.User.Name> mysqlUserNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.mysql.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.postgresql.Database.Name> postgresqlDatabaseNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.postgresql.Database.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.postgresql.User.Name> postgresqlUserNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.postgresql.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	/**
	 * Requires "port" and "net_protocol".
	 */
	public static final ObjectFactory<Port> portFactory = result -> {
		try {
			return Port.valueOf(
				result.getInt("port"),
				Protocol.valueOf(result.getString("net_protocol").toUpperCase(Locale.ROOT))
			);
		} catch(IllegalArgumentException | ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<com.aoindustries.aoserv.client.account.User.Name> userNameFactory = result -> {
		try {
			return com.aoindustries.aoserv.client.account.User.Name.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

	public static final ObjectFactory<PosixPath> posixPathFactory = result -> {
		try {
			return PosixPath.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};
}
