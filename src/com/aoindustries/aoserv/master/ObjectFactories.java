/*
 * Copyright 2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.net.FirewallZone;
import com.aoindustries.dbc.ObjectFactory;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.Email;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.net.Protocol;
import com.aoindustries.util.i18n.Money;
import com.aoindustries.validation.ValidationException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Currency;
import java.util.Locale;

/**
 * A set of object factories for various types.
 *
 * @author  AO Industries, Inc.
 */
final public class ObjectFactories {
    
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

    public static final ObjectFactory<Port> portFactory = (ResultSet result) -> {
		try {
			return Port.valueOf(
				result.getInt(1),
				Protocol.valueOf(result.getString(2).toUpperCase(Locale.ROOT))
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
