/*
 * Copyright 2013, 2015, 2017 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.FirewalldZoneName;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.aoserv.client.validator.MySQLDatabaseName;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.PostgresDatabaseName;
import com.aoindustries.aoserv.client.validator.PostgresUserId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.dbc.ObjectFactory;
import com.aoindustries.net.DomainName;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.net.Protocol;
import com.aoindustries.validation.ValidationException;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static final ObjectFactory<AccountingCode> accountingCodeFactory = (ResultSet result) -> {
		try {
			return AccountingCode.valueOf(result.getString(1));
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

    public static final ObjectFactory<GroupId> groupIdFactory = (ResultSet result) -> {
		try {
			return GroupId.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

    public static final ObjectFactory<FirewalldZoneName> firewalldZoneNameFactory = (ResultSet result) -> {
		try {
			return FirewalldZoneName.valueOf(result.getString(1));
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

    public static final ObjectFactory<MySQLDatabaseName> mySQLDatabaseNameFactory = (ResultSet result) -> {
		try {
			return MySQLDatabaseName.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

    public static final ObjectFactory<MySQLUserId> mySQLUserIdFactory = (ResultSet result) -> {
		try {
			return MySQLUserId.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

    public static final ObjectFactory<PostgresDatabaseName> postgresDatabaseNameFactory = (ResultSet result) -> {
		try {
			return PostgresDatabaseName.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

    public static final ObjectFactory<PostgresUserId> postgresUserIdFactory = (ResultSet result) -> {
		try {
			return PostgresUserId.valueOf(result.getString(1));
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

    public static final ObjectFactory<UserId> userIdFactory = (ResultSet result) -> {
		try {
			return UserId.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};

    public static final ObjectFactory<UnixPath> unixPathFactory = (ResultSet result) -> {
		try {
			return UnixPath.valueOf(result.getString(1));
		} catch(ValidationException e) {
			throw new SQLException(e.getLocalizedMessage(), e);
		}
	};
}
