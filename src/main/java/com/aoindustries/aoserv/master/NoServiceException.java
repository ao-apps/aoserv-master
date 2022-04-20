/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.lang.Throwables;

/**
 * @author  AO Industries, Inc.
 */
public class NoServiceException extends MasterServiceException {

  private static final long serialVersionUID = 1L;

  public NoServiceException(String message) {
    super(message);
  }

  public NoServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoServiceException(Throwable cause) {
    super(cause);
  }

  static {
    Throwables.registerSurrogateFactory(NoServiceException.class, (template, cause) ->
      new NoServiceException(template.getMessage(), cause)
    );
  }
}
