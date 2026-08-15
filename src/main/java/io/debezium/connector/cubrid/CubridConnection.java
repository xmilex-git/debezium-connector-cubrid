/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * {@link JdbcConnection} extension to be used with CUBRID.
 */
public class CubridConnection extends JdbcConnection {

    private static final String DRIVER_CLASS_NAME = "cubrid.jdbc.driver.CUBRIDDriver";

    private static final String QUOTED_CHARACTER = "\"";

    private static final String URL_PATTERN = "jdbc:cubrid:${"
            + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}:${"
            + JdbcConfiguration.DATABASE + "}:::";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            URL_PATTERN,
            DRIVER_CLASS_NAME,
            CubridConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(CubridConnectorConfig.PORT.defaultValueAsString()));

    public CubridConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    /**
     * @return a JDBC connection string for the current configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }
}
