/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.plugin.clickhouse.ClickHouseEngineType.MERGETREE;
import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.ORDER_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.PARTITION_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.PRIMARY_KEY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.SAMPLE_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.StandardReadMappings.jdbcTypeToPrestoType;
import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Splitter TABLE_PROPERTY_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    protected static final String identifierQuote = "\"";
    private final boolean mapStringAsVarchar;

    @Inject
    public ClickHouseClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ClickHouseConfig clickHouseConfig)
    {
        super(connectorId, config, identifierQuote, connectionFactory(config, clickHouseConfig));
        mapStringAsVarchar = clickHouseConfig.isMapStringAsVarchar();
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, ClickHouseConfig clickHouseConfig)
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("map-string-as-varchar", String.valueOf(clickHouseConfig.isMapStringAsVarchar()));
        return new DriverConnectionFactory(
                new ClickHouseDriver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        return jdbcTypeToPrestoType(typeHandle, mapStringAsVarchar);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = handle.getSchemaName();
            String table = handle.getTableName();
            String columnName = column.getName();
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                schema = schema != null ? schema.toUpperCase(ENGLISH) : null;
                table = table.toUpperCase(ENGLISH);
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD COLUMN %s",
                    quoted(handle.getCatalogName(), schema, table),
                    getColumnDefinitionSql(column, columnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JdbcErrorCode.JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcOutputTableHandle createTable(ConnectorTableMetadata tableMetadata, ConnectorSession session, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session, identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnDefinitionSql(column, columnName));
            }

            RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), tableName);
            String sql = createTableSql(remoteTableName, columnList.build(), tableMetadata);
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(tableName));
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        String schemaName = oldTable.getSchemaName();
        String tableName = oldTable.getTableName();
        String newSchemaName = newTable.getSchemaName();
        String newTableName = newTable.getTableName();

        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                schemaName = schemaName.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format("RENAME TABLE %s.%s TO %s.%s",
                    quoted(schemaName),
                    quoted(tableName),
                    quoted(newSchemaName),
                    quoted(newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private String getColumnDefinitionSql(ColumnMetadata column, String columnName)
    {
        StringBuilder builder = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        String columnTypeMapping = toWriteMapping(column.getType());
        if (column.isNullable()) {
            builder.append("Nullable(").append(columnTypeMapping).append(")");
        }
        else {
            builder.append(columnTypeMapping);
        }
        return builder.toString();
    }

    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        ClickHouseEngineType engine = ClickHouseTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());
        if (engine == MERGETREE && formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).equals(Optional.empty())) {
            // order_by property is required
            throw new PrestoException(INVALID_TABLE_PROPERTY,
                    format("The property of %s is required for table engine %s", ORDER_BY_PROPERTY, engine.getEngineType()));
        }
        formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));
        formatProperty(ClickHouseTableProperties.getPrimaryKey(tableProperties)).ifPresent(value -> tableOptions.add("PRIMARY KEY " + value));
        formatProperty(ClickHouseTableProperties.getPartitionBy(tableProperties)).ifPresent(value -> tableOptions.add("PARTITION BY " + value));
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + value));

        return format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build()));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                PreparedStatement statement = connection.prepareStatement("SELECT engine, sorting_key, partition_key, primary_key, sampling_key " +
                        "FROM system.tables " +
                        "WHERE database = ? AND name = ?")) {
            statement.setString(1, tableHandle.getSchemaName());
            statement.setString(2, tableHandle.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
                while (resultSet.next()) {
                    String engine = resultSet.getString("engine");
                    if (!isNullOrEmpty(engine)) {
                        // Don't throw an exception because many table engines aren't supported in ClickHouseEngineType
                        Optional<ClickHouseEngineType> engineType = Enums.getIfPresent(ClickHouseEngineType.class, engine.toUpperCase(ENGLISH)).toJavaUtil();
                        engineType.ifPresent(type -> properties.put(ENGINE_PROPERTY, type));
                    }
                    String sortingKey = resultSet.getString("sorting_key");
                    if (!isNullOrEmpty(sortingKey)) {
                        properties.put(ORDER_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(sortingKey));
                    }
                    String partitionKey = resultSet.getString("partition_key");
                    if (!isNullOrEmpty(partitionKey)) {
                        properties.put(PARTITION_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(partitionKey));
                    }
                    String primaryKey = resultSet.getString("primary_key");
                    if (!isNullOrEmpty(primaryKey)) {
                        properties.put(PRIMARY_KEY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(primaryKey));
                    }
                    String samplingKey = resultSet.getString("sampling_key");
                    if (!isNullOrEmpty(samplingKey)) {
                        properties.put(SAMPLE_BY_PROPERTY, samplingKey);
                    }
                }
                return properties.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * format property to match ClickHouse create table statement
     *
     * @param properties property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return Optional.empty();
        }
        else if (properties.size() == 1) {
            // only one column
            return Optional.of(properties.get(0));
        }
        else {
            // include more than one column
            return Optional.of("(" + String.join(",", properties) + ")");
        }
    }

    private String toWriteMapping(Type type)
    {
        if (type == BOOLEAN) {
            // ClickHouse uses UInt8 as boolean, restricted values to 0 and 1.
            return "UInt8";
        }
        if (type == TINYINT) {
            return "Int8";
        }
        if (type == SMALLINT) {
            return "Int16";
        }
        if (type == INTEGER) {
            return "Int32";
        }
        if (type == BIGINT) {
            return "Int64";
        }
        if (type == REAL) {
            return "Float32";
        }
        if (type == DOUBLE) {
            return "Float64";
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            return dataType;
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return "String";
        }
        if (type instanceof VarbinaryType) {
            return "String";
        }
        if (type == DATE) {
            return "Date";
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse support the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(null, schemaName, newTableName),
                quoted(null, schemaName, tableName));

        try {
            execute(connection, sql);
        }
        catch (SQLException e) {
            PrestoException exception = new PrestoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + sql));
            throw exception;
        }
    }

    private String quoted(RemoteTableName remoteTableName)
    {
        return quoted(
                remoteTableName.getCatalogName().orElse(null),
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }
}
