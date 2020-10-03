package com.evanwht;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.evanwht.Keywords.INSERT;
import static com.evanwht.Keywords.INTO;
import static com.evanwht.Keywords.VALUES;

/**
 * Builds prepared statements for simple INSERT statements.
 *
 * @author evanwht1@gmail.com
 */
public class InsertBuilder {

    private String table;
    private final Map<Column, Object> values = new LinkedHashMap<>();

    /**
     * @param table name of the table to insert data into
     * @return the builder this was invoked on
     */
    public InsertBuilder table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * Adds a column to be inserted in the db.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param value the desired value of the column. Can be null
     * @return the builder this was invoked on
     */
    public InsertBuilder value(final Column column, final Object value) {
        this.values.put(column, value);
        return this;
    }

    /**
     * Builds a INSERT statement for the select columns in the table supplied to this builder.
     * Only be visible for testing.
     *
     * @return INSERT statement
     */
    String createStatement() {
        final StringJoiner sj = new StringJoiner(" ", INSERT, ";")
                .add(INTO)
                .add(table);
        if (!values.isEmpty()) {
            sj.add(values.keySet()
                         .stream()
                         .map(Column::getName)
                         .collect(Collectors.joining(", ", "(", ")")))
              .add(VALUES)
              .add(values.values()
                         .stream()
                         .map(p -> "?")
                         .collect(Collectors.joining(", ", "(", ")")));
        }
        return sj.toString();
    }

    /**
     * Builds the PreparedStatement and sets the necessary values for any where clauses
     *
     * @param connection connection to the db to perform this statement on
     * @return a prepared statement that can be executed
     * @throws SQLException if the table name was empty or an error occurred performing the query
     */
    public OptionalLong execute(final Connection connection) throws SQLException {
        if (table == null) {
            throw new SQLException("No table defined");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement(), Statement.RETURN_GENERATED_KEYS);
        int index = 1;
        for (Map.Entry<Column, Object> p : values.entrySet()) {
            statement.setObject(index++, p.getValue(), p.getKey().getType());
        }
        final int rows = statement.executeUpdate();
        if (rows > 0) {
            try (final ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return OptionalLong.of(generatedKeys.getLong(1));
                }
            }
        }
        return OptionalLong.empty();
    }
}

