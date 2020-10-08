package com.evanwht.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.evanwht.sql.Keywords.DELETE;
import static com.evanwht.sql.Keywords.FROM;
import static com.evanwht.sql.Keywords.WHERE;

/**
 * Builds prepared statements for simple DELETE statements.
 *
 * @author evanwht1@gmail.com
 */
public class DeleteBuilder {

    private String table;
    private final Map<Column, Object> clauses = new LinkedHashMap<>();

    /**
     * @param table name of the table to delete data from
     * @return the builder this was invoked on
     */
    public DeleteBuilder table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * Adds a clause to the delete statement to delete only certain rows.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param value the desired value of the column. Can be null
     * @return the builder this was invoked on
     */
    public DeleteBuilder where(final Column column, final Object value) {
        this.clauses.put(column, value);
        return this;
    }

    /**
     * Builds a DELETE statement for the table and where clauses supplied to this builder.
     * Only be visible for testing.
     *
     * @return DELETE statement
     */
    String createStatement() {
        return new StringJoiner(" ", DELETE, ";")
                .add(FROM)
                .add(table)
                .add(WHERE)
                .add(clauses.entrySet()
                            .stream()
                            .map(e -> e.getKey().getName() + (e.getValue() == null ? " IS NULL" : " = ?"))
                            .collect(Collectors.joining(" AND ")))
                .toString();
    }

    /**
     * Builds the PreparedStatement and sets the necessary values for any where clauses
     *
     * @param connection connection to the db to perform this statement on
     * @return a prepared statement that can be executed
     * @throws SQLException if the table name was empty or an error occurred performing the query
     */
    public OptionalInt execute(final Connection connection) throws SQLException {
        if (table == null || clauses.isEmpty()) {
            throw new SQLException("Need both table and at least one where clause");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p.getValue() != null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        final int rows = statement.executeUpdate();
        return rows > 0 ? OptionalInt.of(rows) : OptionalInt.empty();
    }
}
