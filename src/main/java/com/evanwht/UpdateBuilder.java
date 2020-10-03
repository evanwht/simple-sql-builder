package com.evanwht;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.evanwht.Keywords.SET;
import static com.evanwht.Keywords.UPDATE;
import static com.evanwht.Keywords.WHERE;

/**
 * Builds prepared statements for simple UPDATE statements.
 *
 * @author evanwht1@gmail.com
 */
public class UpdateBuilder {

    private String table;
    private final Map<Column, Object> values = new LinkedHashMap<>();
    private final Map<Column, Object> clauses = new LinkedHashMap<>();

    /**
     * @param table name of the table to update data on
     * @return the builder this was invoked on
     */
    public UpdateBuilder table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * Adds a column to be updated in the db.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param value the desired value of the column. Can be null
     * @return the builder this was invoked on
     */
    public UpdateBuilder value(final Column column, final Object value) {
        this.values.put(column, value);
        return this;
    }

    /**
     * Adds a clause to the update statement to update only certain rows.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param value the desired value of the column. Can be null
     * @return the builder this was invoked on
     */
    public UpdateBuilder where(final Column column, final Object value) {
        clauses.put(column, value);
        return this;
    }

    /**
     * Builds a UPDATE statement for the select columns in the table and where clauses supplied to this builder.
     * Only be visible for testing.
     *
     * @return DELETE statement
     */
    String createStatement() {
        final StringJoiner sj = new StringJoiner(" ", UPDATE, ";")
                .add(table)
                .add(SET);
        sj.add(values.keySet().stream()
                     .map(s -> s.getName() + " = ?")
                     .collect(Collectors.joining(", ")));
        if (!clauses.isEmpty()) {
            sj.add(WHERE)
              .add(clauses.entrySet()
                          .stream()
                          .map(s -> s.getKey().getName() + (s.getValue() == null ? " IS NULL" : " = ?"))
                          .collect(Collectors.joining(" AND ")));
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
    public OptionalInt execute(final Connection connection) throws SQLException {
        if (table == null || values.isEmpty()) {
            throw new SQLException("No table defined");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : values.entrySet()) {
            if (p.getValue() == null) {
                statement.setNull(index++, p.getKey().getType());
            } else {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p != null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        final int rows = statement.executeUpdate();
        if (rows > 0) {
            return OptionalInt.of(rows);
        }
        return OptionalInt.empty();
    }
}

