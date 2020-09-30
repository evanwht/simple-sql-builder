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
 * @author evanwht1@gmail.com
 */
public class UpdateBuilder {

    private String table;
    private final Map<Column, Object> values = new LinkedHashMap<>();
    private final Map<Column, Object> clauses = new LinkedHashMap<>();

    public UpdateBuilder table(final String table) {
        this.table = table;
        return this;
    }

    public UpdateBuilder value(final Column column, final Object value) {
        this.values.put(column, value);
        return this;
    }

    public UpdateBuilder where(final Column column, final Object value) {
        clauses.put(column, value);
        return this;
    }

    /**
     * Creates an update statement for a select amount of columns. Of the form: "UPDATE {table} SET {col} = {val} WHERE {col} = {val};"
     * @return update SQL statement
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

