package com.evanwht;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author evanwht1@gmail.com
 */
public class DeleteBuilder extends SqlBuilder {

    private String table;
    private final Map<Column, Object> clauses = new LinkedHashMap<>();

    public DeleteBuilder table(final String table) {
        this.table = table;
        return this;
    }

    public DeleteBuilder where(final Column column, final Object value) {
        this.clauses.put(column, value);
        return this;
    }

    String createStatement() {
        return new StringJoiner(" ", DELETE, ";")
                .add(FROM)
                .add("phoosball." + table)
                .add(WHERE)
                .add(clauses.entrySet()
                            .stream()
                            .map(e -> e.getKey().getName() + (e.getValue() == null ? " IS NULL" : " = ?"))
                            .collect(Collectors.joining(" AND ")))
                .toString();
    }

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
