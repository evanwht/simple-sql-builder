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

/**
 * @author evanwht1@gmail.com
 */
public class InsertBuilder extends SqlBuilder {

    private String table;
    private final Map<Column, Object> values = new LinkedHashMap<>();

    public InsertBuilder table(final String table) {
        this.table = table;
        return this;
    }

    public InsertBuilder value(final Column column, final Object value) {
        this.values.put(column, value);
        return this;
    }

    // Visible For Testing
    String createStatement() {
        final StringJoiner sj = new StringJoiner(" ", INSERT, ";")
                .add(INTO)
                .add("phoosball." + table);
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

