package com.evanwht;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author evanwht1@gmail.com
 */
public class SelectBuilder<T> extends SqlBuilder {

    private String table;
    private final List<String> columns = new ArrayList<>();
    private final Map<Column, Object> clauses = new LinkedHashMap<>();
    private final ResultMapper<T> resultMapper;

    public SelectBuilder(final ResultMapper<T> resultMapper) {
        this.resultMapper = resultMapper;
    }

    public static SelectBuilder<ResultSet> resultSetSelector() {
        return new SelectBuilder<>(rs -> rs);
    }

    public SelectBuilder<T> table(final String table) {
        this.table = table;
        return this;
    }

    public SelectBuilder<T> select(final Column column) {
        this.columns.add(column.getName());
        return this;
    }

    public SelectBuilder<T> where(final Column column, final Object value) {
        clauses.put(column, value);
        return this;
    }

    String createStatement() {
        final StringJoiner sj = new StringJoiner(" ", SELECT, ";");
        if (columns.isEmpty()) {
            sj.add("*");
        } else {
            sj.add(String.join(", ", columns));
        }
        sj.add(FROM).add("phoosball." + table);
        if (!clauses.isEmpty()) {
            sj.add(WHERE)
              .add(clauses.entrySet().stream()
                          .map(e -> e.getKey().getName() + (e.getValue() == null ? " IS NULL" : " = ?"))
                          .collect(Collectors.joining(" AND ")));

        }
        return sj.toString();
    }

    public Optional<T> getOne(final Connection connection) throws SQLException {
        if (table == null) {
            throw new SQLException("No table defined");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p!= null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        final ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return Optional.of(resultMapper.map(resultSet));
        }
        return Optional.empty();
    }

    public List<T> getMany(final Connection connection) throws SQLException {
        if (table == null) {
            throw new SQLException("No table defined");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        final ResultSet resultSet = statement.executeQuery();
        final List<T> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(resultMapper.map(resultSet));
        }
        return list;
    }
}

