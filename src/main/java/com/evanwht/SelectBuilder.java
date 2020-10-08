package com.evanwht;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.evanwht.Keywords.FROM;
import static com.evanwht.Keywords.GROUP_BY;
import static com.evanwht.Keywords.ORDER_BY;
import static com.evanwht.Keywords.SELECT;
import static com.evanwht.Keywords.WHERE;

/**
 * Builds prepared statements for simple SELECT queries.
 *
 * @author evanwht1@gmail.com
 */
public class SelectBuilder<T> {

    private String table;
    private final List<String> columns = new ArrayList<>();
    private final Map<Column, Object> clauses = new LinkedHashMap<>();
    private final Map<String, OrderType> orders = new HashMap<>();
    private final List<String> groupings = new ArrayList<>();
    private final ResultMapper<T> resultMapper;

    /**
     * Create a SelectBuilder with a custom {@link ResultMapper}
     *
     * @param resultMapper maps the expected output to a desired return type
     */
    public SelectBuilder(final ResultMapper<T> resultMapper) {
        this.resultMapper = resultMapper;
    }

    /**
     * Create a SelectBuilder that will try to map the expected result to the fields in a class.
     * This uses reflection to find class fields that match column names. Columns that have no
     * matching field are skipped.
     *
     * @param tClass the class of the desired output objects
     */
    public SelectBuilder(final Class<T> tClass) {
        this.resultMapper = new ClassResultMapper<>(tClass);
    }

    /**
     * creates a SelectBuilder that returns the ResultSet without mapping.
     * @return a new instance of a SelectBuilder that returns a ResultSet
     */
    public static SelectBuilder<ResultSet> resultSetSelector() {
        return new SelectBuilder<>(rs -> rs);
    }

    /**
     * @param table name of the table to pull data from
     * @return the builder this was invoked on
     */
    public SelectBuilder<T> table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * Adds a column to be selected from the db. Never calling this results in all columns being selected.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @return the builder this was invoked on
     */
    public SelectBuilder<T> select(final Column column) {
        this.columns.add(column.getName());
        return this;
    }

    /**
     * Adds a clause to the select statement to filter results.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param value the desired value of the column. Can be null
     * @return the builder this was invoked on
     */
    public SelectBuilder<T> where(final Column column, final Object value) {
        clauses.put(column, value);
        return this;
    }

    /**
     * Adds a column to group the query results by.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @return the builder this was invoked on
     */
    public SelectBuilder<T> groupBy(final Column column) {
        groupings.add(column.getName());
        return this;
    }

    /**
     * Adds a column to order the query results by.
     *
     * @param column a {@link Column} representing a column of the table in the db
     * @param orderType how to order the column. Can be null
     * @return the builder this was invoked on
     */
    public SelectBuilder<T> orderBy(final Column column, final OrderType orderType) {
        orders.put(column.getName(), orderType);
        return this;
    }

    /**
     * Builds a SELECT statement for the table, selected columns, and where clauses supplied to this builder.
     * Only be visible for testing.
     *
     * @return SELECT statement
     */
    String createStatement() {
        final StringJoiner sj = new StringJoiner(" ", SELECT, ";");
        if (columns.isEmpty()) {
            sj.add("*");
        } else {
            sj.add(String.join(", ", columns));
        }
        sj.add(FROM).add(table);
        if (!clauses.isEmpty()) {
            sj.add(WHERE)
              .add(clauses.entrySet().stream()
                          .map(e -> e.getKey().getName() + (e.getValue() == null ? " IS NULL" : " = ?"))
                          .collect(Collectors.joining(" AND ")));
        }
        if (!groupings.isEmpty()) {
            sj.add(GROUP_BY)
                    .add(String.join(", ", groupings));
        }
        if (!orders.isEmpty()) {
            sj.add(ORDER_BY)
                    .add(orders.entrySet().stream()
                    .map(e -> e.getKey() + (e.getValue() == null ? "" : " " + e.getValue().name()))
                    .collect(Collectors.joining(", ")));
        }
        return sj.toString();
    }

    /**
     * Builds the PreparedStatement and sets the necessary values for any where clauses
     *
     * @param connection connection to the db to perform this query on
     * @return a prepared statement that can be executed
     * @throws SQLException if the table name was empty or an error occurred performing the query
     */
    PreparedStatement prepareStatement(final Connection connection) throws SQLException {
        if (table == null || table.isEmpty()) {
            throw new SQLException("No table defined");
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p!= null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        return statement;
    }

    /**
     * Gets the first row returned from the db and maps it to the desired object. Ignores any other rows returned.
     *
     * @param connection connection to the db to perform this query on
     * @return Optional wrapped mapped object
     * @throws SQLException if the table name was empty or an error occurred performing the query
     */
    public Optional<T> getOne(final Connection connection) throws SQLException {
        final ResultSet resultSet = prepareStatement(connection).executeQuery();
        if (resultSet.next()) {
            return Optional.of(resultMapper.map(resultSet));
        }
        return Optional.empty();
    }

    /**
     * Retrieves and maps rows returned from the db.
     *
     * @param connection connection to the db to perform this query on
     * @return List of mapped objects. Never null
     * @throws SQLException if the table name was empty or an error occurred performing the query
     */
    public List<T> getMany(final Connection connection) throws SQLException {
        final ResultSet resultSet = prepareStatement(connection).executeQuery();
        final List<T> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(resultMapper.map(resultSet));
        }
        return list;
    }
}

