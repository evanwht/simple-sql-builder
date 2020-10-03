package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.internal.matchers.Or;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.swing.*;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class SelectBuilderTest {

    private final MockDB mockDB = new MockDB();

    @Test
    void testSingleColumn() throws SQLException {
        final String expectedSql = "SELECT * FROM test_table WHERE varCharCol = ?;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .where(TestColumns.VAR_CHAR, "val");

        assertEquals(expectedSql, builder.createStatement());
        assertTrue(builder.getOne(mockDB.connection).isPresent());

        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void testOrderBy() throws SQLException {
        final String expectedSql = "SELECT * FROM test_table ORDER BY varCharCol ASC, intCol DESC;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .orderBy(TestColumns.VAR_CHAR, OrderType.ASC)
                .orderBy(TestColumns.INT, OrderType.DESC);

        assertEquals(expectedSql, builder.createStatement());
        assertTrue(builder.getOne(mockDB.connection).isPresent());
    }

    @Test
    void testGetOne() throws SQLException {
        final String expectedSql = "SELECT * FROM test_table WHERE varCharCol = ?;";
        final SelectBuilder<Boolean> builder = new SelectBuilder<>(
                rs -> {
                    assertEquals("val1", rs.getString(TestColumns.VAR_CHAR.getName()));
                    assertEquals(1, rs.getInt(TestColumns.INT.getName()));
                    assertNotNull(rs.getArray(TestColumns.ARRAY.getName()));
                    return true;
                })
                .table("test_table")
                .where(TestColumns.VAR_CHAR, "val");

        assertEquals(expectedSql, builder.createStatement());
        assertTrue(builder.getOne(mockDB.connection).isPresent());

        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void testMultiColumn() throws SQLException {
        final String expectedSql = "SELECT intCol, arrayCol FROM test_table WHERE varCharCol = ? AND intCol = ?;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .select(TestColumns.INT)
                .select(TestColumns.ARRAY)
                .where(TestColumns.VAR_CHAR, "val")
                .where(TestColumns.INT, 2);

        assertEquals(expectedSql, builder.createStatement());
        assertTrue(builder.getOne(mockDB.connection).isPresent());

        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
    }

    @Test
    void testMultiRow() throws SQLException {
        final String expectedSql = "SELECT intCol, arrayCol FROM test_table WHERE varCharCol = ? AND intCol = ?;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .select(TestColumns.INT)
                .select(TestColumns.ARRAY)
                .where(TestColumns.VAR_CHAR, "val")
                .where(TestColumns.INT, 2);

        assertEquals(expectedSql, builder.createStatement());
        assertEquals(2, builder.getMany(mockDB.connection).size());

        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
    }

    static class Result {

        private final String str;
        private final int in;
        private final List<String> arr;

        public Result(final String str, final int in, final List<String> arr) {
            this.str = str;
            this.in = in;
            this.arr = arr;
        }
    }

    @Test
    void testGetMulti() throws SQLException {
        final String expectedSql = "SELECT intCol, arrayCol FROM test_table WHERE varCharCol = ? AND intCol = ?;";
        final SelectBuilder<Result> builder = new SelectBuilder<>(
                rs -> {
                    final Array array = rs.getArray(TestColumns.ARRAY.getName());
                    final ResultSet resultSet = array.getResultSet();
                    final List<String> arr = new ArrayList<>();
                    while (resultSet.next()) {
                        arr.add(resultSet.getString(2));
                    }
                    return new Result(rs.getString(TestColumns.VAR_CHAR.getName()), rs.getInt(TestColumns.INT.getName()), arr);
                })
                .table("test_table")
                .select(TestColumns.INT)
                .select(TestColumns.ARRAY)
                .where(TestColumns.VAR_CHAR, "val")
                .where(TestColumns.INT, 2);

        assertEquals(expectedSql, builder.createStatement());
        final List<Result> many = builder.getMany(mockDB.connection);
        assertEquals(2, many.size());
        Result result = many.get(0);
        assertEquals("val1", result.str);
        assertEquals(1, result.in);
        assertEquals(2, result.arr.size());
        assertEquals("col1", result.arr.get(0));
        assertEquals("col2", result.arr.get(1));

        result = many.get(1);
        assertEquals("val2", result.str);
        assertEquals(2, result.in);
        assertEquals(2, result.arr.size());
        assertEquals("col1", result.arr.get(0));
        assertEquals("col2", result.arr.get(1));

        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
    }
}
