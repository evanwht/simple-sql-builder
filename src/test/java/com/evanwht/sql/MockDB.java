package com.evanwht.sql;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author evanwht1@gmail.com
 */
public class MockDB {

    final Connection connection = mock(Connection.class);
    final PreparedStatement statement = mock(PreparedStatement.class);
    private final ResultSet resultSet = mock(ResultSet.class);
    private final Array array1 = mock(Array.class);
    private final Array array2 = mock(Array.class);
    private final ResultSet arrayResultSet1 = mock(ResultSet.class);
    private final ResultSet arrayResultSet2 = mock(ResultSet.class);

    MockDB() {
        try {
            init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void init() throws SQLException {
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.prepareStatement(anyString(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(statement);

        when(statement.executeQuery()).thenReturn(resultSet);
        when(statement.executeUpdate()).thenReturn(1);
        when(statement.getGeneratedKeys()).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString(TestColumns.VAR_CHAR.getName())).thenReturn("val1", "val2", null);
        when(resultSet.getInt(TestColumns.INT.getName())).thenReturn(1, 2, 0);
        when(resultSet.getArray(TestColumns.ARRAY.getName())).thenReturn(array1, array2, null);
        when(resultSet.getLong(1)).thenReturn(2L, 3L, 0L);

        when(array1.getResultSet()).thenReturn(arrayResultSet1);
        when(arrayResultSet1.next()).thenReturn(true, true, false);
        when(arrayResultSet1.getString(2)).thenReturn("col1", "col2", null);

        when(array2.getResultSet()).thenReturn(arrayResultSet2);
        when(arrayResultSet2.next()).thenReturn(true, true, false);
        when(arrayResultSet2.getString(2)).thenReturn("col1", "col2", null);
    }


}
