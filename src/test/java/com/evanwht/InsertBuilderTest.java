package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InsertBuilderTest {

    @Mock private Connection db;
    @Mock private PreparedStatement statement;
    @Mock private ResultSet resultSet;

    private final Column varCharCol = new Column() {
        @Override
        public String getName() {
            return "varCharCol";
        }

        @Override
        public int getType() {
            return Types.VARCHAR;
        }
    };
    private final Column intCol = new Column() {
        @Override
        public String getName() {
            return "intCol";
        }

        @Override
        public int getType() {
            return Types.INTEGER;
        }
    };
    private final Column arrayCol = new Column() {
        @Override
        public String getName() {
            return "arrayCol";
        }

        @Override
        public int getType() {
            return Types.ARRAY;
        }
    };

    @Test
    void single() throws SQLException {
        final String expectedSql = "INSERT INTO phoosball.test_table (varCharCol) VALUES (?);";
        final InsertBuilder builder = new InsertBuilder()
                .table("test_table")
                .value(varCharCol, "val");
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql, Statement.RETURN_GENERATED_KEYS)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        when(statement.getGeneratedKeys()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(2L);
        assertEquals(2L, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "INSERT INTO phoosball.test_table (varCharCol, intCol, arrayCol) VALUES (?, ?, ?);";
        final InsertBuilder builder = new InsertBuilder()
                .table("test_table")
                .value(varCharCol, "val")
                .value(intCol, 2)
                .value(arrayCol, List.of("val1", "val2"));
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql, Statement.RETURN_GENERATED_KEYS)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        when(statement.getGeneratedKeys()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(2L);
        assertEquals(2L, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
        verify(statement).setObject(2, 2, Types.INTEGER);
        verify(statement).setObject(3, List.of("val1", "val2"), Types.ARRAY);
    }
}