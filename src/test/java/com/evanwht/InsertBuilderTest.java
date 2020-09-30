package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class InsertBuilderTest {

    private final MockDB mockDB = new MockDB();

    @Test
    void single() throws SQLException {
        final String expectedSql = "INSERT INTO phoosball.test_table (varCharCol) VALUES (?);";
        final InsertBuilder builder = new InsertBuilder()
                .table("test_table")
                .value(TestColumns.VAR_CHAR, "val");
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(2L, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "INSERT INTO phoosball.test_table (varCharCol, intCol, arrayCol) VALUES (?, ?, ?);";
        final InsertBuilder builder = new InsertBuilder()
                .table("test_table")
                .value(TestColumns.VAR_CHAR, "val")
                .value(TestColumns.INT, 2)
                .value(TestColumns.ARRAY, List.of("val1", "val2"));
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(2L, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
        verify(mockDB.statement).setObject(3, List.of("val1", "val2"), Types.ARRAY);
    }
}