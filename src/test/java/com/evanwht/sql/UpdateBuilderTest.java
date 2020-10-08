package com.evanwht.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class UpdateBuilderTest {

    private final MockDB mockDB = new MockDB();

    @Test
    void single() throws SQLException {
        final String expectedSql = "UPDATE test_table SET varCharCol = ?;";
        final UpdateBuilder builder = new UpdateBuilder()
                .table("test_table")
                .value(TestColumns.VAR_CHAR, "val");
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(1, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "UPDATE test_table SET varCharCol = ?, intCol = ?, arrayCol = ? WHERE intCol = ?;";
        final UpdateBuilder builder = new UpdateBuilder()
                .table("test_table")
                .value(TestColumns.VAR_CHAR, "val")
                .value(TestColumns.INT, 2)
                .value(TestColumns.ARRAY, List.of("val1", "val2"))
                .where(TestColumns.INT, 1);
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(1, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
        verify(mockDB.statement).setObject(3, List.of("val1", "val2"), Types.ARRAY);
        verify(mockDB.statement).setObject(4, 1, Types.INTEGER);
    }
}
