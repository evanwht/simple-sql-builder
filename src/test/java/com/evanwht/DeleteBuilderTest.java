package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class DeleteBuilderTest {

    private final MockDB mockDB = new MockDB();

    @Test
    void errorCase() {
        final DeleteBuilder noWhere = new DeleteBuilder().table("test_table");
        assertThrows(SQLException.class, () -> noWhere.execute(mockDB.connection));

        final DeleteBuilder noTable = new DeleteBuilder().where(TestColumns.INT, 1);
        assertThrows(SQLException.class, () -> noTable.execute(mockDB.connection));
    }

    @Test
    void single() throws SQLException {
        final String expectedSql = "DELETE FROM phoosball.test_table WHERE varCharCol = ?;";
        final DeleteBuilder builder = new DeleteBuilder()
                .table("test_table")
                .where(TestColumns.VAR_CHAR, "val");
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(1, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "DELETE FROM phoosball.test_table WHERE varCharCol = ? AND intCol = ?;";
        final DeleteBuilder builder = new DeleteBuilder()
                .table("test_table")
                .where(TestColumns.VAR_CHAR, "val")
                .where(TestColumns.INT, 2);
        assertEquals(expectedSql, builder.createStatement());
        assertEquals(1, builder.execute(mockDB.connection).orElse(0));
        verify(mockDB.statement).setObject(1, "val", Types.VARCHAR);
        verify(mockDB.statement).setObject(2, 2, Types.INTEGER);
    }
}
