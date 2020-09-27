package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class DeleteBuilderTest {

    @Mock private Connection db;
    @Mock private PreparedStatement statement;

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

    @Test
    void errorCase() {
        final DeleteBuilder noWhere = new DeleteBuilder().table("test_table");
        assertThrows(SQLException.class, () -> noWhere.execute(db));

        final DeleteBuilder noTable = new DeleteBuilder().where(intCol, 1);
        assertThrows(SQLException.class, () -> noTable.execute(db));
    }

    @Test
    void single() throws SQLException {
        final String expectedSql = "DELETE FROM phoosball.test_table WHERE varCharCol = ?;";
        final DeleteBuilder builder = new DeleteBuilder()
                .table("test_table")
                .where(varCharCol, "val");
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        assertEquals(1, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "DELETE FROM phoosball.test_table WHERE varCharCol = ? AND intCol = ?;";
        final DeleteBuilder builder = new DeleteBuilder()
                .table("test_table")
                .where(varCharCol, "val")
                .where(intCol, 2);
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        assertEquals(1, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
        verify(statement).setObject(2, 2, Types.INTEGER);
    }
}
