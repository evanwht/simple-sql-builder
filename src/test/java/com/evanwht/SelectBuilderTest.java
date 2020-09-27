package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class SelectBuilderTest {

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
        final String expectedSql = "SELECT * FROM phoosball.test_table WHERE varCharCol = ?;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .where(varCharCol, "val");
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        assertNotNull(builder.getOne(db));
        verify(statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "SELECT intCol, arrayCol FROM phoosball.test_table WHERE varCharCol = ? AND intCol = ?;";
        final SelectBuilder<ResultSet> builder = SelectBuilder.resultSetSelector()
                .table("test_table")
                .select(intCol)
                .select(arrayCol)
                .where(varCharCol, "val")
                .where(intCol, 2);
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        assertNotNull(builder.getOne(db));
        verify(statement).setObject(1, "val", Types.VARCHAR);
        verify(statement).setObject(2, 2, Types.INTEGER);
    }
}
