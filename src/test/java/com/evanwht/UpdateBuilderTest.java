package com.evanwht;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author evanwht1@gmail.com
 */
@ExtendWith(MockitoExtension.class)
public class UpdateBuilderTest {

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
        final String expectedSql = "UPDATE phoosball.test_table SET varCharCol = ?;";
        final UpdateBuilder builder = new UpdateBuilder()
                .table("test_table")
                .value(varCharCol, "val");
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        assertEquals(1, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
    }

    @Test
    void multi() throws SQLException {
        final String expectedSql = "UPDATE phoosball.test_table SET varCharCol = ?, intCol = ?, arrayCol = ? WHERE intCol = ?;";
        final UpdateBuilder builder = new UpdateBuilder()
                .table("test_table")
                .value(varCharCol, "val")
                .value(intCol, 2)
                .value(arrayCol, List.of("val1", "val2"))
                .where(intCol, 1);
        assertEquals(expectedSql, builder.createStatement());
        when(db.prepareStatement(expectedSql)).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        assertEquals(1, builder.execute(db).orElse(0));
        verify(statement).setObject(1, "val", Types.VARCHAR);
        verify(statement).setObject(2, 2, Types.INTEGER);
        verify(statement).setObject(3, List.of("val1", "val2"), Types.ARRAY);
        verify(statement).setObject(4, 1, Types.INTEGER);
    }
}
