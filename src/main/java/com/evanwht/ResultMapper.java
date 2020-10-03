package com.evanwht;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Maps a ResultSet returned from a db query to a given type.
 * {@link SelectBuilder}
 *
 * @author evanwht1@gmail.com
 */
@FunctionalInterface
public interface ResultMapper<T> {
    T map(final ResultSet rs) throws SQLException;
}

