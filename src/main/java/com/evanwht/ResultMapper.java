package com.evanwht;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author evanwht1@gmail.com
 */
@FunctionalInterface
public interface ResultMapper<T> {
    T map(final ResultSet rs) throws SQLException;
}

