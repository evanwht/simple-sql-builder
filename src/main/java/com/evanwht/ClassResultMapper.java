package com.evanwht;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

/**
 * Simple ResultMapper to turn a DB row into an object of the supplied class. Loops over the returned
 * columns and tried to set values on fields that match any of the column names. Only works with simple
 * data types (integer, double, boolean, and string), anything else and it will attempt to set a generic
 * object on the field.
 *
 * @author evanwht1@gmail.com
 */
public class ClassResultMapper<T> implements ResultMapper<T> {

    private final Class<T> tClass;

    public ClassResultMapper(final Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public T map(final ResultSet rs) throws SQLException {
        final Constructor<?> constructor = Arrays.stream(tClass.getConstructors())
                                                 .filter(c -> c.getParameterCount() == 0)
                                                 .findAny()
                                                 .orElseThrow(() -> {
                                                     throw new RuntimeException("Can't instantiate instance of type: " + tClass.getSimpleName());
                                                 });
        try {
            final T o = (T) constructor.newInstance();
            setAllFields(o, rs);
            return o;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Can't instantiate instance of type: " + tClass.getSimpleName());
        }
    }

    private void setAllFields(final T obj, final ResultSet rs) throws SQLException {
        final int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String columnName = rs.getMetaData().getColumnName(i);
            try {
                final Field field = tClass.getField(columnName);
                final int columnType = rs.getMetaData().getColumnType(i);
                setField(field, columnType, i, obj, rs);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                // Silently move on
            }
        }
    }

    private void setField(final Field field, final int type, final int columnIndex, final T obj, final ResultSet rs) throws SQLException, IllegalAccessException {
        switch (type) {
            case Types.INTEGER -> field.setInt(obj, rs.getInt(columnIndex));
            case Types.DOUBLE -> field.setDouble(obj, rs.getDouble(columnIndex));
            case Types.BOOLEAN -> field.setBoolean(obj, rs.getBoolean(columnIndex));
            case Types.VARCHAR -> field.set(obj, rs.getString(columnIndex));
            default -> field.set(obj, rs.getObject(columnIndex));
        }
    }
}
