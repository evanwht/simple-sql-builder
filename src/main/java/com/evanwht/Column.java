package com.evanwht;

/**
 * Represents a column in a table of a database.
 *
 * @author evanwht1@gmail.com
 */
public interface Column {

    /**
     * @return name of the column. Should match exactly what the column is called.
     */
    String getName();

    /**
     * @return the java.sql.Types type of the column
     */
    int getType();
}

