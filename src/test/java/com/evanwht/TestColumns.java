package com.evanwht;

import java.sql.Types;

/**
 * @author evanwht1@gmail.com
 */
public enum TestColumns implements Column {

    VAR_CHAR("varCharCol", Types.VARCHAR),
    INT("intCol", Types.INTEGER),
    ARRAY("arrayCol", Types.ARRAY);

    private final String name;
    private final int type;

    TestColumns(final String name, final int type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getType() {
        return type;
    }
}
