package com.javi.personal.wallascala.processor;

public enum ProcessedTables {
    WALLAPOP_PROPERTIES("wallapop_properties"),
    PROPERTIES_FULL("properties_full"),
    PISOS_PROPERTIES("pisos_properties");

    private final String name;

    ProcessedTables(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static ProcessedTables fromString(String name) {
        for (ProcessedTables table : values()) {
            if (table.name.equalsIgnoreCase(name) || table.name().equalsIgnoreCase(name)) {
                return table;
            }
        }
        throw new IllegalArgumentException("Unknown processed table: " + name + ". Valid values are: " + java.util.Arrays.toString(values()));
    }

}
