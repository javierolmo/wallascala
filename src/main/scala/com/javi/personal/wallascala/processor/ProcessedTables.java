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

}
