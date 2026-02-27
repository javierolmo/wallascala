package com.javi.personal.wallascala.processor;

public enum ProcessedTables {
    WALLAPOP_PROPERTIES("wallapop_properties"),
    PROPERTIES_FULL("properties_full"),
    PISOS_PROPERTIES("pisos_properties"),
    PROPERTIES_BY_ZONE("properties_by_zone"),
    PROPERTIES_TEMPORAL_EVOLUTION("properties_temporal_evolution"),
    PROPERTIES_BY_TYPE("properties_by_type"),
    PROPERTIES_SPECIAL_FEATURES("properties_special_features");

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
