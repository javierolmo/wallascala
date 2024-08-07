package com.javi.personal.wallascala.processor;

public enum ProcessedTables {
    WALLAPOP_PROPERTIES("wallapop_properties"),
    WALLAPOP_PROPERTIES_SNAPSHOTS("wallapop_properties_snapshots"),
    PROPERTIES("properties"),
    PRICE_CHANGES("price_changes"),
    POSTAL_CODE_ANALYSIS("postal_code_analysis"),
    APARTMENT_INVESTMENT_ANALYSIS("apartment_investment_analysis");

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
