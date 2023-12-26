package com.javi.personal.wallascala.processor;

public enum ProcessedTables {
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
}
