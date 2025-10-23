# Wallascala Design Documentation

## Architecture Overview

Wallascala is a Scala-based data processing application built on Apache Spark. The application follows a modular architecture with three main components:

### Components

#### 1. Cleaner
**Purpose**: Validates and cleans raw data according to predefined metadata rules.

**Key Classes**:
- `Cleaner`: Main execution logic for data cleaning
- `CleanerConfig`: Configuration for cleaning operations
- `FieldCleaner`: Validates individual fields
- `MetadataCatalog`: Registry of cleaning metadata

**Flow**:
1. Read raw data from source
2. Apply field-level validations and transformations
3. Separate valid and invalid records
4. Write valid records to target path
5. Write invalid records to exclusions path

#### 2. Processor
**Purpose**: Transforms cleaned data into processed datasets through Extract-Transform-Load (ETL) operations.

**Key Classes**:
- `Processor`: Abstract base class for all ETL operations
- `ProcessorConfig`: Configuration for processor operations
- `DataSourceProvider`: Interface for reading data sources
- `DefaultDataSourceProvider`: Default implementation using file-based storage

**Design Pattern**: Template Method
- `Processor` defines the execution template
- Concrete ETL classes (e.g., `Properties`, `PriceChanges`) implement the `build()` method
- ETL discovery uses reflection with `@ETL` annotation

**Flow**:
1. Discover ETL class based on dataset name
2. Read required source data
3. Apply transformations (implemented in `build()`)
4. Apply schema and type casting
5. Write to target location

#### 3. Launcher
**Purpose**: Generic utility for moving data between different sources and formats.

**Key Classes**:
- `Launcher`: Main execution logic
- `LauncherConfig`: Configuration for data movement
- `SparkReader`/`SparkWriter`: Abstract interfaces for reading/writing

**Features**:
- Support for multiple formats (Parquet, JSON, CSV, Delta, etc.)
- Field flattening
- Column addition
- Coalescing/repartitioning

## Design Patterns

### 1. Template Method Pattern
Used in `Processor` to define the processing workflow while allowing concrete ETL classes to implement specific transformation logic.

### 2. Strategy Pattern
Used in readers and writers to abstract different data source/sink implementations.

### 3. Dependency Injection
- `DataSourceProvider` can be injected into `Processor` for testing
- Configuration objects are passed as constructor parameters

### 4. Builder Pattern
- `SparkSessionFactory` builds configured SparkSession instances
- Configuration objects use scopt for command-line parsing

## Key Design Decisions

### 1. Configuration Validation
Configuration objects validate their parameters using `ValidationHelper`. Validation occurs after parsing to avoid issues with dummy configs used by scopt.

### 2. Error Handling
- Custom `WallaScalaException` for application-specific errors
- Consistent error handling with proper logging
- Fail-fast behavior with clear error messages

### 3. Logging
- `Logging` trait provides consistent logging across components
- Structured logging with context information
- Different log levels for debugging and production

### 4. Constants
- `DatabaseNames`: Centralized database name configuration
- `DataFormat`: Supported data formats
- `SaveMode`: Spark save modes
- Reduces magic strings and typos

### 5. Type Safety
- Java enum for `ProcessedTables` ensures type-safe table references
- Scala case classes for immutable configurations
- Strongly-typed schemas in ETL classes

## Extensibility

### Adding a New ETL
1. Create a new class extending `Processor`
2. Annotate with `@ETL(table = ProcessedTables.YOUR_TABLE)`
3. Add table to `ProcessedTables` enum
4. Implement `build()` method
5. Define schema in `schema` field

### Adding a New Data Format
1. Add format constant to `DataFormat`
2. Ensure Spark supports the format
3. Update reader/writer implementations if needed

### Adding a New Validation Rule
1. Add method to `ValidationHelper`
2. Use in configuration `validate()` method
3. Write unit tests for validation

## Testing Strategy

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test ETL workflows with sample data
- **Mocking**: Use `MockDataSourceProvider` for testing processors

## Best Practices

1. **Immutability**: Use case classes for configuration
2. **Fail Fast**: Validate early, fail with clear messages
3. **Logging**: Log at appropriate levels with context
4. **Documentation**: Document public APIs and complex logic
5. **Type Safety**: Use enums and sealed traits where appropriate
6. **Separation of Concerns**: Keep business logic separate from infrastructure
