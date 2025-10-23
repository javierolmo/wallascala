# Wallascala

A Scala-based project for data processing with Apache Spark.

## Version Management

This project uses [Semantic Versioning](https://semver.org/) for version control. Version numbers follow the format `MAJOR.MINOR.PATCH`.

### Versioning Rules

1. **Version Format**: All versions must follow semantic versioning format: `X.Y.Z` where X, Y, and Z are non-negative integers.
   - Examples of valid versions: `1.0.0`, `1.2.3`, `2.0.0`, `1.0.0-alpha`, `1.0.0+build.123`
   - Examples of invalid versions: `1.2`, `v1.2.3`, `1.2.3.4`

2. **Version Increment Requirement**: Before a pull request can be merged to `main`, the version in `pom.xml` **must** be incremented compared to the current version in the `main` branch.

3. **Automated Tag Creation**: After a pull request is merged to `main`, a Git tag is automatically created with the format `vX.Y.Z` (e.g., `v1.2.0`).

### CI/CD Workflows

#### Pull Request Checks

When you create a pull request to `main`, the following checks are automatically performed:

1. **Version Extraction**: The version is extracted from your PR's `pom.xml`
2. **Semver Validation**: The version is validated to ensure it follows semantic versioning format
3. **Version Comparison**: The PR version is compared with the current `main` branch version
4. **Build Verification**: The project is built to ensure no compilation errors

If any of these checks fail, the pull request cannot be merged.

#### Post-Merge Actions

After merging to `main`:

1. The version is extracted from `pom.xml`
2. A Git tag is created with the format `vX.Y.Z`
3. The tag is pushed to the repository

If a tag already exists for the current version, the tag creation is skipped.

### How to Increment Version

When creating a pull request, update the version in `pom.xml`:

```xml
<version>1.2.1</version>  <!-- Increment from 1.2.0 -->
```

Follow semantic versioning guidelines:
- **PATCH** (`1.2.0` → `1.2.1`): Bug fixes and minor changes
- **MINOR** (`1.2.0` → `1.3.0`): New features, backward compatible
- **MAJOR** (`1.2.0` → `2.0.0`): Breaking changes

## Build and Development

### Prerequisites

- Java 8 or higher
- Maven
- Scala 2.13

### Building the Project

```bash
mvn clean package
```

This will create a JAR file with dependencies in the `target` directory.

### Running Tests

```bash
mvn test
```

## Deployment

The project is automatically deployed to Azure Blob Storage when changes are pushed to:
- `main` branch → artifacts-main container
- `snapshot/*` branches → artifacts-feature container

## License

See LICENSE file for details.
