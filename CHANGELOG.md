# Changelog

## [1.3.1] - 2026-09-26
### Changed
- Actualización para Databricks Runtime 19: Spark 4.2.0 y Scala 2.13.18
- Limpieza de dependencias en el POM: eliminación de delta-spark, azure-storage-blob, spark-hive, jackson directo, slf4j-api, antlr4-runtime
- Actualización de jts-core a 1.20.0, scalatest-maven-plugin a 2.2.0 y maven-assembly-plugin a 3.7.1
- Corrección de la clase principal en maven-assembly-plugin a com.javi.personal.wallascala.launcher.Main
- Configuración de maven.compiler.release a Java 17

## [1.3.0] - 2026-02-22
### Added
- Add configuration for properties_full processor, and new argument -c (--coalesce) for procesor

## [1.2.8] - 2026-02-21
### Changed
- Disable DB creations to prevent using databricks unity catalog

## [1.2.7] - 2026-02-21
### Changed
- Arreglado PisosProcessor
- Cruce de datos entre pisos y polígonos de codigos postales para obtener localización

## [1.2.6] - 2026-02-15
### Changed
- Actualización esquema pisos.com
- Cambio de comportamiento cleaner: Si un campo no existe en origen, se añade al destino con valor nulo en lugar de terminar con error.

## [1.2.5] - 2026-02-15
### Changed
- Adecuación procesos a nueva estructura del datalake

## [1.2.4] - 2026-02-15
### Changed
- Arreglado el workflow de despliegue de relases.

---

> Este changelog documenta los análisis y sugerencias realizados sobre los módulos principales del proyecto, así como el incremento de versión.
