# Changelog

## [1.3.1] - 2026-09-26
### Changed
- Soporte para Databricks Runtime 19 (Spark 4.2.0 y Scala 2.13.18)
- Limpieza y actualización de dependencias y plugins en el POM

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
