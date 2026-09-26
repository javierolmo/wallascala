# Directrices del Proyecto: Scala & Spark (Maven)

## 1. Commits
- Formato: `<tipo>(<alcance>): <descripción>` (ej. `feat(ingest): add filter to sales job`).
- Tipos: `feat`, `fix`, `refactor`, `perf`, `test`, `chore`.
- Commits atómicos y mensajes en minúsculas sin punto final.

## 2. Validaciones Previas
Antes de commitear:
- Ejecutar `mvn clean test` (cero fallos).
- Revisar `git status`: no incluir `target/`, credenciales ni artefactos locales de Spark (`spark-warehouse/`).

## 3. Código Spark & Scala
- Preferir inmutabilidad (`val` sobre `var`).
- Esquemas explícitos (`StructType`) en lugar de inferencia automática.
- Prohibido `.collect()` en producción; usar `broadcast` en joins pequeños.
- Liberar memoria con `.unpersist()` si se usa `.cache()`.
- Sin `println` ni llamadas a `.show()` residuales.

## 4. Protocolo de Pull Request (PR)
Al solicitar abrir o preparar una PR:
1. **`pom.xml`**: Subir versión en `<version>x.y.z</version>` (SemVer) si no se ha incrementado ya.
2. **`CHANGELOG.md`**: Si no existe la entrada para la versión actual, añadirla arriba con este formato:
   ```markdown
   ## [x.y.z] - YYYY-MM-DD
   ### <Added|Changed|Fixed|Removed>
   - Descripción concisa. Debe ser mínima. Si puede ser un único punto, mejor.