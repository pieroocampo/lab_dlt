<img src="https://raw.githubusercontent.com/Databricks-BR/lab_dlt/main/images/lab_dlt.png">

# Consultas SQL para analizar nuestro Delta Live Tables

## Contenido

En esta carpeta, hay 3 consultas SQL que podemos utilizar para analizar nuestro pipeline de datos:
</br>
* **`00_Delta_live_tables_data_quality_monitoring.sql`**
  - En este archivo, se crea una vista sobre las expectativas del Delta Live Tables para que podamos monitorear la calidad de los datos en un panel de control (que puede ser el mismo panel de control de negocios).

* **`01_Delta_live_tables_silver_query.sql`**
  - Analizando nuestra tabla silver, que tiene datos filtrados con nuestras expectativas y reglas de negocio para el pipeline.

* **`02_Delta_live_tables_gold_query.sql`**
  - Leyendo los datos agregados diariamente y por red de tarjeta de crédito, excelente tabla para incluir en paneles de control de negocios que tienen un número conocido de filtros y análisis repetidos, ya que podemos reducir el tamaño de los almacenes que consumen la información (no tendrán que hacer la agregación de los datos en tiempo de ejecución).

## Cómo visualizar la calidad de los datos

Después de crear nuestro pipeline de Delta Live Tables, se generará un UUID con el formato `7819dcc4-d56e-4bf8-8e7e-39b6ac68ed64` y necesitamos incluirlo en la línea 25 del archivo `00_Delta_live_tables_data_quality_monitoring.sql`.
