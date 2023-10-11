-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Creando un pipeline de Delta Live Tables con expectativas de calidad de datos en SQL
-- MAGIC
-- MAGIC En este cuaderno, realizamos la ingestión de datos JSON que están siendo generados por el cuaderno de ejemplo (`Credit_Card_Transaction_Data_Generator`) en una tabla de landing utilizando el Auto-loader, y luego procesamos los datos para asegurarnos de que no estamos propagando registros que no cumplen con nuestras reglas de negocio.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Definiendo los widgets de nuestro pipeline de Delta Live Tables
-- MAGIC Definimos parámetros que se utilizarán en nuestro pipeline de datos para buscar los datos en la zona de aterrizaje y definir:
-- MAGIC - La ruta de los archivos fuente
-- MAGIC - El formato de los archivos fuente
-- MAGIC - El nombre de la tabla bronze
-- MAGIC - La opción de inferir los tipos de columnas de nuestros archivos fuente
-- MAGIC - El nombre de la tabla silver
-- MAGIC

-- COMMAND ----------

-- CREATE WIDGET TEXT landing_path DEFAULT 'dbfs:/Users/<SEU_EMAIL>/demos/dlt_credit_cards/';
-- CREATE WIDGET TEXT bronze_data_format DEFAULT 'json';
-- CREATE WIDGET TEXT bronze_table_name DEFAULT 'landing_credit_card_transaction';
-- CREATE WIDGET TEXT bronze_infer_column_types DEFAULT 'true';
-- CREATE WIDGET TEXT clean_silver_table DEFAULT 'merchant_credit_card_transactions';
-- CREATE WIDGET TEXT aggregated_gold_table DEFAULT 'gold_merchant_credit_card_transactions_daily_agg';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Ingeriendo los datos en formato JSON en nuestra tabla bronze incremental (Delta)
-- MAGIC Declaramos nuestra tabla incremental utilizando la sintaxis de `CREATE STREAMING LIVE TABLE` y especificamos los parámetros de ruta y formato de los datos de origen.
-- MAGIC

-- COMMAND ----------

CREATE STREAMING LIVE TABLE ${bronze_table_name}
AS
SELECT * FROM cloud_files("${landing_path}", "${bronze_data_format}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Creando nuestra tabla silver, incremental y con expectativas de calidad
-- MAGIC Aquí, declararemos nuestra tabla silver limpia, que aplica reglas de negocio para asegurar que nuestros procesos que dependen de los datos que estamos ingiriendo tendrán información íntegra para trabajar. También aplicamos transformaciones a los tipos de datos para que los procesos
-- MAGIC

-- COMMAND ----------

CREATE STREAMING LIVE TABLE ${clean_silver_table} (
  -- El nombre del establecimiento no puede estar vacío
  CONSTRAINT merchant_name_not_null EXPECT (merchant_name IS NOT NULL) ON VIOLATION DROP ROW
  -- La red de la tarjeta debe estar en la lista de tarjetas preaprobadas
  ,CONSTRAINT card_network_in_approved_list EXPECT (card_network IN ('Mastercard', 'Visa', 'Elo', 'Amex')) ON VIOLATION DROP ROW
  -- El número de cuotas debe ser mayor que cero
  ,CONSTRAINT installments_greater_than_zero EXPECT (installments > 0) ON VIOLATION DROP ROW
  -- Validando el BIN (primeros dígitos) de las tarjetas de crédito con las redes correspondientes 
  ,CONSTRAINT card_bin_in_card_network EXPECT ((card_network = 'Mastercard' AND card_bin IN (51, 52, 53, 54)) OR (card_network = 'Visa' AND card_bin IN (4)) OR (card_network = 'Amex' AND card_bin IN (34, 37)) OR (card_network = 'Elo' AND card_bin IN (636368, 636369, 438935, 504175, 451416, 636297, 5067, 4576, 4011, 506699))) ON VIOLATION DROP ROW
   -- Garantizando que el nombre de la persona está escrito correctamente
  ,CONSTRAINT card_holder_not_null EXPECT (card_holder is not NULL) ON VIOLATION DROP ROW
  -- El valor de un pago debe ser mayor a cero, y el de un reembolso menor a 0
  ,CONSTRAINT bill_value_valid EXPECT (((bill_value > 0) and (transaction_type = 'expense')) or ((bill_value < 0) and (transaction_type = 'chargeback'))) ON VIOLATION DROP ROW
  -- La fecha de vencimiento de la tarjeta debe ser mayor a la fecha de la transacción
  ,CONSTRAINT card_expiration_date_valid EXPECT (card_expiration_date > `timestamp`) ON VIOLATION DROP ROW
)
AS
SELECT 
  transaction_id
  ,type AS transaction_type
  ,to_timestamp(`timestamp`) as `timestamp`
  ,merchant_type
  ,merchant_name
  ,card_holder
  ,currency
  ,card_network
  ,CAST(card_bin as LONG) AS card_bin
  ,CAST(bill_value as DOUBLE) AS bill_value
  ,CAST(installments AS INT) as installments
  ,last_day(to_date(card_expiration_date,"MM/yy")) AS card_expiration_date
FROM 
  stream(live.${bronze_table_name})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Agregando nuestros datos diariamente para mejorar el rendimiento (y reducir costos) para el equipo de negocios que realizará análisis de ventas
-- MAGIC

-- COMMAND ----------


CREATE LIVE TABLE ${aggregated_gold_table} (
  merchant_type STRING COMMENT "Tipo de establecimiento involucrado en las transacciones",
  card_network STRING COMMENT "Red de tarjetas que emitió las tarjetas de crédito que realizaron las transacciones",
  timestamp_day DATE COMMENT "Fecha en la que ocurrieron las transacciones",
  sum_bill_value DOUBLE COMMENT "Suma del monto que se transaccionó en esa fecha para esa red de tarjetas y tipo de establecimiento"
)

  COMMENT "Tabla gold agregada diariamente para todos los datos transaccionales agrupados por red de tarjetas y tipo de establecimiento"

AS

SELECT
  merchant_type
  ,card_network
  ,CAST(date_trunc('DAY', `timestamp`) AS DATE) AS timestamp_day
  ,SUM(bill_value) AS sum_bill_value
FROM
  LIVE.${clean_silver_table}
GROUP BY
  ALL

