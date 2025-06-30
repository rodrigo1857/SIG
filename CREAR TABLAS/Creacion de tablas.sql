-- 0. Crear el esquema y fijar search_path
CREATE SCHEMA IF NOT EXISTS sistema_informacion_gerencial;
SET search_path TO sistema_informacion_gerencial;


-- 1. ------------ Dimensiones básicas ------------

-- 1.1 dm_area
CREATE TABLE IF NOT EXISTS dm_area (
  cod_siaf_area     VARCHAR NOT NULL PRIMARY KEY,
  area_name         CHAR(6) NOT NULL,
  area_display_name VARCHAR NOT NULL,
  nivel             INTEGER,
  id_area           INTEGER,
  idsuperior        INTEGER
);
ALTER TABLE dm_area OWNER TO postgres;

-- 1.2 dm_fuente
CREATE TABLE IF NOT EXISTS dm_fuente (
  fuente_siaf VARCHAR NOT NULL PRIMARY KEY,
  desc_fuente VARCHAR NOT NULL
);
ALTER TABLE dm_fuente OWNER TO postgres;

-- 1.3 dm_generica
CREATE TABLE IF NOT EXISTS dm_generica (
  id_generica   INTEGER NOT NULL PRIMARY KEY,
  cod_generica  VARCHAR,
  nomb_generica VARCHAR
);
ALTER TABLE dm_generica OWNER TO postgres;


-- 2. --------- Hechos institucional consolidados (PARTICIONADA) ---------

-- 2.1 Tabla padre
CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados (
  cod_siaf_area   VARCHAR    NOT NULL
    REFERENCES dm_area(cod_siaf_area),
  num_certificado VARCHAR    NOT NULL,
  anio            INTEGER    NOT NULL,
  monto           NUMERIC(19,2) NOT NULL,
  CONSTRAINT pk_hi_conso UNIQUE (num_certificado, anio)
)
PARTITION BY LIST (anio);

ALTER TABLE hechos_institucional_consolidados OWNER TO postgres;

-- 2.2 Particiones de ejemplo
CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados_2023
  PARTITION OF hechos_institucional_consolidados
  FOR VALUES IN (2023);
ALTER TABLE hechos_institucional_consolidados_2023 OWNER TO postgres;

CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados_2024
  PARTITION OF hechos_institucional_consolidados
  FOR VALUES IN (2024);
ALTER TABLE hechos_institucional_consolidados_2024 OWNER TO postgres;

CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados_2025
  PARTITION OF hechos_institucional_consolidados
  FOR VALUES IN (2025);
ALTER TABLE hechos_institucional_consolidados_2025 OWNER TO postgres;

-- Partición DEFAULT para cualquier otro año
CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados_default
  PARTITION OF hechos_institucional_consolidados DEFAULT;
ALTER TABLE hechos_institucional_consolidados_default OWNER TO postgres;


-- 3. --------- dm_certificado (sin FK directo) ---------

CREATE TABLE IF NOT EXISTS dm_certificado (
  ano_eje            INTEGER,
  num_certificado    VARCHAR,
  cod_siaf_area      VARCHAR,
  secuencia          VARCHAR,
  sec_ejec           VARCHAR,
  monto_clasificador NUMERIC(19,2),
  siaf_id_fuente     VARCHAR,
  glosa              VARCHAR,
  correlativo        VARCHAR,
  siaf_id_clasificador VARCHAR,
  clasificador       VARCHAR,
  generica           VARCHAR,
  cod_doc            VARCHAR,
  num_doc            VARCHAR,
  estado_envio       VARCHAR,
  estado_registro    VARCHAR,
  fecha_creacion_clt DATE,
  idmeta             VARCHAR,
  codmeta            VARCHAR,
  nomb_met_ins       VARCHAR
);
ALTER TABLE dm_certificado OWNER TO postgres;

-- 3.1 Trigger de validación de integridad (reemplaza la FK)
CREATE OR REPLACE FUNCTION trg_validate_hi_conso()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM hechos_institucional_consolidados h
     WHERE h.num_certificado = NEW.num_certificado
       AND h.anio            = NEW.ano_eje
  ) THEN
    RAISE EXCEPTION
      'No existe certificado % del año % en hechos_institucional_consolidados',
      NEW.num_certificado, NEW.ano_eje;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chk_hi_conso_on_dm_certificado
  BEFORE INSERT OR UPDATE ON dm_certificado
  FOR EACH ROW EXECUTE FUNCTION trg_validate_hi_conso();


-- 4. --------- dm_expediente ---------

CREATE TABLE IF NOT EXISTS dm_expediente (
  ano_eje               INTEGER    NOT NULL,
  sec_ejec              CHAR(6),
  area_siaf             VARCHAR,
  expediente            VARCHAR,
  fase                  VARCHAR,
  secuencia             VARCHAR,
  certificado_secuencia VARCHAR,
  correlativo           VARCHAR,
  ciclo                 VARCHAR,
  certificado           VARCHAR,
  fecha_autorizacion    DATE,
  fuente_siaf           VARCHAR,
  clasificador          VARCHAR,
  generica              VARCHAR,
  monto_nacional        NUMERIC(19,2),
  cod_doc               VARCHAR,
  num_doc               VARCHAR,
  estado_envio          VARCHAR,
  siaf_id_clasificador  VARCHAR
);
ALTER TABLE dm_expediente OWNER TO postgres;


-- 5. --------- hechos_pim ---------

CREATE TABLE IF NOT EXISTS hechos_pim (
  id_area        INTEGER,
  cod_siaf_area  VARCHAR,
  id_fuente      INTEGER      NOT NULL,
  monto_pia      NUMERIC(19,2) NOT NULL,
  monto_pim      NUMERIC(19,2) NOT NULL,
  id_periodo_pla INTEGER      NOT NULL,
  generica       VARCHAR,
  id_generica    INTEGER
    REFERENCES dm_generica(id_generica),
  fuente_siaf    VARCHAR
);
ALTER TABLE hechos_pim OWNER TO postgres;


-- 6. --------- vw_obras_materializada (vista materializada) ---------

CREATE TABLE IF NOT EXISTS vw_obras_materializada (
  id_area_usuaria   INTEGER,
  area_description  VARCHAR,
  id_item           INTEGER,
  desc_item         VARCHAR,
  id_fecha          INTEGER,
  id_obra           INTEGER,
  nomb_obra         VARCHAR,
  cui               VARCHAR,
  cantidad          VARCHAR,
  meta              VARCHAR,
  monto             NUMERIC(19,2),
  num_requerimiento VARCHAR,
  num_hoja_ruta     VARCHAR,
  num_oc            VARCHAR,
  num_siaf          VARCHAR,
  num_certificado   VARCHAR,
  monto_certificado NUMERIC(19,2),
  estado            VARCHAR,
  oficina           VARCHAR
);
ALTER TABLE vw_obras_materializada OWNER TO postgres;
