-- ------------------------------------------------------------------
-- Script de creación de esquema y tablas en sistema_informacion_gerencial
-- Algunas tablas particionadas por LIST(anio) – una partición = un año
-- ------------------------------------------------------------------

-- 1️⃣ Crear esquema (si no existe) y apuntar a él
CREATE SCHEMA IF NOT EXISTS sistema_informacion_gerencial;
SET search_path TO sistema_informacion_gerencial;

-- 2️⃣ Tablas estáticas (sin particiones) --------------------------

-- dm_area
CREATE TABLE IF NOT EXISTS dm_area (
    cod_siaf_area     VARCHAR NOT NULL PRIMARY KEY,
    area_name         CHAR(6) NOT NULL,
    area_display_name VARCHAR NOT NULL,
    nivel             INTEGER,
    id_area           INTEGER,
    idsuperior        INTEGER
);
ALTER TABLE dm_area OWNER TO postgres;

-- dm_fuente
CREATE TABLE IF NOT EXISTS dm_fuente (
    fuente_siaf VARCHAR NOT NULL PRIMARY KEY,
    desc_fuente VARCHAR NOT NULL
);
ALTER TABLE dm_fuente OWNER TO postgres;

-- dm_generica
CREATE TABLE IF NOT EXISTS dm_generica (
    id_generica   INTEGER NOT NULL PRIMARY KEY,
    cod_generica  VARCHAR    UNIQUE,
    nomb_generica VARCHAR
);
ALTER TABLE dm_generica OWNER TO postgres;

-- hechos_pim
CREATE TABLE IF NOT EXISTS hechos_pim (
    id_periodo_pla INTEGER   NOT NULL,
    id_fuente      INTEGER   NOT NULL,
    fuente_siaf    VARCHAR   NOT NULL REFERENCES dm_fuente(fuente_siaf),
    id_area        INTEGER,
    cod_siaf_area  VARCHAR,
    id_generica    INTEGER   REFERENCES dm_generica(id_generica),
    generica       VARCHAR,
    monto_pia      NUMERIC(19,2) NOT NULL,
    monto_pim      NUMERIC(19,2) NOT NULL
);
ALTER TABLE hechos_pim OWNER TO postgres;

-- vw_obras_materializada (vista materializada)
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

-- hechos_rrhh_consolidados
CREATE TABLE IF NOT EXISTS hechos_rrhh_consolidados (
    tipo            VARCHAR NOT NULL,
    trabajador      VARCHAR NOT NULL,
    cantidad        INTEGER NOT NULL,
    monto           NUMERIC(19,2) NOT NULL,
    fecha           DATE    NOT NULL,
    mes             VARCHAR NOT NULL,
    trimestre       VARCHAR NOT NULL,
    generica        VARCHAR NOT NULL,
    clasificador    VARCHAR NOT NULL,
    tipo_subvencion VARCHAR NOT NULL
);
ALTER TABLE hechos_rrhh_consolidados OWNER TO postgres;


-- 3️⃣ Tablas particionadas por LIST(anio) ---------------------------

-- 3.1 hechos_institucional_consolidados
CREATE TABLE IF NOT EXISTS hechos_institucional_consolidados (
    cod_siaf_area   VARCHAR NOT NULL REFERENCES dm_area(cod_siaf_area),
    num_certificado VARCHAR NOT NULL,
    anio            INTEGER NOT NULL,
    monto           NUMERIC(19,2) NOT NULL,
    PRIMARY KEY (num_certificado, anio)
)
PARTITION BY LIST (anio);
ALTER TABLE hechos_institucional_consolidados OWNER TO postgres;



-- 3.2 dm_certificado
CREATE TABLE IF NOT EXISTS dm_certificado (
    ano_eje              INTEGER NOT NULL,
    num_certificado      VARCHAR NOT NULL,
    cod_siaf_area        VARCHAR,
    secuencia            VARCHAR,
    sec_ejec             VARCHAR,
    monto_clasificador   NUMERIC(19,2),
    fuente_siaf          VARCHAR REFERENCES dm_fuente(fuente_siaf),
    glosa                VARCHAR,
    correlativo          VARCHAR,
    siaf_id_clasificador VARCHAR,
    clasificador         VARCHAR,
    generica             VARCHAR REFERENCES dm_generica(cod_generica),
    cod_doc              VARCHAR,
    num_doc              VARCHAR,
    estado_envio         VARCHAR,
    estado_registro      VARCHAR,
    fecha_creacion_clt   DATE,
    idmeta               VARCHAR,
    codmeta              VARCHAR,
    nomb_met_ins         VARCHAR,
)
PARTITION BY LIST (ano_eje);
ALTER TABLE dm_certificado OWNER TO postgres;



-- 3.3 dm_expediente
CREATE TABLE IF NOT EXISTS dm_expediente (
    ano_eje               INTEGER NOT NULL,
    sec_ejec              CHAR(6),
    area_siaf             VARCHAR,
    expediente            VARCHAR,
    fase                  VARCHAR,
    secuencia             VARCHAR,
    correlativo           VARCHAR,
    ciclo                 VARCHAR,
    certificado           VARCHAR,
    certificado_secuencia VARCHAR,
    fecha_autorizacion    DATE,
    fuente_siaf           VARCHAR REFERENCES dm_fuente(fuente_siaf),
    clasificador          VARCHAR,
    generica              VARCHAR REFERENCES dm_generica(cod_generica),
    monto_nacional        NUMERIC(19,2),
    cod_doc               VARCHAR,
    num_doc               VARCHAR,
    estado_envio          VARCHAR,
    siaf_id_clasificador  VARCHAR,
)
PARTITION BY LIST (ano_eje);
ALTER TABLE dm_expediente OWNER TO postgres;

