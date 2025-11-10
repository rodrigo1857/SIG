/****************************************************************************************
* ESQUEMA: sistema_informacion_gerencial
* DESCRIPCIÓN:
* Data Warehouse / Data Mart para el análisis de información presupuestaria y financiera,
* modelado como un esquema estrella/copo de nieve.
*
* COMPONENTES:
* - Tablas de Dimensiones (dm_*): Atributos descriptivos (quién, qué, dónde).
* - Tablas de Hechos (hechos_*): Métricas y eventos numéricos (cuánto).
* - Vistas Materializadas (vm_*): Instantáneas para optimizar el rendimiento de consultas.
*
* NOTA DE ORDEN:
* El orden de creación es crítico.
* 1. Dimensiones "Padre" (dm_area, dm_fuente, dm_generica, dm_clasificador).
* 2. Hechos "Central" (hechos_institucional_consolidados), que referencia a las
* dimensiones padre.
* 3. Dimensiones "Hijas" (dm_certificado, dm_expediente), que referencian a la
* tabla de hechos central.
* 4. Resto de tablas y vistas materializadas.
****************************************************************************************/

--- Creación del nuevo esquema
CREATE SCHEMA IF NOT EXISTS sistema_informacion_gerencial;

--- Ubicación del esquema
SET search_path TO sistema_informacion_gerencial;

------------------------------------------------------------------------------------------
-- 1. TABLAS DE DIMENSIONES "PADRE"
-- (No dependen de otras tablas o solo de otras dimensiones)
------------------------------------------------------------------------------------------

--- 1.1. dm_area
--- Propósito: Dimensión de áreas o unidades organizacionales.
---            Almacena la jerarquía de las áreas (id_superior, nivel).
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_area
(
    area_siaf   VARCHAR NOT NULL,
    cod_area    CHAR(6) NOT NULL,
    desc_area   VARCHAR NOT NULL,
    nivel       INTEGER,
    id_area     INTEGER,
    id_superior INTEGER,
    CONSTRAINT dm_area_pk PRIMARY KEY (area_siaf)
);

--- 1.2. dm_fuente
--- Propósito: Dimensión de fuentes de financiamiento (Ej. Recursos Ordinarios, RDR, etc.).
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_fuente
(
    fuente_siaf   VARCHAR NOT NULL,
    desc_fuente   VARCHAR NOT NULL,
    CONSTRAINT dm_fuente_pk PRIMARY KEY (fuente_siaf)
);

--- 1.3. dm_generica
--- Propósito: Dimensión de alto nivel para la clasificación del gasto (la "Genérica" del gasto,
---            Ej. "Gasto Corriente", "Gasto de Capital").
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_generica
(
    id_generica   INTEGER NOT NULL,
    generica_siaf VARCHAR NOT NULL,
    desc_generica VARCHAR,
    CONSTRAINT dm_generica_pk PRIMARY KEY (generica_siaf)
);

--- 1.4. dm_clasificador
--- Propósito: Dimensión detallada del clasificador presupuestario (gasto o ingreso).
---            Almacena la descripción específica de cada partida. Particionada por año.
---            Incluye un campo TSVECTOR (fts_clasificador) para búsqueda de texto completo.
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_clasificador
(
    idclasificador_siaf VARCHAR NOT NULL,
    generica            VARCHAR,
    clasificador        VARCHAR,
    descripcion         VARCHAR,
    fts_clasificador    TSVECTOR GENERATED ALWAYS AS (to_tsvector('spanish'::regconfig,
                                                                  (COALESCE(descripcion, '')::TEXT || ' '::TEXT) || (COALESCE(clasificador, '')::TEXT)
                                                      )) STORED,
    anio                INTEGER NOT NULL,
    CONSTRAINT dm_clasificador_pk PRIMARY KEY (anio, idclasificador_siaf)
)
    PARTITION BY LIST (anio);

------------------------------------------------------------------------------------------
-- 2. TABLA DE HECHOS CENTRAL
-- (Referencia a las dimensiones "Padre")
------------------------------------------------------------------------------------------

--- 2.1. hechos_institucional_consolidados
--- Propósito: Tabla de hechos principal (Fact table).
---            Consolida los montos de las fases del gasto (Certificado, Compromiso, Devengado, Girado)
---            a nivel de área, fuente, genérica y clasificador. Es el núcleo del análisis.
---            Particionada por año.
---
---            *** DEBE CREARSE ANTES de dm_certificado y dm_expediente ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.hechos_institucional_consolidados
(
    area_siaf                 VARCHAR NOT NULL,
    num_certificado           VARCHAR NOT NULL,
    anio                      INTEGER NOT NULL,
    monto_certificado         NUMERIC(19, 2) NOT NULL,
    id_hecho_institucional    BIGINT NOT NULL,
    monto_devengado           NUMERIC(19, 2),
    fuente_siaf               VARCHAR NOT NULL,
    generica_siaf             VARCHAR NOT NULL,
    idclasificador_siaf       VARCHAR NOT NULL,
    clasificador_siaf         VARCHAR,
    monto_compromiso_anual    NUMERIC(19, 2),
    monto_compromiso_mensual  NUMERIC(19, 2),
    monto_girado              NUMERIC(19, 2),

    CONSTRAINT hechos_institucional_consolidados_pk
        PRIMARY KEY (id_hecho_institucional, idclasificador_siaf, anio),

    CONSTRAINT hechos_institucional_consolidados_dm_area_area_siaf_fk
        FOREIGN KEY (area_siaf) REFERENCES sistema_informacion_gerencial.dm_area (area_siaf),

    CONSTRAINT hechos_institucional_consolidados_dm_clasificador_idclasificado
        FOREIGN KEY (anio, idclasificador_siaf) REFERENCES sistema_informacion_gerencial.dm_clasificador (anio, idclasificador_siaf),

    CONSTRAINT hechos_institucional_consolidados_dm_fuente_fuente_siaf_fk
        FOREIGN KEY (fuente_siaf) REFERENCES sistema_informacion_gerencial.dm_fuente (fuente_siaf),

    CONSTRAINT hechos_institucional_consolidados_dm_generica_generica_siaf_fk
        FOREIGN KEY (generica_siaf) REFERENCES sistema_informacion_gerencial.dm_generica (generica_siaf)
)
    PARTITION BY LIST (anio);



create unique index hechos_institucional_consolidados_num_certificado_anio_fuente_s
    on sistema_informacion_gerencial.hechos_institucional_consolidados (num_certificado, anio, fuente_siaf, idclasificador_siaf);

------------------------------------------------------------------------------------------
-- 3. TABLAS DE DIMENSIONES "HIJAS" / DETALLE
-- (Dependen de la tabla de Hechos Central)
------------------------------------------------------------------------------------------

--- 3.1. dm_certificado
--- Propósito: Detalle de los certificados presupuestarios (Nivel de detalle).
---            Almacena información específica de cada certificado, vinculada a los hechos.
---            Particionada por año.
---            *** DEPENDE DE hechos_institucional_consolidados ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_certificado
(
    id_hecho_institucional BIGINT,
    anio                   INTEGER NOT NULL,
    num_certificado        VARCHAR NOT NULL,
    area_siaf              VARCHAR,
    secuencia              VARCHAR NOT NULL,
    ejecutora              VARCHAR,
    monto_clasificador     NUMERIC(19, 2),
    fuente_siaf            VARCHAR,
    glosa                  VARCHAR,
    correlativo            VARCHAR NOT NULL,
    idclasificador_siaf    VARCHAR NOT NULL,
    clasificador           VARCHAR,
    generica_siaf          VARCHAR,
    cod_doc                VARCHAR,
    num_doc                VARCHAR,
    estado_envio           VARCHAR,
    estado_registro        VARCHAR,
    fecha_creacion_clt     DATE,
    idmeta                 VARCHAR NOT NULL,
    codmeta                VARCHAR,
    nomb_met_ins           VARCHAR,

    CONSTRAINT dm_certificado_hechos_institucional_consolidados_anio_id_hechos
        FOREIGN KEY (anio, id_hecho_institucional, idclasificador_siaf)
            REFERENCES sistema_informacion_gerencial.hechos_institucional_consolidados (anio, id_hecho_institucional, idclasificador_siaf),

    CONSTRAINT dm_certificado_pk
        PRIMARY KEY (anio, id_hecho_institucional, secuencia, correlativo, idclasificador_siaf, idmeta)
)
    PARTITION BY LIST (anio);

--- 3.2. dm_expediente
--- Propósito: Detalle de los expedientes SIAF (fases de Compromiso, Devengado, Girado).
---            Almacena el detalle de cada operación. Particionada por año.
---            *** DEPENDE DE hechos_institucional_consolidados ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_expediente
(
    anio                   INTEGER NOT NULL,
    ejecutora              CHAR(6),
    expediente             VARCHAR NOT NULL,
    fase                   VARCHAR,
    secuencia              VARCHAR NOT NULL,
    correlativo            VARCHAR NOT NULL,
    ciclo                  VARCHAR,
    fecha_autorizacion     DATE,
    clasificador           VARCHAR,
    monto_nacional         NUMERIC(19, 2),
    cod_doc                VARCHAR NOT NULL,
    num_doc                VARCHAR,
    estado_envio           VARCHAR,
    idclasificador_siaf    VARCHAR NOT NULL,
    trimestre              INTEGER,
    id_hecho_institucional BIGINT NOT NULL,
    certificado            VARCHAR,
    certificado_secuencia  VARCHAR,

    CONSTRAINT dm_expediente_hechos_institucional_consolidados_id_hechos_insti
        FOREIGN KEY (anio, id_hecho_institucional, idclasificador_siaf)
            REFERENCES sistema_informacion_gerencial.hechos_institucional_consolidados (anio, id_hecho_institucional, idclasificador_siaf),

    CONSTRAINT dm_expediente_pk
        PRIMARY KEY (anio, id_hecho_institucional, expediente, secuencia, correlativo, idclasificador_siaf, ciclo, fase)
)
    PARTITION BY LIST (anio);

------------------------------------------------------------------------------------------
-- 4. RESTO DE TABLAS (DIMENSIONES, HECHOS Y TABLAS DE TRABAJO)
-- (SECCIÓN REORDENADA PARA CORREGIR DEPENDENCIAS)
------------------------------------------------------------------------------------------

--- 4.1. hechos_pim (MOVILIZADA)
--- Propósito: Tabla de hechos (o instantánea) que almacena los montos del PIA y PIM
---            a nivel de ejecutora, fuente y genérica. Particionada por año.
---            *** DEBE CREARSE ANTES de dm_pim_q20 y dm_pim_clasificador ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.hechos_pim
(
    anio          INTEGER NOT NULL,
    fuente_siaf   VARCHAR NOT NULL,
    generica_siaf VARCHAR NOT NULL,
    monto_pia     NUMERIC(19, 2),
    monto_pim     NUMERIC(19, 2),
    CONSTRAINT hechos_pim_pk PRIMARY KEY (anio,fuente_siaf, generica_siaf)
)
    PARTITION BY LIST (anio);

--- 4.2. dm_pim_q20 (DEPENDE DE hechos_pim)
--- Propósito: Almacena los montos PIA y PIM, agregados por área, fuente y genérica.
---            *** DEPENDE DE hechos_pim ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_pim_q20
(
    anio integer NOT NULL,
    fuente_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    id_area integer NOT NULL,
    area_siaf character varying COLLATE pg_catalog."default",
    id_generica integer,
    monto_pia numeric(19,2) NOT NULL,
    monto_pim numeric(19,2) NOT NULL,
    generica_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT dm_pim_q20_pk PRIMARY KEY (anio, id_area, fuente_siaf, generica_siaf),
    CONSTRAINT dm_pim_q20_hechos_pim_anio_fuente_siaf_generica_siaf_fk FOREIGN KEY (generica_siaf, fuente_siaf, anio)
        REFERENCES sistema_informacion_gerencial.hechos_pim (generica_siaf, fuente_siaf, anio)
) PARTITION BY LIST (anio);

--- 4.3. dm_pim_clasificador (DEPENDE DE hechos_pim)
--- Propósito: Tabla agregada que almacena el monto PIM a nivel de clasificador.
---            *** DEPENDE DE hechos_pim ***
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_pim_clasificador
(
    anio integer NOT NULL,
    fuente_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    idclasificador_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    generica_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    monto_pim numeric(19,2),
    CONSTRAINT dm_pim_clasificador_pk PRIMARY KEY (anio, fuente_siaf, generica_siaf, idclasificador_siaf),
    CONSTRAINT dm_pim_clasificador_hechos_pim_anio_fuente_siaf_generica_siaf_f FOREIGN KEY (generica_siaf, fuente_siaf, anio)
        REFERENCES sistema_informacion_gerencial.hechos_pim (generica_siaf, fuente_siaf, anio)
) PARTITION BY LIST (anio);

--- 4.4. hechos_rrhh_consolidados
--- Propósito: Tabla de hechos para gastos de Recursos Humanos.
---            Consolida montos de planillas, número de trabajadores, vinculado a
---            clasificadores y metas. Particionada por año.
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.hechos_rrhh_consolidados
(
    anio                   INTEGER NOT NULL,
    certificado            VARCHAR NOT NULL,
    id_planilla            INTEGER NOT NULL,
    num_planilla           VARCHAR NOT NULL,
    area_siaf              VARCHAR NOT NULL,
    cod_tipo_pla           VARCHAR,
    nomb_tipo_pla          VARCHAR,
    cod_tipo_trabajador    INTEGER,
    desc_tipo_trabajador   VARCHAR,
    cod_estado_trabajador  INTEGER,
    desc_estado_trabajador VARCHAR,
    fuente_siaf            VARCHAR NOT NULL,
    generica_siaf          VARCHAR NOT NULL,
    idclasificador_siaf    VARCHAR NOT NULL,
    cantidad_trabajadores  INTEGER,
    monto_certificado      NUMERIC(19, 2),
    monto_expediente       NUMERIC(19, 2),
    id_meta                INTEGER NOT NULL,
    cod_meta               VARCHAR NOT NULL,

    CONSTRAINT hechos_rrhh_consolidados_pk
        PRIMARY KEY (id_meta, id_planilla, area_siaf, idclasificador_siaf, fuente_siaf, generica_siaf, certificado, anio)
)
    PARTITION BY LIST (anio);

--- 4.5. vw_obras_materializada
--- Propósito: Tabla que consolida información de obras, integrando datos logísticos
---            (requerimiento, OC) con datos financieros (SIAF, certificado).
---            (Nota: Está definida como CREATE TABLE, no como VISTA).
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.vw_obras_materializada
(
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
    monto             NUMERIC(19, 2),
    num_requerimiento VARCHAR,
    num_hoja_ruta     VARCHAR,
    num_oc            VARCHAR,
    num_siaf          VARCHAR,
    num_certificado   VARCHAR,
    monto_certificado NUMERIC(19, 2),
    estado            VARCHAR,
    oficina           VARCHAR
);

--- 4.5. dm_ejecucion_q20
--- Propósito: Tabla que consolida información de ejecucion proveniente del sistema Q20 en una tabla consolidada por
---            certificado

CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_ejecucion_q20
(
    id_certificacion bigint,
    anio integer NOT NULL,
    area_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    num_certificado character varying COLLATE pg_catalog."default" NOT NULL,
    fuente_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    idclasificador_siaf character varying COLLATE pg_catalog."default" NOT NULL,
    clasificador_siaf character varying COLLATE pg_catalog."default",
    monto_certificado numeric DEFAULT 0,
    monto_compromiso_anual numeric DEFAULT 0,
    monto_devengado numeric DEFAULT 0,
    monto_girado numeric DEFAULT 0,
    CONSTRAINT dm_ejecucion_q20_pk PRIMARY KEY (anio, fuente_siaf, idclasificador_siaf, num_certificado),
    CONSTRAINT dm_ejecucion_q20_hechos_institucional_consolidados_num_certific FOREIGN KEY (idclasificador_siaf, fuente_siaf, num_certificado, anio)
        REFERENCES sistema_informacion_gerencial.hechos_institucional_consolidados (idclasificador_siaf, fuente_siaf, num_certificado, anio),
)PARTITION BY LIST (anio);




------------------------------------------------------------------------------------------
-- 5. VISTAS MATERIALIZADAS (VM)
------------------------------------------------------------------------------------------

--- 5.1. VISTAS MATERIALIZADAS SIMPLES
--- Propósito: Instantáneas 1:1 de las tablas base para optimizar consultas.

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_dm_area
AS SELECT * FROM sistema_informacion_gerencial.dm_area;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_area ON sistema_informacion_gerencial.vm_dm_area(area_siaf);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_dm_fuente
AS SELECT * FROM sistema_informacion_gerencial.dm_fuente;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_fuente ON sistema_informacion_gerencial.vm_dm_fuente(fuente_siaf);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_dm_generica
AS SELECT * FROM sistema_informacion_gerencial.dm_generica;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_generica ON sistema_informacion_gerencial.vm_dm_generica(generica_siaf);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_hechos_institucional_consolidados
AS SELECT * FROM sistema_informacion_gerencial.hechos_institucional_consolidados;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_hechos_institucional_consolidados ON  sistema_informacion_gerencial.vm_hechos_institucional_consolidados(id_hecho_institucional, idclasificador_siaf, anio);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_dm_certificado
AS SELECT * FROM sistema_informacion_gerencial.dm_certificado;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_certificado ON sistema_informacion_gerencial.vm_dm_certificado(anio, id_hecho_institucional, secuencia, correlativo, idclasificador_siaf, idmeta);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_dm_expediente
AS SELECT * FROM sistema_informacion_gerencial.dm_expediente;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_expediente ON sistema_informacion_gerencial.vm_dm_expediente(anio, id_hecho_institucional, expediente, secuencia, correlativo, idclasificador_siaf, ciclo, fase);

CREATE MATERIALIZED VIEW IF NOT EXISTS vm_hechos_pim
AS SELECT * FROM sistema_informacion_gerencial.hechos_pim;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_hechos_pim ON sistema_informacion_gerencial.vm_hechos_pim(anio,fuente_siaf, generica_siaf);

CREATE MATERIALIZED VIEW IF NOT EXISTS sistema_informacion_gerencial.vm_dm_pim_q20
AS SELECT * FROM sistema_informacion_gerencial.dm_pim_q20;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_dm_pim ON sistema_informacion_gerencial.vm_dm_pim_q20(anio, id_area, fuente_siaf, generica_siaf);

--- 5.2. VISTAS MATERIALIZADAS COMPLEJAS

--- 5.2.1. vm_pim_clasificador
--- Propósito: Consolida el PIM y el Devengado por clasificador.
---            Une datos del SIAF (devengados de expedientes) y de RRHH (Q20, devengados de planillas).
---            Agrupa los devengados por origen (SIAF, Q20) en un campo JSONB.
CREATE MATERIALIZED VIEW IF NOT EXISTS sistema_informacion_gerencial.vm_pim_clasificador AS
WITH
    devengado_x_clasificador AS (
        SELECT
            de.anio,
            hic.fuente_siaf,
            de.idclasificador_siaf,
            sum(de.monto_nacional) AS monto_devengado
        FROM
            sistema_informacion_gerencial.dm_expediente de
                JOIN sistema_informacion_gerencial.hechos_institucional_consolidados hic
                     ON de.id_hecho_institucional = hic.id_hecho_institucional
                     AND de.idclasificador_siaf = hic.idclasificador_siaf
        WHERE
            de.ciclo = 'G' AND de.fase = 'D'
        GROUP BY
            de.anio,
            de.idclasificador_siaf,
            hic.fuente_siaf
    ),
    pim_x_clasificador AS (
        SELECT
            dpc.anio,
            dpc.fuente_siaf,
            dpc.generica_siaf,
            dpc.idclasificador_siaf,
            dcla.clasificador,
            dcla.fts_clasificador,
            dcla.descripcion,
            dpc.monto_pim
        FROM
            sistema_informacion_gerencial.dm_pim_clasificador dpc
                JOIN sistema_informacion_gerencial.dm_clasificador dcla
                     ON dpc.idclasificador_siaf::TEXT = dcla.idclasificador_siaf::TEXT
                         AND dpc.anio = dcla.anio
    ),
    unioned AS (
        -- Devengados del SIAF
        SELECT
            pxc.anio,
            pxc.fuente_siaf,
            pxc.idclasificador_siaf,
            pxc.clasificador,
            pxc.fts_clasificador,
            pxc.generica_siaf,
            pxc.descripcion,
            'SIAF'::TEXT AS origen,
            pxc.monto_pim,
            dxc.monto_devengado
        FROM
            pim_x_clasificador pxc
                JOIN devengado_x_clasificador dxc
                     ON pxc.anio = dxc.anio
                         AND pxc.fuente_siaf::TEXT = dxc.fuente_siaf::TEXT
                         AND pxc.idclasificador_siaf::TEXT = dxc.idclasificador_siaf::TEXT
        WHERE
            pxc.monto_pim > 0::NUMERIC
        UNION ALL
        -- Devengados de RRHH (Q20)
        SELECT
            hrc.anio,
            hrc.fuente_siaf,
            dc.idclasificador_siaf,
            dc.clasificador,
            dc.fts_clasificador,
            dc.generica AS generica_siaf,
            dc.descripcion,
            'Q20'::TEXT AS origen,
            dpc.monto_pim,
            sum(hrc.monto_expediente) AS monto_devengado
        FROM
            sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
                JOIN sistema_informacion_gerencial.dm_clasificador dc
                     ON hrc.idclasificador_siaf::TEXT = dc.idclasificador_siaf::TEXT
                         AND hrc.anio = dc.anio
                JOIN sistema_informacion_gerencial.dm_pim_clasificador dpc
                     ON hrc.anio = dpc.anio
                         AND hrc.fuente_siaf::TEXT = dpc.fuente_siaf::TEXT
                         AND hrc.idclasificador_siaf::TEXT = dpc.idclasificador_siaf::TEXT
        GROUP BY
            hrc.anio,
            dc.idclasificador_siaf,
            hrc.fuente_siaf,
            dc.clasificador,
            dc.fts_clasificador,
            dc.generica,
            dpc.monto_pim,
            dc.descripcion
    )
-- Agrupación final, consolidando orígenes en JSONB
SELECT
    unioned.anio,
    unioned.fuente_siaf,
    unioned.idclasificador_siaf,
    unioned.clasificador,
    unioned.fts_clasificador,
    unioned.generica_siaf,
    unioned.descripcion,
    'SIAF'::TEXT AS origen, -- Este 'origen' parece ser un alias fijo, no el del JSON
    unioned.monto_pim,
    jsonb_object_agg(unioned.origen, unioned.monto_devengado) AS devengados_por_origen
FROM
    unioned
GROUP BY
    unioned.anio,
    unioned.fuente_siaf,
    unioned.idclasificador_siaf,
    unioned.clasificador,
    unioned.fts_clasificador,
    unioned.generica_siaf,
    unioned.descripcion,
    unioned.monto_pim
ORDER BY
    unioned.anio,
    unioned.idclasificador_siaf;

ALTER MATERIALIZED VIEW IF EXISTS sistema_informacion_gerencial.vm_pim_clasificador OWNER TO postgres;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_pim_clasificador ON sistema_informacion_gerencial.vm_pim_clasificador(anio, fuente_siaf, idclasificador_siaf, origen);

--- 5.2.2. vm_search_clasificador_area
--- Propósito: Agrega el monto devengado por clasificador y área.
---            Une devengados SIAF y RRHH (Q20).
---            Incluye lógica de negocio para agrupar áreas:
---            1. Áreas con id_superior = 10468 se muestran individualmente.
---            2. Áreas con id_superior <> 10468 (o nulo) se agrupan bajo 'ADMINISTRACION CENTRAL'.
CREATE MATERIALIZED VIEW IF NOT EXISTS sistema_informacion_gerencial.vm_search_clasificador_area AS
-- Parte 1: Devengado SIAF para áreas específicas (id_superior = 10468)
SELECT
    de.anio,
    hic.fuente_siaf,
    hic.area_siaf,
    da.cod_area,
    da.desc_area,
    dcl.generica AS generica_siaf,
    dcl.clasificador,
    dcl.descripcion AS desc_clasificador,
    'SIAF'::VARCHAR AS origen,
    sum(de.monto_nacional) AS monto_devengado
FROM
    sistema_informacion_gerencial.vm_dm_expediente de
        JOIN sistema_informacion_gerencial.vm_hechos_institucional_consolidados hic
             ON de.id_hecho_institucional = hic.id_hecho_institucional
        JOIN sistema_informacion_gerencial.dm_clasificador dcl
             ON de.idclasificador_siaf::TEXT = dcl.idclasificador_siaf::TEXT
                 AND dcl.anio = hic.anio
        JOIN sistema_informacion_gerencial.dm_area da
             ON hic.area_siaf::TEXT = da.area_siaf::TEXT
WHERE
    da.id_superior = 10468 AND de.ciclo = 'G' AND de.fase = 'D'
GROUP BY
    de.idclasificador_siaf, de.anio, dcl.descripcion, hic.fuente_siaf,
    dcl.clasificador, dcl.generica, da.id_superior, da.desc_area,
    da.cod_area, hic.area_siaf

UNION ALL

-- Parte 2: Devengado SIAF para 'ADMINISTRACION CENTRAL' (resto de áreas)
SELECT
    de.anio,
    hic.fuente_siaf,
    '0001'::VARCHAR AS area_siaf,
    'D65'::BPCHAR AS cod_area,
    'ADMINISTRACION CENTRAL'::VARCHAR AS desc_area,
    dcl.generica AS generica_siaf,
    dcl.clasificador,
    dcl.descripcion AS desc_clasificador,
    'SIAF'::VARCHAR AS origen,
    sum(de.monto_nacional) AS monto_devengado
FROM
    sistema_informacion_gerencial.vm_dm_expediente de
        JOIN sistema_informacion_gerencial.vm_hechos_institucional_consolidados hic
             ON de.id_hecho_institucional = hic.id_hecho_institucional
        JOIN sistema_informacion_gerencial.dm_clasificador dcl
             ON de.idclasificador_siaf::TEXT = dcl.idclasificador_siaf::TEXT
                 AND dcl.anio = hic.anio
        JOIN sistema_informacion_gerencial.dm_area da
             ON hic.area_siaf::TEXT = da.area_siaf::TEXT
WHERE
    (da.id_superior <> 10468 OR da.id_superior IS NULL)
  AND de.ciclo = 'G'
  AND de.fase = 'D'
GROUP BY
    de.idclasificador_siaf, de.anio, dcl.descripcion, hic.fuente_siaf,
    dcl.clasificador, dcl.generica

UNION ALL

-- Parte 3: Devengado RRHH (Q20) para áreas específicas (id_superior = 10468)
SELECT
    hrc.anio,
    hrc.fuente_siaf,
    hrc.area_siaf,
    da.cod_area,
    da.desc_area,
    hrc.generica_siaf,
    dc.clasificador,
    dc.descripcion AS desc_clasificador,
    'Q20'::VARCHAR AS origen,
    sum(hrc.monto_expediente) AS monto_devengado
FROM
    sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
        JOIN sistema_informacion_gerencial.dm_clasificador dc
             ON hrc.idclasificador_siaf::TEXT = dc.idclasificador_siaf::TEXT
                 AND hrc.anio = dc.anio
        JOIN sistema_informacion_gerencial.dm_area da
             ON hrc.area_siaf::TEXT = da.area_siaf::TEXT
WHERE
    da.id_superior = 10468
GROUP BY
    dc.clasificador, hrc.area_siaf, da.desc_area, da.cod_area,
    hrc.anio, hrc.fuente_siaf, hrc.generica_siaf, dc.descripcion

UNION ALL

-- Parte 4: Devengado RRHH (Q20) para 'ADMINISTRACION CENTRAL' (resto de áreas)
SELECT DISTINCT
    hrc.anio,
    hrc.fuente_siaf,
    '0001'::TEXT AS area_siaf,
    'D65'::TEXT AS cod_area,
    'ADMINISTRACION CENTRAL'::TEXT AS desc_area,
    hrc.generica_siaf,
    dc.clasificador,
    dc.descripcion AS desc_clasificador,
    'Q20'::TEXT AS origen,
    sum(hrc.monto_expediente) AS monto_devengado
FROM
    sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
        JOIN sistema_informacion_gerencial.dm_clasificador dc
             ON hrc.idclasificador_siaf::TEXT = dc.idclasificador_siaf::TEXT
                 AND hrc.anio = dc.anio
        JOIN sistema_informacion_gerencial.dm_area da
             ON hrc.area_siaf::TEXT = da.area_siaf::TEXT
WHERE
    da.id_superior <> 10468 OR da.id_superior IS NULL
GROUP BY
    dc.clasificador, hrc.anio, hrc.fuente_siaf, hrc.generica_siaf,
    dc.descripcion;

ALTER MATERIALIZED VIEW IF EXISTS sistema_informacion_gerencial.vm_search_clasificador_area OWNER TO postgres;
CREATE UNIQUE INDEX IF NOT EXISTS idx_vm_search_clasificador_area ON sistema_informacion_gerencial.vm_search_clasificador_area(anio, fuente_siaf, clasificador, area_siaf, origen);
