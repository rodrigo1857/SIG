--- creacion del nuevo esquema
CREATE SCHEMA IF NOT EXISTS sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;

---- creacion de las tablas
-------1
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_area
(
    area_siaf VARCHAR NOT NULL
        CONSTRAINT dm_area_pk
            PRIMARY KEY,
    cod_area  CHAR(6) NOT NULL,
    desc_area VARCHAR NOT NULL,
    nivel     INTEGER,
    id_area   INTEGER,
    id_superior INTEGER
);


-------2
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_fuente
(
    fuente_siaf VARCHAR NOT NULL
        CONSTRAINT dm_fuente_pk
            PRIMARY KEY,
    desc_fuente VARCHAR NOT NULL
);

-------3
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_generica
(
    id_generica   INTEGER NOT NULL,
    generica_siaf VARCHAR NOT NULL
        CONSTRAINT dm_generica_pk
            PRIMARY KEY,
    desc_generica VARCHAR
);

-------4
CREATE TABLE sistema_informacion_gerencial.hechos_institucional_consolidados
(
    area_siaf             VARCHAR        NOT NULL
        CONSTRAINT hechos_institucional_consolidados_dm_area_area_siaf_fk
            REFERENCES sistema_informacion_gerencial.dm_area,
    num_certificado       VARCHAR        NOT NULL,
    anio                  INTEGER        NOT NULL,
    monto_certificado     NUMERIC(19, 2) NOT NULL,
    id_hecho_institucional BIGINT         NOT NULL,
    monto_devengado       NUMERIC(19, 2),
    fuente_siaf           VARCHAR        NOT NULL
        CONSTRAINT hechos_institucional_consolidados_dm_fuente_fuente_siaf_fk
            REFERENCES sistema_informacion_gerencial.dm_fuente,
    generica_siaf         VARCHAR        NOT NULL
        CONSTRAINT hechos_institucional_consolidados_dm_generica_generica_siaf_fk
            REFERENCES sistema_informacion_gerencial.dm_generica,
    idclasificador_siaf   VARCHAR,
    clasificador_siaf     VARCHAR,
    monto_compromiso_anual  NUMERIC(19, 2),
    monto_compromiso_mensual NUMERIC(19, 2),
    monto_girado          NUMERIC(19, 2),
    CONSTRAINT hechos_institucional_consolidados_pk
        -- (CORREGIDO) La PK ya estaba correcta, solo la verificamos.
        PRIMARY KEY (id_hecho_institucional, idclasificador_siaf, anio)
)
    PARTITION BY LIST (anio);


-------5
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_pim
(
    anio          INTEGER        NOT NULL,
    fuente_siaf   VARCHAR        NOT NULL,
    id_area       INTEGER,
    area_siaf     VARCHAR,
    id_generica   INTEGER,
    monto_pia     NUMERIC(19, 2) NOT NULL,
    monto_pim     NUMERIC(19, 2) NOT NULL,
    generica_siaf VARCHAR,
    -- (CORREGIDO) Añadida PK para que el índice de la MV sea válido.
    CONSTRAINT dm_pim_pk
        PRIMARY KEY (anio, id_area, fuente_siaf, generica_siaf)
);


-------6
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.hechos_pim
(
    anio          INTEGER NOT NULL,
    ejecutora     VARCHAR NOT NULL,
    fuente_siaf   VARCHAR NOT NULL,
    generica_siaf VARCHAR NOT NULL,
    monto_pia     NUMERIC(19, 2),
    monto_pim     NUMERIC(19, 2),
    -- (CORREGIDO) Añadida PK. Incluye 'anio' (clave de partición)
    CONSTRAINT hechos_pim_pk
        PRIMARY KEY (anio, ejecutora, fuente_siaf, generica_siaf)
)
    PARTITION BY LIST (anio);



-------7
CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_certificado
(
    id_hecho_institucional BIGINT,
    anio                   INTEGER        NOT NULL,
    num_certificado        VARCHAR        NOT NULL,
    area_siaf              VARCHAR,
    secuencia              VARCHAR        NOT NULL,
    ejecutora              VARCHAR,
    monto_clasificador     NUMERIC(19, 2),
    fuente_siaf            VARCHAR,
    glosa                  VARCHAR,
    correlativo            VARCHAR        NOT NULL,
    idclasificador_siaf    VARCHAR        NOT NULL,
    clasificador           VARCHAR,
    generica_siaf          VARCHAR,
    cod_doc                VARCHAR,
    num_doc                VARCHAR,
    estado_envio           VARCHAR,
    estado_registro        VARCHAR,
    fecha_creacion_clt     DATE,
    idmeta                 VARCHAR        NOT NULL,
    codmeta                VARCHAR,
    nomb_met_ins           VARCHAR,
    CONSTRAINT dm_certificado_hechos_institucional_consolidados_anio_id_hechos
        FOREIGN KEY (anio, id_hecho_institucional, idclasificador_siaf) REFERENCES sistema_informacion_gerencial.hechos_institucional_consolidados (anio, id_hecho_institucional, idclasificador_siaf),
    -- (CORREGIDO) Añadida PK. Debe incluir 'anio' (clave de partición).
    CONSTRAINT dm_certificado_pk
        PRIMARY KEY (anio, id_hecho_institucional, secuencia, correlativo, idclasificador_siaf, idmeta)
)
    PARTITION BY LIST (anio);


-------8
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
    id_hecho_institucional BIGINT  NOT NULL,
    certificado            VARCHAR,
    certificado_secuencia  VARCHAR,
    CONSTRAINT dm_expediente_hechos_institucional_consolidados_id_hechos_insti
        FOREIGN KEY (anio, id_hecho_institucional, idclasificador_siaf) REFERENCES sistema_informacion_gerencial.hechos_institucional_consolidados (anio, id_hecho_institucional, idclasificador_siaf),
    -- (CORREGIDO) Añadida PK. Debe incluir 'anio' (clave de partición).
    CONSTRAINT dm_expediente_pk
        PRIMARY KEY (anio, id_hecho_institucional, expediente, secuencia, correlativo, idclasificador_siaf,ciclo,fase)
)
    PARTITION BY LIST (anio);



-------9
-- (NOTA) Esta tabla tiene prefijo 'vw_' (vista) pero está creada como TABLA.
-- La dejo como tabla, ya que así estaba en tu script original.
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


-------10
CREATE TABLE sistema_informacion_gerencial.hechos_rrhh_consolidados
(
    anio                   INTEGER        NOT NULL,
    certificado            VARCHAR        NOT NULL,
    id_planilla            INTEGER        NOT NULL,
    num_planilla           VARCHAR        NOT NULL,
    area_siaf              VARCHAR        NOT NULL,
    cod_tipo_pla           VARCHAR,
    nomb_tipo_pla          VARCHAR,
    cod_tipo_trabajador    INTEGER,
    desc_tipo_trabajador   VARCHAR,
    cod_estado_trabajador  INTEGER,
    desc_estado_trabajador VARCHAR,
    fuente_siaf            VARCHAR        NOT NULL,
    generica_siaf          VARCHAR        NOT NULL,
    idclasificador_siaf    VARCHAR        NOT NULL,
    cantidad_trabajadores  INTEGER,
    monto_certificado      NUMERIC(19, 2),
    monto_expediente       NUMERIC(19, 2),
    id_meta                INTEGER        NOT NULL,
    cod_meta               VARCHAR        NOT NULL,
    CONSTRAINT hechos_rrhh_consolidados_pk
        PRIMARY KEY (id_meta, id_planilla, area_siaf, idclasificador_siaf, fuente_siaf, generica_siaf, certificado,
                     anio)
)
    PARTITION BY LIST (anio);



CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_clasificador
(
    idclasificador_siaf VARCHAR,
    generica            VARCHAR,
    clasificador        VARCHAR,
    descripcion         VARCHAR,
    fts_clasificador    TSVECTOR GENERATED ALWAYS AS (to_tsvector('spanish'::REGCONFIG,
                                                                  ((((COALESCE(descripcion, ''::CHARACTER VARYING))::TEXT || ' '::TEXT) ||
                                                                    (COALESCE(clasificador, ''::CHARACTER VARYING))::TEXT)))) STORED
);


CREATE TABLE IF NOT EXISTS sistema_informacion_gerencial.dm_pim_clasificador
(
    anio                INTEGER,
    fuente_siaf         VARCHAR,
    idclasificador_siaf VARCHAR,
    generica_siaf       VARCHAR,
    monto_pim           NUMERIC(19, 2)
);

----- VISTAS MATERIALIZADAS Y SUS INDICES (CORREGIDOS) -----

CREATE MATERIALIZED VIEW vm_dm_area AS SELECT * FROM sistema_informacion_gerencial.dm_area;
CREATE UNIQUE INDEX idx_vm_dm_area ON sistema_informacion_gerencial.vm_dm_area(area_siaf);

CREATE MATERIALIZED VIEW vm_dm_fuente AS SELECT * FROM sistema_informacion_gerencial.dm_fuente;
CREATE UNIQUE INDEX idx_vm_dm_fuente ON vm_dm_fuente(fuente_siaf);

CREATE MATERIALIZED VIEW vm_dm_generica AS SELECT * FROM sistema_informacion_gerencial.dm_generica;
CREATE UNIQUE INDEX idx_vm_dm_generica ON vm_dm_generica(generica_siaf);

CREATE MATERIALIZED VIEW vm_hechos_institucional_consolidados AS SELECT * FROM sistema_informacion_gerencial.hechos_institucional_consolidados;
-- (CORREGIDO) El índice debe coincidir con la PK de la tabla base.
CREATE UNIQUE INDEX idx_vm_hechos_institucional_consolidados ON vm_hechos_institucional_consolidados(id_hecho_institucional, idclasificador_siaf, anio);

CREATE MATERIALIZED VIEW vm_dm_certificado AS SELECT * FROM sistema_informacion_gerencial.dm_certificado;
-- (CORREGIDO) El índice debe coincidir con la nueva PK de la tabla base (e incluir 'anio')
CREATE UNIQUE INDEX idx_vm_dm_certificado ON vm_dm_certificado(anio, id_hecho_institucional, secuencia, correlativo, idclasificador_siaf, idmeta);

CREATE MATERIALIZED VIEW vm_dm_expediente AS SELECT * FROM sistema_informacion_gerencial.dm_expediente;
-- (CORREGIDO) El índice debe coincidir con la nueva PK de la tabla base (e incluir 'anio')
CREATE UNIQUE INDEX idx_vm_dm_expediente ON vm_dm_expediente(anio, id_hecho_institucional, expediente, secuencia, correlativo, idclasificador_siaf,ciclo,fase);

CREATE MATERIALIZED VIEW vm_hechos_pim AS SELECT * FROM sistema_informacion_gerencial.hechos_pim;
-- (CORREGIDO) El índice debe coincidir con la nueva PK de la tabla base.
CREATE UNIQUE INDEX idx_vm_hechos_pim ON vm_hechos_pim(anio, ejecutora, fuente_siaf, generica_siaf);

CREATE MATERIALIZED VIEW vm_dm_pim AS SELECT * FROM sistema_informacion_gerencial.dm_pim;
-- (CORREGIDO) El índice debe coincidir con la nueva PK de la tabla base.
CREATE UNIQUE INDEX idx_vm_dm_pim ON vm_dm_pim(anio, id_area, fuente_siaf, generica_siaf);

--------------------------------------
create materialized view if not exists sistema_informacion_gerencial.vm_pim_clasificador as
WITH devengado_x_clasificador AS (SELECT de.anio,
                                         hic.fuente_siaf,
                                         de.idclasificador_siaf,
                                         sum(de.monto_nacional) AS monto_devengado
                                  FROM sistema_informacion_gerencial.dm_expediente de
                                           JOIN sistema_informacion_gerencial.hechos_institucional_consolidados hic
                                                ON de.id_hecho_institucional = hic.id_hecho_institucional
                                  WHERE de.ciclo = 'G' AND de.fase = 'D'
                                  GROUP BY de.anio, de.idclasificador_siaf, hic.fuente_siaf),
     pim_x_clasificador AS (SELECT dpc.anio,
                                   dpc.fuente_siaf,
                                   dpc.generica_siaf,
                                   dpc.idclasificador_siaf,
                                   dcla.clasificador,
                                   dcla.fts_clasificador,
                                   dcla.descripcion,
                                   dpc.monto_pim
                            FROM sistema_informacion_gerencial.dm_pim_clasificador dpc
                                     JOIN sistema_informacion_gerencial.dm_clasificador dcla
                                          ON dpc.idclasificador_siaf::text = dcla.idclasificador_siaf::text),
     unioned AS (SELECT pxc.anio,
                        pxc.fuente_siaf,
                        pxc.idclasificador_siaf,
                        pxc.clasificador,
                        pxc.fts_clasificador,
                        pxc.generica_siaf,
                        pxc.descripcion,
                        'SIAF'::text AS origen,
                        pxc.monto_pim,
                        dxc.monto_devengado
                 FROM pim_x_clasificador pxc
                          JOIN devengado_x_clasificador dxc
                               ON pxc.anio = dxc.anio AND pxc.fuente_siaf::text = dxc.fuente_siaf::text AND
                                  pxc.idclasificador_siaf::text = dxc.idclasificador_siaf::text
                 WHERE pxc.monto_pim > 0::numeric
                 UNION ALL
                 SELECT hrc.anio,
                        hrc.fuente_siaf,
                        dc.idclasificador_siaf,
                        dc.clasificador,
                        dc.fts_clasificador,
                        dc.generica               AS generica_siaf,
                        dc.descripcion,
                        'Q20'::text               AS origen,
                        dpc.monto_pim,
                        sum(hrc.monto_expediente) AS monto_devengado
                 FROM sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
                          JOIN sistema_informacion_gerencial.dm_clasificador dc
                               ON hrc.idclasificador_siaf::text = dc.idclasificador_siaf::text
                          JOIN sistema_informacion_gerencial.dm_pim_clasificador dpc
                               ON hrc.anio = dpc.anio AND hrc.fuente_siaf::text = dpc.fuente_siaf::text AND
                                  hrc.idclasificador_siaf::text = dpc.idclasificador_siaf::text
                 GROUP BY hrc.anio, dc.idclasificador_siaf, hrc.fuente_siaf, dc.clasificador, dc.fts_clasificador,
                          dc.generica, dpc.monto_pim, dc.descripcion)
SELECT unioned.anio,
       unioned.fuente_siaf,
       unioned.idclasificador_siaf,
       unioned.clasificador,
       unioned.fts_clasificador,
       unioned.generica_siaf,
       unioned.descripcion,
       'SIAF'::text                                              AS origen,
       unioned.monto_pim,
       jsonb_object_agg(unioned.origen, unioned.monto_devengado) AS devengados_por_origen
FROM unioned
GROUP BY unioned.anio, unioned.fuente_siaf, unioned.idclasificador_siaf, unioned.clasificador, unioned.fts_clasificador,
         unioned.generica_siaf, unioned.descripcion, unioned.monto_pim
ORDER BY unioned.anio, unioned.idclasificador_siaf;

alter materialized view sistema_informacion_gerencial.vm_pim_clasificador owner to postgres;
CREATE UNIQUE INDEX idx_vm_pim_clasificador ON sistema_informacion_gerencial.vm_pim_clasificador(anio,fuente_siaf,idclasificador_siaf,origen);


--------------------------------------------------------

create materialized view if not exists sistema_informacion_gerencial.vm_search_clasificador_area as
SELECT de.anio,
       hic.fuente_siaf,
       hic.area_siaf,
       da.cod_area,
       da.desc_area,
       dcl.generica              AS generica_siaf,
       dcl.clasificador,
       dcl.descripcion           AS desc_clasificador,
       'SIAF'::character varying AS origen,
       sum(de.monto_nacional)    AS monto_devengado
FROM sistema_informacion_gerencial.vm_dm_expediente de
         JOIN sistema_informacion_gerencial.vm_hechos_institucional_consolidados hic
              ON de.id_hecho_institucional = hic.id_hecho_institucional
         JOIN sistema_informacion_gerencial.dm_clasificador dcl
              ON de.idclasificador_siaf::text = dcl.idclasificador_siaf::text
         JOIN sistema_informacion_gerencial.dm_area da ON hic.area_siaf::text = da.area_siaf::text
WHERE da.id_superior = 10468 AND de.ciclo = 'G' AND de.fase = 'D'
GROUP BY de.idclasificador_siaf, de.anio, dcl.descripcion, hic.fuente_siaf, dcl.clasificador, dcl.generica,
         da.id_superior, da.desc_area, da.cod_area, hic.area_siaf
UNION ALL
SELECT de.anio,
       hic.fuente_siaf,
       '0001'::character varying                   AS area_siaf,
       'D65'::bpchar                               AS cod_area,
       'ADMINISTRACION CENTRAL'::character varying AS desc_area,
       dcl.generica                                AS generica_siaf,
       dcl.clasificador,
       dcl.descripcion                             AS desc_clasificador,
       'SIAF'::character varying                   AS origen,
       sum(de.monto_nacional)                      AS monto_devengado
FROM sistema_informacion_gerencial.vm_dm_expediente de
         JOIN sistema_informacion_gerencial.vm_hechos_institucional_consolidados hic
              ON de.id_hecho_institucional = hic.id_hecho_institucional
         JOIN sistema_informacion_gerencial.dm_clasificador dcl
              ON de.idclasificador_siaf::text = dcl.idclasificador_siaf::text
         JOIN sistema_informacion_gerencial.dm_area da ON hic.area_siaf::text = da.area_siaf::text
WHERE (da.id_superior <> 10468
   OR da.id_superior IS NULL) AND de.ciclo = 'G' AND de.fase = 'D'
GROUP BY de.idclasificador_siaf, de.anio, dcl.descripcion, hic.fuente_siaf, dcl.clasificador, dcl.generica
UNION ALL
SELECT hrc.anio,
       hrc.fuente_siaf,
       hrc.area_siaf,
       da.cod_area,
       da.desc_area,
       hrc.generica_siaf,
       dc.clasificador,
       dc.descripcion            AS desc_clasificador,
       'Q20'::character varying  AS origen,
       sum(hrc.monto_expediente) AS monto_devengado
FROM sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
         JOIN sistema_informacion_gerencial.dm_clasificador dc ON hrc.idclasificador_siaf::text = dc.idclasificador_siaf::text
         JOIN sistema_informacion_gerencial.dm_area da ON hrc.area_siaf::text = da.area_siaf::text
WHERE da.id_superior = 10468
GROUP BY dc.clasificador, hrc.area_siaf, da.desc_area, da.cod_area, hrc.anio, hrc.fuente_siaf, hrc.generica_siaf,
         dc.descripcion
UNION ALL
SELECT DISTINCT hrc.anio,
                hrc.fuente_siaf,
                '0001'::text                   AS area_siaf,
                'D65'::text                    AS cod_area,
                'ADMINISTRACION CENTRAL'::text AS desc_area,
                hrc.generica_siaf,
                dc.clasificador,
                dc.descripcion                 AS desc_clasificador,
                'Q20'::text                    AS origen,
                sum(hrc.monto_expediente)      AS monto_devengado
FROM sistema_informacion_gerencial.hechos_rrhh_consolidados hrc
         JOIN sistema_informacion_gerencial.dm_clasificador dc ON hrc.idclasificador_siaf::text = dc.idclasificador_siaf::text
         JOIN sistema_informacion_gerencial.dm_area da ON hrc.area_siaf::text = da.area_siaf::text
WHERE da.id_superior <> 10468
   OR da.id_superior IS NULL
GROUP BY dc.clasificador, hrc.anio, hrc.fuente_siaf, hrc.generica_siaf, dc.descripcion;
alter materialized view sistema_informacion_gerencial.vm_search_clasificador_area owner to postgres;
CREATE UNIQUE INDEX idx_vm_search_clasificador_area ON sistema_informacion_gerencial.vm_search_clasificador_area(anio,fuente_siaf,clasificador,area_siaf,origen);



