----- creacion del nuevo esquema
create schema if not exists sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;

---- creacion de las tablas
-------1
create table if not exists sistema_informacion_gerencial.dm_area
(
    area_siaf   varchar not null
        constraint dm_area_pk
            primary key,
    cod_area    char(6) not null,
    desc_area   varchar not null,
    nivel       integer,
    id_area     integer,
    id_superior integer
);


-------2
create table if not exists sistema_informacion_gerencial.dm_fuente
(
    fuente_siaf varchar not null
        constraint dm_fuente_pk
            primary key,
    desc_fuente varchar not null
);

-------3
create table if not exists sistema_informacion_gerencial.dm_generica
(
    id_generica   integer not null,
    generica_siaf varchar not null
        constraint dm_generica_pk
            primary key,
    desc_generica varchar
);

-------4
create table sistema_informacion_gerencial.hechos_institucional_consolidados
(
    area_siaf                varchar        not null
        constraint hechos_institucional_consolidados_dm_area_area_siaf_fk
            references sistema_informacion_gerencial.dm_area,
    num_certificado          varchar        not null,
    anio                     integer        not null,
    monto_certificado        numeric(19, 2) not null,
    id_hecho_institucional   bigint         not null,
    monto_devengado          numeric(19, 2),
    fuente_siaf              varchar        not null
        constraint hechos_institucional_consolidados_dm_fuente_fuente_siaf_fk
            references sistema_informacion_gerencial.dm_fuente,
    generica_siaf            varchar        not null
        constraint hechos_institucional_consolidados_dm_generica_generica_siaf_fk
            references sistema_informacion_gerencial.dm_generica,
    idclasificador_siaf      varchar,
    clasificador_siaf        varchar,
    monto_compromiso_anual   numeric(19, 2),
    monto_compromiso_mensual numeric(19, 2),
    monto_girado             numeric(19, 2),
    constraint hechos_institucional_consolidados_pk
        primary key (id_hecho_institucional, anio)
)
partition by LIST (anio);


-------5
create table if not exists sistema_informacion_gerencial.dm_pim
(
    anio          integer        not null,
    fuente_siaf   varchar        not null,
    id_area       integer,
    area_siaf     varchar,
    id_generica   integer,
    monto_pia     numeric(19, 2) not null,
    monto_pim     numeric(19, 2) not null,
    generica_siaf varchar
);


-------6
create table if not exists sistema_informacion_gerencial.hechos_pim
(
    anio          integer not null,
    ejecutora     varchar not null,
    fuente_siaf   varchar not null,
    generica_siaf varchar not null,
    monto_pia     numeric(19, 2),
    monto_pim     numeric(19, 2)
)
    partition by LIST (anio);



-------7
create table if not exists sistema_informacion_gerencial.dm_certificado
(
    id_hecho_institucional bigint,
    anio                   integer not null,
    num_certificado        varchar not null,
    area_siaf              varchar,
    secuencia              varchar not null,
    ejecutora              varchar,
    monto_clasificador     numeric(19, 2),
    fuente_siaf            varchar,
    glosa                  varchar,
    correlativo            varchar not null,
    idclasificador_siaf    varchar not null,
    clasificador           varchar,
    generica_siaf          varchar,
    cod_doc                varchar,
    num_doc                varchar,
    estado_envio           varchar,
    estado_registro        varchar,
    fecha_creacion_clt     date,
    idmeta                 varchar not null,
    codmeta                varchar,
    nomb_met_ins           varchar,
    constraint dm_certificado_hechos_institucional_consolidados_anio_id_hechos
        foreign key (anio, id_hecho_institucional) references sistema_informacion_gerencial.hechos_institucional_consolidados (anio,id_hecho_institucional)
)
    partition by LIST (anio);


-------8
create table if not exists sistema_informacion_gerencial.dm_expediente
(
    anio                   integer not null,
    ejecutora              char(6),
    expediente             varchar not null,
    fase                   varchar,
    secuencia              varchar not null,
    correlativo            varchar not null,
    ciclo                  varchar,
    fecha_autorizacion     date,
    clasificador           varchar,
    monto_nacional         numeric(19, 2),
    cod_doc                varchar not null,
    num_doc                varchar,
    estado_envio           varchar,
    idclasificador_siaf    varchar not null,
    trimestre              integer,
    id_hecho_institucional bigint  not null,
    certificado            varchar,
    certificado_secuencia  varchar,
    constraint dm_expediente_hechos_institucional_consolidados_id_hechos_insti
        foreign key (anio, id_hecho_institucional) references sistema_informacion_gerencial.hechos_institucional_consolidados (anio, id_hecho_institucional)

)
    partition by LIST (anio);



-------9
create table if not exists sistema_informacion_gerencial.vw_obras_materializada
(
    id_area_usuaria   integer,
    area_description  varchar,
    id_item           integer,
    desc_item         varchar,
    id_fecha          integer,
    id_obra           integer,
    nomb_obra         varchar,
    cui               varchar,
    cantidad          varchar,
    meta              varchar,
    monto             numeric(19, 2),
    num_requerimiento varchar,
    num_hoja_ruta     varchar,
    num_oc            varchar,
    num_siaf          varchar,
    num_certificado   varchar,
    monto_certificado numeric(19, 2),
    estado            varchar,
    oficina           varchar
);


-------10
create table sistema_informacion_gerencial.hechos_rrhh_consolidados
(
    anio                   integer not null,
    certificado            varchar not null,
    id_planilla            integer not null,
    num_planilla           varchar not null,
    area_siaf              varchar not null,
    cod_tipo_pla           varchar,
    nomb_tipo_pla          varchar,
    cod_tipo_trabajador    integer,
    desc_tipo_trabajador   varchar,
    cod_estado_trabajador  integer,
    desc_estado_trabajador varchar,
    fuente_siaf            varchar not null,
    generica_siaf          varchar not null,
    idclasificador_siaf    varchar not null,
    cantidad_trabajadores  integer,
    monto_certificado      numeric(19, 2),
    monto_expediente       numeric(19, 2),
    id_meta                integer not null,
    cod_meta               varchar not null,
    constraint hechos_rrhh_consolidados_pk
        primary key (id_meta, id_planilla, area_siaf, idclasificador_siaf, fuente_siaf, generica_siaf, certificado,
                     anio)
)
    partition by LIST (anio);



create table if not exists sistema_informacion_gerencial.dm_clasificador
(
    idclasificador_siaf varchar,
    generica            varchar,
    clasificador        varchar,
    descripcion         varchar,
    fts_clasificador    tsvector generated always as (to_tsvector('spanish'::regconfig,
                                                                  (((COALESCE(descripcion, ''::character varying))::text || ' '::text) ||
                                                                   (COALESCE(clasificador, ''::character varying))::text))) stored
);





create table if not exists sistema_informacion_gerencial.dm_pim_clasificador
(
    anio                integer,
    fuente_siaf         varchar,
    idclasificador_siaf varchar,
    generica_siaf       varchar,
    monto_pim           numeric(19, 2)
);



-----
CREATE MATERIALIZED VIEW vm_dm_area AS SELECT * FROM sistema_informacion_gerencial.dm_area;
CREATE UNIQUE INDEX idx_vm_dm_area ON sistema_informacion_gerencial.vm_dm_area(area_siaf);

CREATE MATERIALIZED VIEW vm_dm_fuente AS SELECT * FROM sistema_informacion_gerencial.dm_fuente;
CREATE UNIQUE INDEX idx_vm_dm_fuente ON vm_dm_fuente(fuente_siaf);

CREATE MATERIALIZED VIEW vm_dm_generica AS SELECT * FROM sistema_informacion_gerencial.dm_generica;
CREATE UNIQUE INDEX idx_vm_dm_generica ON vm_dm_generica(generica_siaf);

CREATE MATERIALIZED VIEW vm_hechos_institucional_consolidados AS SELECT * FROM sistema_informacion_gerencial.hechos_institucional_consolidados;
CREATE UNIQUE INDEX idx_vm_hechos_institucional_consolidados ON vm_hechos_institucional_consolidados(id_hecho_institucional);

CREATE MATERIALIZED VIEW vm_dm_certificado AS SELECT * FROM sistema_informacion_gerencial.dm_certificado;
CREATE UNIQUE INDEX idx_vm_dm_certificado ON vm_dm_certificado(id_hecho_institucional,secuencia,correlativo,idclasificador_siaf,idmeta);

CREATE MATERIALIZED VIEW vm_dm_expediente AS SELECT * FROM sistema_informacion_gerencial.dm_expediente;
CREATE UNIQUE INDEX idx_vm_dm_expediente ON vm_dm_expediente(id_hecho_institucional,expediente,secuencia,correlativo,idclasificador_siaf);

CREATE MATERIALIZED VIEW vm_hechos_pim AS SELECT * FROM sistema_informacion_gerencial.hechos_pim;
CREATE UNIQUE INDEX idx_vm_hechos_pim ON vm_hechos_pim(anio,fuente_siaf,generica_siaf);

CREATE MATERIALIZED VIEW vm_dm_pim AS SELECT * FROM sistema_informacion_gerencial.dm_pim;
CREATE UNIQUE INDEX idx_vm_dm_pim ON vm_dm_pim(anio,id_area,fuente_siaf,generica_siaf);

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



