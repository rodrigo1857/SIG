----- creacion del nuevo esquema
create schema if not exists sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;

---- creacion de las tablas

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

alter table sistema_informacion_gerencial.dm_area
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_fuente
(
    fuente_siaf varchar not null
        constraint dm_fuente_pk
            primary key,
    desc_fuente varchar not null
);

alter table sistema_informacion_gerencial.dm_fuente
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_generica
(
    id_generica   integer not null,
    generica_siaf varchar not null
        constraint dm_generica_pk
            primary key,
    desc_generica varchar
);

alter table sistema_informacion_gerencial.dm_generica
    owner to postgres;

create table if not exists sistema_informacion_gerencial.hechos_institucional_consolidados
(
    area_siaf              varchar        not null
        constraint hechos_institucional_consolidados_dm_area_area_siaf_fk
            references sistema_informacion_gerencial.dm_area,
    num_certificado        varchar        not null,
    anio                   integer        not null,
    monto_certificado      numeric(19, 2) not null,
    id_hecho_institucional bigint         not null,
    monto_expediente       numeric(19, 2),
    fuente_siaf            varchar        not null
        constraint hechos_institucional_consolidados_dm_fuente_fuente_siaf_fk
            references sistema_informacion_gerencial.dm_fuente,
    generica_siaf          varchar        not null
        constraint hechos_institucional_consolidados_dm_generica_generica_siaf_fk
            references sistema_informacion_gerencial.dm_generica,
    constraint hechos_institucional_consolidados_pk
        primary key (id_hecho_institucional, anio)
)
    partition by LIST (anio);

alter table sistema_informacion_gerencial.hechos_institucional_consolidados
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_pim
(
    anio          integer        not null,
    id_fuente     integer        not null,
    fuente_siaf   varchar        not null,
    id_area       integer,
    area_siaf     varchar,
    id_generica   integer,
    monto_pia     numeric(19, 2) not null,
    monto_pim     numeric(19, 2) not null,
    generica_siaf varchar
);

alter table sistema_informacion_gerencial.dm_pim
    owner to postgres;

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

alter table sistema_informacion_gerencial.hechos_pim
    owner to postgres;


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

alter table sistema_informacion_gerencial.dm_certificado
    owner to postgres;

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

alter table sistema_informacion_gerencial.dm_expediente
    owner to postgres;

-------8
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

alter table sistema_informacion_gerencial.vw_obras_materializada
    owner to postgres;
-------9
create table if not exists sistema_informacion_gerencial.hechos_rrhh_consolidados
(
    tipo            varchar        not null,
    trabajador      varchar        not null,
    cantidad        integer        not null,
    monto           numeric(19, 2) not null,
    fecha           date           not null,
    mes             varchar        not null,
    trimestre       varchar        not null,
    generica        varchar        not null,
    clasificador    varchar        not null,
    tipo_subvencion varchar        not null
);

alter table sistema_informacion_gerencial.hechos_rrhh_consolidados
    owner to postgres;
