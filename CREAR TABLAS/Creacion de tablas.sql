----- creacion del nuevo esquema
create schema if not exists sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;

---- creacion de las tablas


----- creacion del nuevo esquema
create schema if not exists sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;
create table if not exists sistema_informacion_gerencial.dm_area
(
    area_siaf   varchar not null
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
        primary key,
    desc_fuente varchar not null
);

alter table sistema_informacion_gerencial.dm_fuente
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_generica
(
    id_generica   integer not null
        primary key,
    generica_siaf varchar
        constraint dm_generica_cod_generica_key
            unique,
    desc_generica varchar
);

alter table sistema_informacion_gerencial.dm_generica
    owner to postgres;

create table if not exists sistema_informacion_gerencial.hechos_institucional_consolidados
(
    area_siaf       varchar        not null,
    num_certificado varchar        not null,
    anio            integer        not null,
    monto           numeric(19, 2) not null,
    constraint hechos_institucional_consolidados_pk
        primary key (anio, num_certificado)
)
    partition by LIST (anio);

alter table sistema_informacion_gerencial.hechos_institucional_consolidados
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_expediente
(
    anio                  integer not null,
    ejecutora             char(6),
    area_siaf             varchar
        constraint dm_expediente_dm_area_area_siaf_fk
            references sistema_informacion_gerencial.dm_area,
    expediente            varchar not null,
    fase                  varchar,
    secuencia             varchar not null,
    correlativo           varchar not null,
    ciclo                 varchar,
    certificado           varchar,
    certificado_secuencia varchar,
    fecha_autorizacion    date,
    fuente_siaf           varchar
        constraint dm_expediente_dm_fuente_fuente_siaf_fk
            references sistema_informacion_gerencial.dm_fuente,
    clasificador          varchar,
    generica_siaf         varchar
        constraint dm_expediente_dm_generica_generica_siaf_fk
            references sistema_informacion_gerencial.dm_generica (generica_siaf),
    monto_nacional        numeric(19, 2),
    cod_doc               varchar not null,
    num_doc               varchar,
    estado_envio          varchar,
    idclasificador_siaf   varchar not null,
    trimestre             integer,
    constraint dm_expediente_pk
        primary key (anio, expediente, secuencia, correlativo, cod_doc, idclasificador_siaf)
)
    partition by LIST (anio);

alter table sistema_informacion_gerencial.dm_expediente
    owner to postgres;

create table if not exists sistema_informacion_gerencial.hechos_pim
(
    anio          integer not null,
    ejecutora     varchar not null,
    fuente_siaf   varchar not null,
    generica_siaf varchar not null,
    monto_pia     numeric(19, 2),
    monto_pim     numeric(19, 2),
    constraint hechos_pim_pk
        primary key (anio, fuente_siaf, generica_siaf)
);

alter table sistema_informacion_gerencial.hechos_pim
    owner to postgres;

create table if not exists sistema_informacion_gerencial.dm_pim
(
    anio          integer        not null,
    id_fuente     integer        not null,
    fuente_siaf   varchar        not null
        references sistema_informacion_gerencial.dm_fuente,
    id_area       integer,
    area_siaf     varchar,
    id_generica   integer
        references sistema_informacion_gerencial.dm_generica,
    monto_pia     numeric(19, 2) not null,
    monto_pim     numeric(19, 2) not null,
    generica_siaf varchar,
    constraint dm_pim_hechos_pim_anio_fuente_siaf_generica_siaf_fk
        foreign key (anio, fuente_siaf, generica_siaf) references sistema_informacion_gerencial.hechos_pim
);

alter table sistema_informacion_gerencial.dm_pim
    owner to postgres;


create table if not exists sistema_informacion_gerencial.dm_certificado
(
    anio                integer not null,
    num_certificado     varchar not null,
    area_siaf           varchar
        constraint dm_certificado_dm_area_area_siaf_fk
            references sistema_informacion_gerencial.dm_area,
    secuencia           varchar not null,
    ejecutora           varchar,
    monto_clasificador  numeric(19, 2),
    fuente_siaf         varchar
        constraint dm_certificado_dm_fuente_fuente_siaf_fk
            references sistema_informacion_gerencial.dm_fuente,
    glosa               varchar,
    correlativo         varchar not null,
    idclasificador_siaf varchar not null,
    clasificador        varchar,
    generica_siaf       varchar
        constraint dm_certificado_dm_generica_generica_siaf_fk
            references sistema_informacion_gerencial.dm_generica (generica_siaf),
    cod_doc             varchar,
    num_doc             varchar,
    estado_envio        varchar,
    estado_registro     varchar,
    fecha_creacion_clt  date,
    idmeta              varchar not null,
    codmeta             varchar,
    nomb_met_ins        varchar,
    constraint dm_certificado_pk
        primary key (anio, num_certificado, secuencia, correlativo, idmeta, idclasificador_siaf),
    constraint dm_certificado_hechos_institucional_consolidados_num_certificad
        foreign key (num_certificado, anio) references sistema_informacion_gerencial.hechos_institucional_consolidados (num_certificado, anio)
)
    partition by LIST (anio);

alter table sistema_informacion_gerencial.dm_certificado
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
