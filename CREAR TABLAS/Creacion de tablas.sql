----- creacion del nuevo esquema
create schema if not exists sistema_informacion_gerencial;

--- ubicacion del esquema
SET search_path TO sistema_informacion_gerencial;

---- creacion de las tablas

-------1--------
create table if not exists sistema_informacion_gerencial.dm_area
(
    cod_siaf_area     varchar not null
        constraint dm_area_pk
            primary key,
    area_name         char(6) not null,
    area_display_name varchar not null,
    nivel             integer,
    id_area           integer,
    idsuperior        integer
);

alter table sistema_informacion_gerencial.dm_area
    owner to postgres;


----------2
    
create table if not exists sistema_informacion_gerencial.dm_fuente
(
    fuente_siaf varchar not null
        constraint dm_fuente_pk
            primary key,
    desc_fuente varchar not null,
);

alter table sistema_informacion_gerencial.dm_fuente
    owner to postgres;



-------3
create table if not exists sistema_informacion_gerencial.dm_generica
(
    id_generica   integer not null
        constraint dm_generica_pk
            primary key,
    cod_generica  varchar,
    nomb_generica varchar
);

alter table sistema_informacion_gerencial.dm_generica
    owner to postgres;

-------4

create table if not exists sistema_informacion_gerencial.hechos_institucional_consolidados
(
    cod_siaf_area   varchar        not null
        constraint hechos_institucional_consolidados_dm_area_cod_siaf_area_fk
            references sistema_informacion_gerencial.dm_area,
    num_certificado varchar        not null,
    anio            integer        not null,
    monto           numeric(19, 2) not null,
    constraint hechos_institucional_consolidados_pk
        unique (num_certificado, anio)
);

alter table sistema_informacion_gerencial.hechos_institucional_consolidados
    owner to postgres;



-------5
create table if not exists sistema_informacion_gerencial.dm_certificado
(
    ano_eje              integer,
    num_certificado      varchar,
    cod_siaf_area        varchar,
    secuencia            varchar,
    sec_ejec             varchar,
    monto_clasificador   numeric(19, 2),
    siaf_id_fuente       varchar,
    glosa                varchar,
    correlativo          varchar,
    siaf_id_clasificador varchar,
    clasificador         varchar,
    generica             varchar,
    cod_doc              varchar,
    num_doc              varchar,
    estado_envio         varchar,
    estado_registro      varchar,
    fecha_creacion_clt   date,
    idmeta               varchar,
    codmeta              varchar,
    nomb_met_ins         varchar,
    constraint dm_certificado_hechos_institucional_consolidados_anio_num_certi
        foreign key (ano_eje, num_certificado) references sistema_informacion_gerencial.hechos_institucional_consolidados(anio, num_certificado)
);

alter table sistema_informacion_gerencial.dm_certificado
    owner to postgres;

-------6
create table if not exists sistema_informacion_gerencial.dm_expediente
(
    ano_eje               integer not null,
    sec_ejec              char(6),
    area_siaf             varchar,
    expediente            varchar,
    fase                  varchar,
    secuencia             varchar,
    certificado_secuencia varchar,
    correlativo           varchar,
    ciclo                 varchar,
    certificado           varchar,
    fecha_autorizacion    date,
    fuente_siaf           varchar,
    clasificador          varchar,
    generica              varchar,
    monto_nacional        numeric(19, 2),
    cod_doc               varchar,
    num_doc               varchar,
    estado_envio          varchar,
    siaf_id_clasificador  varchar
);

alter table sistema_informacion_gerencial.dm_expediente
    owner to postgres;

-------7
create table if not exists sistema_informacion_gerencial.hechos_pim
(
    id_area        integer,
    cod_siaf_area  varchar,
    id_fuente      integer        not null,
    monto_pia      numeric(19, 2) not null,
    monto_pim      numeric(19, 2) not null,
    id_periodo_pla integer        not null,
    generica       varchar,
    id_generica    integer
        constraint hechos_pim_dm_generica_id_generica_fk
            references sistema_informacion_gerencial.dm_generica,
    fuente_siaf    varchar
);

alter table sistema_informacion_gerencial.hechos_pim
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
