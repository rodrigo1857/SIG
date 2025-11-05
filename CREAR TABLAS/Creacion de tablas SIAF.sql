create table bytsscom_bytsiaf.sig_certificado
(
    ano_eje          char(4)  not null,
    sec_ejec         char(6),
    certificado      char(10) not null,
    tipo_certificado char,
    estado_registro  char,
    cod_error        char(2),
    cod_mensa        char(4),
    estado_envio     char,
    tipo_operacion   char(2),
    constraint sig_certificado_pk
        primary key (ano_eje, certificado)
);

alter table bytsscom_bytsiaf.sig_certificado
    owner to bytsscom_bytsiaf;

create table bytsscom_bytsiaf.sig_certificado_fase
(
    ano_eje              char(4)  not null,
    sec_ejec             char(6),
    certificado          char(10) not null,
    secuencia            char(4)  not null,
    secuencia_padre      char(4),
    fuente_financ        char(4),
    etapa                char,
    tipo_id              char,
    ruc                  char(11),
    es_compromiso        char,
    monto                numeric(19, 2),
    monto_comprometido   numeric(19, 2),
    monto_nacional       numeric(19, 2),
    glosa                varchar(250),
    estado_registro      char,
    cod_error            char(2),
    cod_mensa            char(4),
    estado_envio         char,
    saldo_nacional       numeric(19, 2),
    ind_anulacion        char,
    tipo_financiamiento  char,
    tipo_operacion       char(2),
    sec_area             char(4),
    sys_id_certificacion integer,
    sys_estado           char,
    sys_last_correlativo char(4),
    sys_monto_neto       numeric(19, 2),
    constraint sig_certificado_fase_pk
        primary key (ano_eje, certificado, secuencia)
);

alter table bytsscom_bytsiaf.sig_certificado_fase
    owner to bytsscom_bytsiaf;

create index sig_certificado_fase_idx01
    on bytsscom_bytsiaf.sig_certificado_fase (ano_eje, certificado);

create table bytsscom_bytsiaf.sig_certificado_meta
(
    ano_eje               char(4)  not null,
    sec_ejec              char(6),
    certificado           char(10) not null,
    secuencia             char(4)  not null,
    correlativo           char(4)  not null,
    id_clasificador       char(7)  not null,
    sec_func              char(4)  not null,
    monto                 numeric(19, 2),
    monto_comprometido    numeric(19, 2),
    monto_nacional        numeric(19, 2),
    estado_registro       char,
    cod_error             char(2),
    cod_mensa             char(4),
    estado_envio          char,
    monto_nacional_ajuste numeric(19, 2),
    sys_cod_clasif        varchar(20),
    sys_id_clasificador   integer,
    constraint sig_certificado_meta_pk
        primary key (ano_eje, certificado, secuencia, correlativo, id_clasificador, sec_func)
);

alter table bytsscom_bytsiaf.sig_certificado_meta
    owner to bytsscom_bytsiaf;

create index sig_certificado_meta_idx01
    on bytsscom_bytsiaf.sig_certificado_meta (ano_eje, certificado);

create table bytsscom_bytsiaf.sig_certificado_secuencia
(
    ano_eje                char(4)  not null,
    sec_ejec               char(6),
    certificado            char(10) not null,
    secuencia              char(4)  not null,
    correlativo            char(4)  not null,
    cod_doc                char(3),
    num_doc                char(20),
    fecha_doc              date,
    estado_registro        char,
    estado_envio           char,
    ind_certificacion      char,
    estado_registro2       char,
    estado_envio2          char,
    monto                  numeric(19, 2),
    monto_comprometido     numeric(19, 2),
    monto_nacional         numeric(19, 2),
    moneda                 char(6),
    tipo_cambio            numeric(19, 15),
    cod_error              char(2),
    cod_mensa              char(4),
    tipo_registro          char,
    fecha_bd_oracle        date,
    estado_ctb             char,
    secuencia_solicitud    char(10),
    fecha_creacion_clt     date,
    fecha_modificacion_clt date,
    flg_interfase          char(20),
    constraint sig_certificado_secuencia_pk
        primary key (ano_eje, certificado, secuencia, correlativo)
);

alter table bytsscom_bytsiaf.sig_certificado_secuencia
    owner to bytsscom_bytsiaf;

create index sig_certificado_secuencia_idx01
    on bytsscom_bytsiaf.sig_certificado_secuencia (ano_eje, certificado);

create table bytsscom_bytsiaf.sig_especifica_detalle
(
    id_clasificador     char(7) not null,
    ano_eje             varchar not null,
    tipo_transaccion    char,
    generica            char,
    subgenerica         char(2),
    subgenerica_det     char(2),
    especifica          char(2),
    especifica_det      char(2),
    descripcion         varchar(250),
    ambito              char,
    estado              char,
    exclusivo_tp        char,
    sys_cod_clasif      varchar(20),
    sys_id_clasificador integer,
    constraint sig_especifica_detalle_pk
        primary key (id_clasificador, ano_eje)
);

alter table bytsscom_bytsiaf.especifica_detalle
    owner to bytsscom_bytsiaf;



create table bytsscom_bytsiaf.sig_expediente
(
    ano_eje                   char(4)  not null,
    sec_ejec                  char(6),
    expediente                char(10) not null,
    mes_eje                   char(2),
    cod_doc                   char(3),
    num_doc                   char(20),
    fecha_doc                 date,
    fecha_ing                 date,
    usuario_ing               char(15),
    fecha_mod                 date,
    usuario_mod               char(15),
    tipo_operacion            char(2),
    sec_ejec2                 char(6),
    modalidad_compra          char(2),
    clase_menor_cuantia       char(2),
    sec_area                  char(4),
    flag_encargo              char,
    expediente_encargante     char(10),
    cod_mensa                 char(4),
    estado                    char,
    estado_envio              char,
    archivo                   char(40),
    tipo_proceso              char(2),
    id_proceso                char(8),
    id_contrato               char(8),
    sec_ejec_contrato         char(6),
    fase_contractual          char,
    procedencia               numeric,
    expediente_financiamiento char(10),
    constraint sig_expediente_pk
        primary key (ano_eje, expediente)
);

alter table bytsscom_bytsiaf.sig_expediente
    owner to bytsscom_bytsiaf;

create table bytsscom_bytsiaf.sig_expediente_fase
(
    ano_eje                     char(4)  not null,
    sec_ejec                    char(6),
    expediente                  char(10) not null,
    ciclo                       char     not null,
    fase                        char     not null,
    secuencia                   char(4)  not null,
    secuencia_padre             char(4),
    secuencia_anterior          char(4),
    mes_ctb                     char(2),
    monto_nacional              numeric(19, 2),
    monto_saldo                 numeric(19, 2),
    origen                      char,
    fuente_financ               char(2),
    mejor_fecha                 date,
    tipo_id                     char,
    ruc                         char(11),
    tipo_pago                   char,
    tipo_recurso                char(2),
    tipo_compromiso             char(2),
    organismo                   char(3),
    proyecto                    char(3),
    estado                      char,
    estado_envio                char,
    archivo                     char(40),
    tipo_giro                   char,
    tipo_financiamiento         char,
    cod_doc_ref                 char(3),
    fecha_doc_ref               date,
    num_doc_ref                 char(20),
    certificado                 char(10),
    certificado_secuencia       char(4),
    sec_ejec_ruc                char(6),
    sys_id_registro             integer,
    sys_id_corr_fase            integer,
    sys_estado                  char,
    sys_last_correlativo        char(4),
    sys_monto_neto              numeric(19, 2),
    sec_ejec_reciproca          char(6),
    transferencia_financiera_id char(10),
    ceam_oce_det_id             char(10),
    constraint sig_expediente_fase_pk
        primary key (ano_eje, expediente, ciclo, secuencia, fase)
);

alter table bytsscom_bytsiaf.sig_expediente_fase
    owner to bytsscom_bytsiaf;

create index sig_expediente_fase_idx01
    on bytsscom_bytsiaf.sig_expediente_fase (ano_eje, expediente);

create table bytsscom_bytsiaf.sig_expediente_meta
(
    ano_eje             char(4)  not null,
    sec_ejec            char(6),
    expediente          char(10) not null,
    ciclo               char     not null,
    fase                char     not null,
    secuencia           char(4)  not null,
    correlativo         char(4)  not null,
    categ_gasto         char,
    grupo_gasto         char,
    modalidad_gasto     char(2),
    elemento_gasto      char(2),
    sec_func            char(4)  not null,
    monto               numeric(19, 2),
    monto_saldo         numeric(19, 2),
    monto_nacional      numeric(19, 2),
    ind_proceso         char,
    edicion             char,
    estado              char,
    estado_envio        char,
    archivo             char(40),
    id_clasificador     char(7)  not null,
    sys_cod_clasif      varchar(20),
    sys_id_clasificador integer,
    primary key (ano_eje, expediente, ciclo, secuencia, fase, correlativo, sec_func, id_clasificador)
);

alter table bytsscom_bytsiaf.sig_expediente_meta
    owner to bytsscom_bytsiaf;

create index sig_expediente_meta_idx01
    on bytsscom_bytsiaf.sig_expediente_meta (ano_eje, expediente);

create table bytsscom_bytsiaf.sig_expediente_secuencia
(
    ano_eje                   char(4)  not null,
    sec_ejec                  char(6),
    expediente                char(10) not null,
    ciclo                     char     not null,
    fase                      char     not null,
    secuencia                 char(4)  not null,
    correlativo               char(4)  not null,
    cod_doc                   char(3),
    num_doc                   char(30),
    fecha_doc                 date,
    moneda                    char(5),
    tipo_cambio               numeric(19, 15),
    monto                     numeric(19, 2),
    monto_saldo               numeric(19, 2),
    monto_nacional            numeric(19, 2),
    monto_extranjero          numeric(19, 2),
    fecha_ing                 date,
    usuario_ing               char(15),
    fecha_mod                 date,
    usuario_mod               char(15),
    num_record                numeric,
    serie_doc                 char(4),
    ano_proceso               char(4),
    mes_proceso               char(2),
    dia_proceso               char(2),
    grupo                     char(10),
    edicion                   char,
    ano_cta_cte               char(4),
    banco                     char(3),
    cta_cte                   char(3),
    fecha_autorizacion        date,
    cod_mensa                 char(4),
    estado_ctb                char,
    estado_ctb_anterior       char,
    estado                    char,
    estado_anterior           char,
    estado_envio              char,
    archivo                   char(40),
    reg_multiple              char(10),
    cta_bco_ejec              char(20),
    flg_interfase             char,
    ind_contabiliza           char,
    tipo_cambio_ps            numeric(19, 15),
    sec_proceso               char(10),
    cod_doc_b                 char(3),
    fecha_doc_b               date,
    num_doc_b                 char(20),
    fecha_bd_oracle           date,
    mes_afectacion_calendario char(2),
    secuencia_solicitud       char(10),
    fecha_creacion_clt        date,
    fecha_modificacion_clt    date,
    usuario_creacion_clt      char(20),
    usuario_modificacion_clt  char(20),
    fecha_autorizacion_giro   date,
    verifica_1                char(250),
    constraint sig_expediente_secuencia_pk
        primary key (ano_eje, expediente, ciclo, secuencia, correlativo, fase)
);

alter table bytsscom_bytsiaf.sig_expediente_secuencia
    owner to bytsscom_bytsiaf;

create index sig_expediente_secuencia_idx01
    on bytsscom_bytsiaf.sig_expediente_secuencia (ano_eje, expediente);

create table bytsscom_bytsiaf.sig_mpp_pca_x_especifica
(
    ano_eje             char(4),
    sec_ejec            char(6),
    fuente_financ       char(2),
    categoria_gasto     char,
    tipo_transaccion    char,
    generica            char,
    id_clasificador     char(7),
    monto_asignado      numeric(19, 2),
    monto_comprometido  numeric(19, 2),
    monto_a_solicitado  numeric(19, 2),
    monto_de_solicitado numeric(19, 2)
);

alter table bytsscom_bytsiaf.sig_mpp_pca_x_especifica
    owner to bytsscom_bytsiaf;

