
-- dm_area

---------


select
    id_area,
    area_name,
    cod_siaf_area,
    area_display_name,
    case id_parent_area when 10468 then 0
        else 1 end as is_central
from bytsscom_bytcore.area
where length(area_name) = 3 and cod_siaf_area not in ('0000', '0001')
union
select
    id_area,
    area_name,
    cod_siaf_area,
    area_display_name,
    case id_parent_area when 10468 then 0
        else 1 end as is_central
from bytsscom_bytcore.area
where id_parent_area = 10030 and length(area_name) = 5
and id_area in (10383, 10390, 10395, 10385)
union
select
    0 as id_area,
    '000' as area_name,
    '0000' as cod_siaf_area,
    'UNMSM' as area_display_name
    , 2 as is_central
union
select
    11327 as id_area,
    'D65' as area_name,
    '0001' as cod_siaf_area,
    'ADMINISTRACION CENTRAL' as area_display_name
    , 2 as is_central;


-- dm_fuente

SELECT
    siaf_codigo as fuente_siaf,
    desc_fuente
    FROM bytsscom_bytcore.fuente
    where id_fuente<4
   order by siaf_codigo;

--- hechos_consolidados

WITH certificado_anio_area AS(
     SELECT DISTINCT
        c.siaf_certificado,
        cert.ano_eje,
        a.cod_siaf_area
    FROM bytsscom_bytsig.vw_certificacion c
             INNER JOIN bytsscom_bytsig.memo_requerimiento m
                        ON c.id_memo_requerimiento = m.id_memo_requerimiento
             INNER JOIN bytsscom_bytsiaf.certificado cert
                        ON c.id_anio::varchar = cert.ano_eje
                            AND c.siaf_certificado = cert.certificado
             INNER JOIn bytsscom_bytcore.area a
                        ON a.id_area = m.id_area
    WHERE c.esta_cert = 'A'
)
SELECT
    cf.ano_eje as anio,
    certificado as num_certificado,
    coalesce( caa.cod_siaf_area,'0000') as cod_siaf_area,
    fuente_financ as fuente_siaf,
    monto
    FROM bytsscom_bytsiaf.certificado_fase  cf
    left join certificado_anio_area caa
        on caa.siaf_certificado = cf.certificado and caa.ano_eje = cf.ano_eje
    WHERE es_compromiso = 'N' and estado_registro = 'A' and secuencia = '0001'
group by cf.ano_eje, certificado, monto,fuente_financ,caa.cod_siaf_area;

----- dm_certificado

WITH certificado_anio_area AS(
    SELECT DISTINCT
        c.siaf_certificado,
        cert.ano_eje,
        a.cod_siaf_area
    FROM bytsscom_bytsig.vw_certificacion c
             INNER JOIN bytsscom_bytsig.memo_requerimiento m
                        ON c.id_memo_requerimiento = m.id_memo_requerimiento
             INNER JOIN bytsscom_bytsiaf.certificado cert
                        ON c.id_anio::varchar = cert.ano_eje
                            AND c.siaf_certificado = cert.certificado
             INNER JOIn bytsscom_bytcore.area a
                        ON a.id_area = m.id_area
    WHERE c.esta_cert = 'A'
)
SELECT
    cert.ano_eje as ano_eje,
    cert.certificado as num_certificado,
    cf.secuencia,
    f.siaf_codigo as siaf_id_fuente,
    f.abre_fuente as fuente,
    cs.cod_doc,
    cs.num_doc,
    cla.codigo_siaf as siaf_id_clasificador,
    cla.cod_clasif clasificador,
    substr(cla.cod_clasif,0,3) as generica,
    mis.id_meta_institucional as idmeta,
    mis.sec_func as codmeta,
    mp.nomb_met_ins,
    COALESCE(areas.cod_siaf_area,'0000') as cod_siaf_area,
    sum(cs.monto_nacional) as monto_nacional,
    sum(cm.monto_nacional) as monto_clasificador
FROM bytsscom_bytsiaf.certificado cert
         INNER JOIN bytsscom_bytsiaf.certificado_fase cf
                    ON cert.certificado = cf.certificado
                        AND cert.ano_eje = cf.ano_eje
         INNER JOIN bytsscom_bytcore.fuente f
                    ON cf.fuente_financ = f.siaf_codigo
         INNER JOIN bytsscom_bytsiaf.certificado_secuencia cs
                    ON cert.certificado = cs.certificado
                        AND cert.ano_eje = cs.ano_eje
                        AND cf.secuencia = cs.secuencia
                        AND cs.estado_registro = 'A'
         INNER JOIN bytsscom_bytsiaf.certificado_meta cm
                    ON cf.certificado = cm.certificado
                        AND cf.ano_eje = cm.ano_eje
                        AND cf.secuencia = cm.secuencia
         INNER JOIN bytsscom_bytcore.clasificador cla
                    ON cla.codigo_siaf = cm.id_clasificador
         LEFT JOIN bytsscom_bytcore.meta_institucional_siaf mis
                   ON cm.sec_func = mis.sec_func
                       AND cm.ano_eje =  mis.id_anio::varchar
         INNER JOIN bytsscom_bytcore.meta_institucional mp
                    ON mis.id_meta_institucional = mp.id_meta_institucional
         LEFT JOIN certificado_anio_area areas
                   ON cf.certificado = areas.siaf_certificado AND areas.ano_eje = cf.ano_eje
GROUP BY cert.ano_eje,
         cert.certificado,
         cf.secuencia,
         f.siaf_codigo,
         f.abre_fuente,
         cs.cod_doc,
         cs.num_doc,
         cla.codigo_siaf,
         cla.cod_clasif,
         substr(cla.cod_clasif,0,3),
         mis.id_meta_institucional,
         mis.sec_func,
         mp.nomb_met_ins,
         areas.cod_siaf_area