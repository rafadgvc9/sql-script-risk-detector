USE DATABASE DB_SIL_{{ environment }};
USE SCHEMA MDM_CORPORATIVO;

CREATE OR REPLACE PROCEDURE SP_LOAD_DELTA_TB_CATEGORIES_COMPLETE_HIST(config VARIANT)
RETURNS VARIANT NOT NULL
LANGUAGE SQL
AS

$$
 
BEGIN
TRUNCATE IF EXISTS TB_CATEGORIES_COMPLETE_HIST;

INSERT INTO TB_CATEGORIES_COMPLETE_HIST 

    SELECT DISTINCT
    CO_IDENTIFIER AS CO_CATEGORIA,
    CO_SUBSITE,
    CO_TYPE,
    DS_NAME AS DS_NOMBRE,
    TS_EVENTO,
    DS_INFO_COMPLETE:business_object:metadata:is_publish::boolean AS IS_PUBLICADO,
    DS_INFO_COMPLETE:business_object:parent_categories[0]:identifier::string AS CO_CATEGORIA_PADRE,
    DS_INFO_COMPLETE:business_object:parent_categories[0]:order::numeric AS NU_ORDEN_CATEGORIA_HIJO,
    ARRAY_DISTINCT(
        ARRAY_APPEND(
            COALESCE(
                TRANSFORM(
                    COALESCE(
                        FILTER(
                            DS_INFO_COMPLETE:business_object:previous_names,
                            I -> I:locale::string = 'es_ES'
                        )[0],
                        DS_INFO_COMPLETE:business_object:previous_names[0]
                    ):names,
                    I -> I:name
                ),
                []::variant
            ),
            DS_NOMBRE
        )
    ) AS VT_NOMBRES_ANTERIORES,
    DS_INFO_COMPLETE:business_object:classifications AS VT_CLASSIFICATIONS,
    ARRAY_CONTAINS(
        'SaleHierarchy'::variant,
        DS_INFO_COMPLETE:business_object:classifications
    ) AS IS_SALE_HIERARCHY,
    CO_EVENT_TYPE as DS_EVENT_TYPE,
    LEAD(TS_EVENTO)
            OVER (
                PARTITION BY co_CATEGORIA, CO_SUBSITE
                ORDER BY TS_EVENTO ASC
            )
            AS TS_EVENTO_END_TIME,
    TS_EVENTO_END_TIME IS null AS IS_RECORD_ACTUAL,
    CO_HIERARCHY
FROM DB_BRO_{{ environment }}.FIREFLY.VW_CATEGORIES_COMPLETE;
RETURN OBJECT_CONSTRUCT(
            'status', 'SUCCESS', 
            'coment', 'Proceso completado');
END;
$$;  