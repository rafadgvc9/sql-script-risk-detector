USE DATABASE DB_SIL_{{ environment }};
USE SCHEMA MDM_CORPORATIVO;
CREATE OR REPLACE PROCEDURE SP_LOAD_DELTA_TB_EMPRESA_GRUPO_PERSONAL (config VARIANT)
RETURNS VARIANT NOT NULL
LANGUAGE SQL
COMMENT = '
    Version: 1.0.0
    Source/s:  DB_BRO_<environment>.MAESTRAS.VW_EMPRESA_GRUPO_PERSONAL
    Target/s: DB_SIL_<environment>.MDM_CORPORATIVO.TB_EMPRESA_GRUPO_PERSONAL
    Inputs: variant (json) with the following properties:
    - environment: contains the enviroment in which to execute the process
    - df_run_id: caller id for the processes executed by Azure Data Factory
    - co_procedure: name of the procedure for registration in the High-watermarking metadata table
    - ts_override_data_start (Optional: reprocess): if informed, all data with timestamp greater than the same are loaded
'
EXECUTE AS CALLER
AS
$$
DECLARE
    statement VARCHAR;
    res RESULTSET;
    error VARCHAR;
 
    inserted_rows integer := 0;
    updated_rows integer := 0;
    deleted_rows integer := 0;
 
    -- Configurations
    environment varchar := config:environment;
    df_run_id varchar := config:df_run_id;
    co_procedure string := config:co_procedure;
    ts_override_data_start_default timestamp := null;
    ts_override_data_start timestamp := coalesce(config:ts_override_data_start, ts_override_data_start_default);

    start_run variant;
    id_run NUMBER;
    ts_data_start timestamp;
    ts_data_end timestamp;

    db_common_public varchar := 'db_common_'|| environment ||'.public';
 
BEGIN
    SYSTEM$LOG('info', 'Init procedure');
    SYSTEM$LOG('info', 'Procedure '|| co_procedure ||' loads DB_SIL_'|| environment ||'.MDM_CORPORATIVO.TB_EMPRESA_GRUPO_PERSONAL');

    use schema identifier(:db_common_public);
    call sp_get_ts_start_run(:co_procedure, :ts_override_data_start) into :start_run;
    id_run := start_run:ID_RUN::number;
    ts_data_start := start_run:TS_DATA_START;
    ts_data_end := start_run:TS_DATA_END;
 
    -- Query execution
    statement := '
        merge into DB_SIL_'|| environment ||'.MDM_CORPORATIVO.TB_EMPRESA_GRUPO_PERSONAL t1
        using (
            select
                CO_EMPRESA,
                DS_EMPRESA,
                CO_PAIS_ALTERNATIVO,
                IS_OBSERVACION,
                DS_EMPRESA_ABREVIADA,
                IS_EXPEDIENTE,
                CO_ACTIVIDAD_PRINCIPAL,
                DS_ACTIVIDAD_PRINCIPAL,
                IS_BORRADO,
                FALSE AS IS_SAP,
                TRUE AS IS_HOST,
                current_date() as DT_FECHA_INSERCION,
                current_timestamp() as TS_TIMESTAMP_INSERCION
            from
                DB_BRO_'|| environment ||'.HOST_MAESTRAS.VW_EMPRESA_GRUPO_PERSONAL
        ) t2
        on t1.CO_EMPRESA = t2.CO_EMPRESA
        when matched then
            update set                
                t1.DS_EMPRESA = t2.DS_EMPRESA,
                t1.CO_PAIS_ALTERNATIVO = t2.CO_PAIS_ALTERNATIVO,
                t1.IS_OBSERVACION = t2.IS_OBSERVACION,
                t1.DS_EMPRESA_ABREVIADA = t2.DS_EMPRESA_ABREVIADA,
                t1.IS_EXPEDIENTE = t2.IS_EXPEDIENTE,
                t1.CO_ACTIVIDAD_PRINCIPAL = t2.CO_ACTIVIDAD_PRINCIPAL,
                t1.DS_ACTIVIDAD_PRINCIPAL = t2.DS_ACTIVIDAD_PRINCIPAL,
                t1.IS_BORRADO = t2.IS_BORRADO,
                t1.IS_SAP = t2.IS_SAP,
                t1.IS_HOST = t2.IS_HOST,
                t1.DT_FECHA_INSERCION = t2.DT_FECHA_INSERCION,
                t1.TS_TIMESTAMP_INSERCION = t2.TS_TIMESTAMP_INSERCION
        when not matched then
            insert
                (t1.CO_EMPRESA, t1.DS_EMPRESA, t1.CO_PAIS_ALTERNATIVO, t1.IS_OBSERVACION, t1.DS_EMPRESA_ABREVIADA, t1.IS_EXPEDIENTE, t1.CO_ACTIVIDAD_PRINCIPAL, t1.DS_ACTIVIDAD_PRINCIPAL,
                t1.IS_BORRADO, t1.IS_SAP, t1.IS_HOST, t1.DT_FECHA_INSERCION, t1.TS_TIMESTAMP_INSERCION)
            values
                (t2.CO_EMPRESA, t2.DS_EMPRESA, t2.CO_PAIS_ALTERNATIVO, t2.IS_OBSERVACION, t2.DS_EMPRESA_ABREVIADA, t2.IS_EXPEDIENTE, t2.CO_ACTIVIDAD_PRINCIPAL, t2.DS_ACTIVIDAD_PRINCIPAL,
                t2.IS_BORRADO, t2.IS_SAP, t2.IS_HOST, t2.DT_FECHA_INSERCION, t2.TS_TIMESTAMP_INSERCION)
    ';
    SYSTEM$LOG('info', 'Execute query: ' || statement);
    res := (EXECUTE IMMEDIATE :statement);
    SYSTEM$LOG('info', 'Result query: Success');
 
    use schema identifier(:db_common_public);
    call sp_get_ts_end_run(:id_run, true);
     
    -- Check how many rows have been inserted and updated (if applies)
    LET inserted_cursor CURSOR FOR res; OPEN inserted_cursor; FETCH inserted_cursor INTO inserted_rows, updated_rows;
    SYSTEM$LOG('info', 'Number of rows inserted: ' || inserted_rows || '. Number of rows updated: ' || updated_rows);

    SYSTEM$LOG('info', 'End procedure');
 
    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS', -- Value could be 'SUCESS' (Default) or 'ERROR'
        'description', '', -- Value could be empty or contain the error description if
        'processed_rows', inserted_rows + updated_rows + deleted_rows, -- Number of modified rows througth the process
        'inserted_rows', inserted_rows, -- Number of inserted rows
        'updated_rows', updated_rows, -- Number of updated rows
        'deleted_rows', deleted_rows -- Number of deleted rows
    );
EXCEPTION
    WHEN OTHER THEN
        use schema identifier(:db_common_public);
        call sp_get_ts_end_run(:id_run, false);
        SYSTEM$LOG('error', sqlerrm);
        -- ROLLBACK; -- In case some transaction is executed in the body of the process
        RAISE;
END;
$$;