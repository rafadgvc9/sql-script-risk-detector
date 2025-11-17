import sqlparse
import random
import sys
import os
import re
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Callable
from variables_library import RIESGO

def get_active_snowflake_session():
    """Obtiene la sesión activa de Snowflake si está disponible"""
    try:
        from snowflake.snowpark.context import get_active_session
        try:
            session = get_active_session()
        except Exception:
            session = None
    except Exception:
        session = None 
    return session   

def _safe_sql_to_pandas(session, query):
    """Ejecuta una consulta via Snowpark y devuelve DataFrame vacío en fallo"""
    try:
        return session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame()


def _generate_fqn_candidates(object_info: Optional[Dict]) -> List[str]:
    """Genera candidatos FQN para buscar linaje (FULL, PARTIAL, unqualified)"""
    if not object_info or not object_info.get('object'):
        return []
    db = object_info.get('database') or object_info.get('current_context', {}).get('database')
    sch = object_info.get('schema') or object_info.get('current_context', {}).get('schema')
    obj = object_info.get('object')

    candidates = []
    if db and sch:
        candidates.append(f"{db}.{sch}.{obj}")
    if sch:
        candidates.append(f"{sch}.{obj}")
    candidates.append(obj)
    return candidates


def set_template_variables():
    """Establece las variables de template globales"""
    vars_dict: Dict[str, str] = {}

    for k, v in os.environ.items():
        if isinstance(v, str) and v:
            vars_dict[k] = v

    app_env = os.environ.get('APP_ENV') or os.environ.get('ENV') or os.environ.get('environment') or "DES"
    if app_env:
        vars_dict.setdefault('APP_ENV', app_env)
        vars_dict.setdefault('env', app_env)
        vars_dict.setdefault('environment', app_env)

    if 'REGION' in os.environ:
        vars_dict.setdefault('region', os.environ.get('REGION'))
    if 'PROJECT' in os.environ:
        vars_dict.setdefault('project', os.environ.get('PROJECT'))

    if 'env' in vars_dict and isinstance(vars_dict['env'], str):
        vars_dict['env'] = vars_dict['env'].upper()

    return vars_dict


def resolve_template_variables(text: str, variables: Dict[str, str] = None) -> Tuple[str, List[str]]:
    """Detecta y reemplaza variables del tipo '{{ variable }}' o ' :variable: ' de template; retorna (texto_resuelto, vars_detectadas)"""
    if variables is None:
        variables = set_template_variables()
    detected_vars = []
    missing_vars = []
    resolved_text = text
    
    pattern = r'\{\{?\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}?\}'
    
    matches = re.finditer(pattern, text)
    for match in matches:
        var_name = match.group(1).lower()
        detected_vars.append(var_name)
        
        var_value = None
        for key, value in variables.items():
            if key.lower() == var_name:
                var_value = value
                break

        if var_value is not None:
            resolved_text = resolved_text.replace(match.group(0), str(var_value))
        else:
            missing_vars.append(var_name)
            print(f"   ADVERTENCIA: Variable '{{{{ {var_name} }}}}' no encontrada en configuración")

    # detectar variables :nombre_variable
    colon_pattern = r':[A-Za-z_][A-Za-z0-9_]*'
    for m in re.finditer(colon_pattern, text):
        name = m.group(0)[1:].lower()
        if name not in detected_vars:
            detected_vars.append(name)

    return resolved_text, detected_vars


def preprocess_identifiers(sql_text: str, variables: Dict[str, str] = None) -> str:
    """Detecta y reemplaza llamadas a identifier(:var) y unwrap identifier('inner')"""
    if not sql_text:
        return sql_text
    if variables is None:
        variables = set_template_variables()

    vars_lower = {k.lower(): v for k, v in variables.items()}

    # Reemplazar identifier(:var) -> identifier('value') cuando exista
    def repl_colon(match):
        var = match.group(1)
        val = vars_lower.get(var.lower())
        if val is None:
            return match.group(0)
        return f"identifier('{val}')"

    sql_text = re.sub(r"identifier\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)", repl_colon, sql_text, flags=re.IGNORECASE)

    # Ahora eliminar wrappers identifier('inner') -> inner (sin comillas)
    def repl_unwrap(match):
        inner = match.group(1)
        return inner

    sql_text = re.sub(r"identifier\s*\(\s*'([^']+)'\s*\)", repl_unwrap, sql_text, flags=re.IGNORECASE)

    return sql_text

def normalize_dynamic_sql(sql_string: str, template_vars: Dict[str, str] = None) -> str:
    """Normaliza concatenaciones dinámicas en SQL"""
    if template_vars is None:
        template_vars = set_template_variables()
    
    vars_lower = {k.lower(): v for k, v in template_vars.items()}
    
    result = sql_string
    max_iterations = 30
    changed = True
    iteration = 0
    
    # Procesar identifier(:variable)
    identifier_pattern = re.compile(r'identifier\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)', re.IGNORECASE)
    
    def replace_identifier(match):
        var_name = match.group(1)
        var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
        return f"identifier('{var_value}')"
    
    result = identifier_pattern.sub(replace_identifier, result)
    
    # búsqueda de distintas posibles expresiones de una variable
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        
        # texto.|| var ||.texto  --> texto.valor.texto
        match = re.search(r"([A-Za-z0-9_]+)\.\|\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*\|\|\.([A-Za-z0-9_]+)", result, re.IGNORECASE)
        if match:
            prefix = match.group(1)
            var_name = match.group(2)
            suffix = match.group(3)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"{prefix}.{var_value}.{suffix}"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue
        
        # texto.|| valor --> texto.valor
        match = re.search(r"([A-Za-z0-9_]+)\.\|\|\s*([A-Za-z_][A-Za-z0-9_]*)\b", result, re.IGNORECASE)
        if match:
            prefix = match.group(1)
            var_name = match.group(2)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"{prefix}.{var_value}"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue
        
        # valor ||.texto --> valor.texto
        match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\|\|\.([A-Za-z0-9_]+)", result, re.IGNORECASE)
        if match:
            var_name = match.group(1)
            suffix = match.group(2)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"{var_value}.{suffix}"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue
        
        # 'texto' || var || 'texto' --> textovalortexto
        match = re.search(r"'([^']*)'\s*\|\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*\|\|\s*'([^']*)'", result, re.IGNORECASE)
        if match:
            prefix = match.group(1)
            var_name = match.group(2)
            suffix = match.group(3)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"'{prefix}{var_value}{suffix}'"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue
        
        # texto' || var || 'texto --> textovalortexto
        match = re.search(r"([A-Za-z0-9_]+)'(\s*\|\|\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*\|\|\s*)'([^']*)'", result, re.IGNORECASE)
        if match:
            prefix = match.group(1)  
            var_name = match.group(3)  
            suffix = match.group(5)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"{prefix}{var_value}{suffix}"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue

        # 'texto' || var --> textovalor
        match = re.search(r"'([^']*)'\s*\|\|\s*([A-Za-z_][A-Za-z0-9_]*)\b", result, re.IGNORECASE)
        if match:
            prefix = match.group(1)
            var_name = match.group(2)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"'{prefix}{var_value}'"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue

        # var || 'texto' --> valortexto
        match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\|\|\s*'([^']*)'", result, re.IGNORECASE)
        if match:
            var_name = match.group(1)
            suffix = match.group(2)
            var_value = vars_lower.get(var_name.lower(), f"VAR_{var_name.upper()}")
            combined = f"'{var_value}{suffix}'"
            result = result[:match.start()] + combined + result[match.end():]
            changed = True
            continue
    
    return result


# estas funciones deben cambiarse a las existentes
def has_object_lineage(object_info: Optional[Dict] = None, column: Optional[str] = None, session_param: Optional[Any] = None) -> bool:
    """Comprueba si un objeto/columna tiene linaje usando Snowflake (fallback True si no hay sesión)"""

    # preferir la sesión proporcionada, si viene; si no, usar la sesión global
    sess = session_param if session_param is not None else get_active_snowflake_session()

    if sess is None:
        return True

    try:
        if column and object_info:
            db = object_info.get('database') or object_info.get('current_context', {}).get('database')
            sch = object_info.get('schema') or object_info.get('current_context', {}).get('schema')
            obj = object_info.get('object')
            if db and sch and obj and column:
                col_fqn = f"{db}.{sch}.{obj}.{column}"
                df, errors = get_column_lineage(sess, [col_fqn])
                return not df.empty

        if object_info and object_info.get('object'):
            db = object_info.get('database') or object_info.get('current_context', {}).get('database')
            sch = object_info.get('schema') or object_info.get('current_context', {}).get('schema')
            obj = object_info.get('object')

            for cand in _generate_fqn_candidates(object_info):
                df_vw, _ = get_view_lineage(sess, cand)
                if not df_vw.empty:
                    return True

                df_tbl, _ = get_table_lineage(sess, cand)
                if not df_tbl.empty:
                    return True

        return False
    except Exception:
        print("   ADVERTENCIA: No se pudo comprobar linaje del objeto/columna, se asume que tiene linaje.", file=sys.stderr)
        return True


def get_view_lineage(session, view_fqn):
    """
    Obtiene el linaje 'downstream' para un único FQN de VISTA usando consultas secuenciales.
    """
    if not all([session, view_fqn]):
        return pd.DataFrame(), ["Error: La sesión o el FQN de la vista no son válidos."]
    
    try:
        from snowflake.snowpark.exceptions import SnowparkSQLException
        all_lineage = []
        current_objects = [view_fqn]
        processed_objects = set([view_fqn])
        max_depth = 10
        
        for distance in range(1, max_depth + 1):
            if not current_objects:
                break
                
            next_objects = []
            for obj in current_objects:
                query = f"""
                    SELECT 
                        {distance} as DISTANCE,
                        SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_DATABASE, SOURCE_OBJECT_SCHEMA, SOURCE_OBJECT_NAME,
                        TARGET_OBJECT_DOMAIN, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME
                    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('{obj.replace("'", "''")}', 'TABLE', 'DOWNSTREAM', 1))
                    WHERE SOURCE_STATUS = 'ACTIVE' AND TARGET_STATUS = 'ACTIVE'
                """
                results = _safe_sql_to_pandas(session, query)
                if not results.empty:
                    all_lineage.append(results)
                    targets = results.apply(lambda row: f"{row['TARGET_OBJECT_DATABASE']}.{row['TARGET_OBJECT_SCHEMA']}.{row['TARGET_OBJECT_NAME']}", axis=1)
                    new_targets = [t for t in targets if t not in processed_objects]
                    next_objects.extend(new_targets)
                    processed_objects.update(new_targets)
            
            current_objects = list(set(next_objects))
        
        return pd.concat(all_lineage, ignore_index=True) if all_lineage else pd.DataFrame(), []
            
    except SnowparkSQLException as e:
        return pd.DataFrame(), [f"Error de Snowflake al obtener linaje de la vista: {e.message}"]

def get_table_lineage(session, table_fqn, direction: str = 'DOWNSTREAM', max_depth: int = 10):
    """
    Obtiene el linaje para un único FQN de TABLA (upstream o downstream) usando consultas secuenciales.
    """
    if not all([session, table_fqn]):
        return pd.DataFrame(), ["Error: La sesión o el FQN de la tabla no son válidos."]

    try:
        from snowflake.snowpark.exceptions import SnowparkSQLException
        all_lineage = [] 
        current_objects = [table_fqn]
        processed_objects = set([table_fqn])

        for distance in range(1, max_depth + 1):
            if not current_objects:
                break

            next_objects = []
            for obj in current_objects:
                query = f"""
                    SELECT
                        {distance} as DISTANCE,
                        SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_DATABASE, SOURCE_OBJECT_SCHEMA, SOURCE_OBJECT_NAME,
                        TARGET_OBJECT_DOMAIN, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME
                    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('{obj.replace("'", "''")}', 'TABLE', '{direction.upper()}', 1))
                    WHERE SOURCE_STATUS = 'ACTIVE' AND TARGET_STATUS = 'ACTIVE'
                """
                results = _safe_sql_to_pandas(session, query)
                if not results.empty:
                    all_lineage.append(results)
                    targets = results.apply(lambda row: f"{row['TARGET_OBJECT_DATABASE']}.{row['TARGET_OBJECT_SCHEMA']}.{row['TARGET_OBJECT_NAME']}", axis=1)
                    new_targets = [t for t in targets if t not in processed_objects]
                    next_objects.extend(new_targets)
                    processed_objects.update(new_targets)

            current_objects = list(set(next_objects))

        return pd.concat(all_lineage, ignore_index=True) if all_lineage else pd.DataFrame(), []

    except SnowparkSQLException as e:
        return pd.DataFrame(), [f"Error de Snowflake al obtener linaje de la tabla: {e.message}"]

def get_column_lineage(session, column_fqns_list):
    """
    Obtiene el linaje 'downstream' para una lista de FQNs de COLUMNAS usando consultas secuenciales.
    """
    if not all([session, column_fqns_list]):
        return pd.DataFrame(), ["Error: La sesión o la lista de FQNs de columna no son válidas."]
    
    try:
        from snowflake.snowpark.exceptions import SnowparkSQLException
        all_lineage_dfs = []
        
        for fqn in column_fqns_list:
            if not isinstance(fqn, str) or fqn.count('.') < 3:
                continue

            analyzed_column_name = fqn.split('.')[-1]
            lineage_for_col = []
            
            # Objetos de tabla a procesar en cada nivel
            current_table_fqns = [('.'.join(fqn.split('.')[:-1]))]
            processed_tables = set(current_table_fqns)
            
            for distance in range(1, 11):
                if not current_table_fqns:
                    break
                    
                next_table_fqns = []
                for table_fqn in current_table_fqns:
                    column_to_query = f"{table_fqn}.{analyzed_column_name}"
                    query = f"""
                        SELECT 
                            '{analyzed_column_name}' AS ANALYZED_COLUMN,
                            {distance} AS DISTANCE,
                            SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_DATABASE, SOURCE_OBJECT_SCHEMA, SOURCE_OBJECT_NAME,
                            TARGET_OBJECT_DOMAIN, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME
                        FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('{column_to_query.replace("'", "''")}', 'COLUMN', 'DOWNSTREAM', 1))
                        WHERE SOURCE_STATUS = 'ACTIVE' AND TARGET_STATUS = 'ACTIVE'
                    """
                    try:
                        results = session.sql(query).to_pandas()
                        if not results.empty:
                            lineage_for_col.append(results)
                            targets = results.apply(lambda row: f"{row['TARGET_OBJECT_DATABASE']}.{row['TARGET_OBJECT_SCHEMA']}.{row['TARGET_OBJECT_NAME']}", axis=1)
                            new_targets = [t for t in targets if t not in processed_tables]
                            next_table_fqns.extend(new_targets)
                            processed_tables.update(new_targets)
                    except:
                        continue
                
                current_table_fqns = list(set(next_table_fqns))
            
            if lineage_for_col:
                all_lineage_dfs.append(pd.concat(lineage_for_col, ignore_index=True))

        return pd.concat(all_lineage_dfs, ignore_index=True) if all_lineage_dfs else pd.DataFrame(), []
            
    except SnowparkSQLException as e:
        return pd.DataFrame(), [f"Error de Snowflake al obtener linaje de columnas: {e.message}"]

def is_warehouse_xs():
    return random.choice([True, False])


def parse_object_name(obj_name: str) -> Dict[str, Optional[str]]:
    """
    Analiza un nombre de objeto y determina si está completamente cualificado.
    Retorna un diccionario con database, schema, y object.
    """
    if not obj_name:
        return {"database": None, "schema": None, "object": None, "is_qualified": False}
    
    # Eliminar comillas si existen
    obj_name = obj_name.strip('"').strip("'")
    
    parts = obj_name.split('.')
    
    if len(parts) == 3:
        return {
            "database": parts[0],
            "schema": parts[1],
            "object": parts[2],
            "is_qualified": True,
            "qualification_level": "FULL"
        }
    elif len(parts) == 2:
        return {
            "database": None,
            "schema": parts[0],
            "object": parts[1],
            "is_qualified": True,
            "qualification_level": "PARTIAL"
        }
    else:
        return {
            "database": None,
            "schema": None,
            "object": parts[0],
            "is_qualified": False,
            "qualification_level": "NONE"
        }


# construye y calcula el riesgo 
def _create_result(accion: str, objeto: Optional[str], columna: Optional[str], 
                   needs_lineage_check: bool, object_info: Optional[Dict] = None,
                   template_vars: Optional[List[str]] = None) -> Dict[str, Any]:
    """Construye el diccionario resultado de una operacion"""
    riesgo_base = RIESGO[accion]
    riesgo_final = ""
    if isinstance(riesgo_base, tuple) and needs_lineage_check:
        riesgo_con_linaje, riesgo_sin_linaje = riesgo_base
        if has_object_lineage(object_info=object_info, column=columna, session_param=get_active_snowflake_session()):
            riesgo_final = riesgo_con_linaje  
        else:
            riesgo_final = riesgo_sin_linaje 
    else:
        riesgo_final = riesgo_base if isinstance(riesgo_base, str) else riesgo_base[0]
    result = {
        "accion": accion,
        "objeto": objeto,
        "columna": columna,
        "riesgo": riesgo_final
    }
    if object_info:
        result["object_info"] = object_info
    if template_vars:
        result["template_variables"] = template_vars
    
    return result

def extract_procedure_body(stmt_clean: str) -> Optional[str]:
    """Extrae el cuerpo de un procedimiento (entre $$ o comillas simples)"""

    patterns = [
        r"AS\s+\$\$\s*(.*?)\s*\$\$",
        r"AS\s+'(.*?)'",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, stmt_clean, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None

def extract_sql_from_variables(proc_body: str, template_vars: Dict[str, str] = None) -> List[str]:
    """Extrae sentencias SQL asignadas a variables dentro de un cuerpo de procedure."""

    sql_statements = []
    
    # Patrones para diferentes tipos de delimitadores
    patterns = [
        # comillas simples
        r"[A-Za-z_][A-Za-z0-9_]*\s*:=\s*'(.*?)'",      
        # comillas dobles
        r'[A-Za-z_][A-Za-z0-9_]*\s*:=\s*"(.*?)"',      
        # $$
        r"[A-Za-z_][A-Za-z0-9_]*\s*:=\s*\$\$(.*?)\$\$", 
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, proc_body, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            sql_content = match.group(1).strip()
            
            # comprobar que el contenido de la variable contiene sentencias sql
            sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 
                           'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'GRANT', 
                           'REVOKE', 'WITH']
            
            sql_upper = sql_content.upper()
            
            if any(keyword in sql_upper for keyword in sql_keywords):
                normalized_sql = normalize_dynamic_sql(sql_content, template_vars)
                sql_statements.append(normalized_sql)
    
    return sql_statements


def extract_local_variables(proc_body: str, template_vars: Dict[str, str] = None) -> Dict[str, str]:
    """Extrae asignaciones simples de variables dentro del body del procedure y trata de evaluarlas"""
    if template_vars is None:
        template_vars = set_template_variables()

    local_vars: Dict[str, str] = {}

    assign_pattern = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z0-9_]+\s*:=\s*(.*?);", re.IGNORECASE | re.DOTALL)

    for m in re.finditer(assign_pattern, proc_body):
        name = m.group(1).strip()
        expr = m.group(2).strip()

        # dividir por operadores || y evaluar cada parte
        parts = re.split(r"\|\|", expr)
        resolved_parts: List[str] = []

        for p in parts:
            p = p.strip()
            resolved = None

            # literal entre comillas simples
            q = re.match(r"^'(.*)'$", p, re.DOTALL)
            if q:
                resolved = q.group(1)
            else:
                # referencia a config:var
                q2 = re.match(r"^config:([A-Za-z_][A-Za-z0-9_]*)$", p, re.IGNORECASE)
                if q2:
                    key = q2.group(1).lower()
                    resolved = template_vars.get(key)
                else:
                    # referencia con prefijo ':'
                    q3 = re.match(r"^:([A-Za-z_][A-Za-z0-9_]*)$", p)
                    if q3:
                        key = q3.group(1).lower()
                        resolved = template_vars.get(key) or local_vars.get(key)
                    else:
                        # nombre de variable simple
                        q4 = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)$", p)
                        if q4:
                            key = q4.group(1).lower()
                            resolved = local_vars.get(key) or template_vars.get(key)

            if resolved is None:
                # fallback: marcar con placeholder legible
                resolved = f"VAR_{name.upper()}"

            resolved_parts.append(str(resolved))

        local_vars[name.lower()] = ''.join(resolved_parts)

    return local_vars

# funcion principal para analizar todo el script 
def analizar_sql(path_sql: str, template_vars: Dict[str, str] = None):
    sql_text = Path(path_sql).read_text()
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    resolved_sql, all_detected_vars = resolve_template_variables(sql_text, template_vars)

    # Preprocesar identifier(:var) y unwrap identifier('...') a nivel global
    resolved_sql = preprocess_identifiers(resolved_sql, template_vars or set_template_variables())
    
    statements = sqlparse.split(resolved_sql)

    current_context = {
        "database": None,
        "schema": None
    } 

    # pasa por todas las sentencias
    resultados = []
    for stmt in statements:
        # se eliminan los comentarios de las sentencias para evitar que se interpreten comentarios como parte de la sentencia
        lines = stmt.strip().split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('--')]
        stmt_uncommented = '\n'.join(cleaned_lines).strip()
        # aplicar normalización dinámica primero (mantener case original para extraer bodies)
        stmt_normalized = normalize_dynamic_sql(stmt_uncommented, template_vars)
        # extraer cuerpo de procedure sobre la versión normalizada (no upper) para preservar literales
        stmt_clean = stmt_normalized.upper()
        
        if not stmt_clean:
            continue
        
        

        if re.match(r"^CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE", stmt_clean):
            match = re.search(r"PROCEDURE\s+([A-Z0-9_.\"]+)\s*\(", stmt_clean)
            proc_name = match.group(1) if match else None
            
            # extrae las sentencias del procedimiento
            proc_body = extract_procedure_body(stmt_normalized)
            if proc_body:
                # extraer variables locales y combinarlas con template_vars
                local_vars = extract_local_variables(proc_body, template_vars)
                merged_vars = dict(template_vars or set_template_variables())
                # local vars sobrescriben template vars si hay colisión
                for k, v in local_vars.items():
                    merged_vars[k] = v

                proc_body = preprocess_identifiers(proc_body, merged_vars)

                # pasa por todas las variables de texto por si tienen sentencias guardadas
                variable_sqls = extract_sql_from_variables(proc_body, merged_vars)
                for var_sql in variable_sqls:
                    # analiza cada sentencia que tenga la variable
                    var_sql_statements = sqlparse.split(var_sql)
                    
                    for var_stmt in var_sql_statements:
                        var_lines = var_stmt.strip().split('\n')
                        var_cleaned = [l for l in var_lines if not l.strip().startswith('--')]
                        var_stmt_clean = '\n'.join(var_cleaned).strip().upper()
                        
                        if var_stmt_clean:
                            var_results = procesar_sentencia(var_stmt_clean, current_context, proc_name)
                            
                            # marcar cada resultado como que viene de una variable
                            for result in var_results:
                                if result.get('object_info'):
                                    result['object_info']['from_variable'] = True
                            
                            resultados.extend(var_results)


                inner_statements = sqlparse.split(proc_body)

                for inner_stmt in inner_statements:
                    inner_lines = inner_stmt.strip().split('\n')
                    inner_cleaned = [l for l in inner_lines if not l.strip().startswith('--')]
                    inner_raw = '\n'.join(inner_cleaned).strip()
                    # normalizar usando las variables locales/template combinadas
                    inner_normalized = normalize_dynamic_sql(inner_raw, merged_vars)
                    inner_stmt_clean = inner_normalized.strip().upper()

                    if inner_stmt_clean:
                        # pasar por todas las sentencias del procedure que no hayan sido procesadas en las variables
                        if not re.match(r"[A-Z_][A-Z0-9_]*\s*:=\s*'", inner_stmt_clean):
                            inner_results = procesar_sentencia(inner_stmt_clean, current_context, proc_name)
                            resultados.extend(inner_results)
            
            accion_procedure = "CREATE_PROCEDURE"
            needs_lineage = False
            
            if "OR REPLACE" in stmt_clean:
                accion_procedure = "CREATE_OR_REPLACE_PROCEDURE"
                needs_lineage = True
            
            # registrar la creación del procedure
            obj_info = parse_object_name(proc_name) if proc_name else None
            if obj_info:
                obj_info["current_context"] = current_context.copy()
            
            resultados.append(_create_result(accion_procedure, proc_name, None, needs_lineage, obj_info))
        else:
            # procesamiento de sentencia normal
            stmt_results = procesar_sentencia(stmt_clean, current_context)
            resultados.extend(stmt_results)
    
    hay_riesgo = any(r["riesgo"] in ["MEDIA", "ALTA"] for r in resultados)
    return hay_riesgo, resultados

def procesar_sentencia(stmt_clean: str, current_context: Dict, 
                      proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Procesa una sentencia SQL llamando a cada una de las posibles sentencias a ejecutar
    """
    from handlers_library import (
        _handle_use, _handle_create, _handle_alter, _handle_drop,
        _handle_undrop, _handle_truncate, _handle_insert, _handle_merge,
        _handle_delete, _handle_grant, _handle_revoke, _handle_execute,
        _handle_call, STATEMENT_HANDLERS
    )
    for pattern, handler in STATEMENT_HANDLERS:
        if re.match(pattern, stmt_clean):
            return handler(stmt_clean, current_context, proc_context)
    
    return []